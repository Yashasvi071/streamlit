# salesforce_sql_streamlit.py
# pip install streamlit pandas duckdb sqlglot simple-salesforce openpyxl

import streamlit as st
import pandas as pd
import re
import io
import math
import duckdb
from sqlglot import parse_one
import sqlglot
from simple_salesforce import Salesforce
from pathlib import Path

st.set_page_config(page_title="Salesforce → DuckDB SQL Runner", layout="wide")
#st.set_option('server.maxUploadSize', 1024)  # allow bigger uploads (MB). Adjust if needed.

st.title("🔁 Salesforce → DuckDB SQL Runner")

st.markdown(
    "Connect to Salesforce, optionally upload a CSV of IDs (or provide a local path), "
    "enter SQL (use aliases), and run. The app will fetch only necessary fields and run the SQL in DuckDB."
)

# ---------------------------
# Inputs: Salesforce login
# ---------------------------
with st.expander("Salesforce credentials & Folder / IDs (expand) — required"):
    col1, col2, col3 = st.columns(3)
    with col1:
        sf_username = st.text_input("Username (email)", value="", placeholder="user@example.com")
        sf_password = st.text_input("Password", type="password")
        sf_token = st.text_input("Security token", type="password")
    with col2:
        st.markdown("**Optional: IDs CSV**")
        ids_file = st.file_uploader("Upload IDs CSV (optional)", type=["csv"], accept_multiple_files=False, key="ids_upload")
        ids_local_path = st.text_input("Or enter local IDs CSV path (optional)", value="")
        id_object = st.selectbox("IDs belong to (choose after login)", options=["(not connected)"], index=0)
    with col3:
        st.markdown("**SQL Input**")
        query_box = st.text_area(
            "Write SQL here (end with semicolon not required). Use `uploaded_ids` as table name for uploaded IDs.",
            height=180
        )
        run_button = st.button("Run Query", type="primary")

# Status / logs
log = st.empty()
progress = st.progress(0)

# ---------------------------
# Helper functions (kept from your script)
# ---------------------------
def parse_table_aliases(sql):
    try:
        tree = parse_one(sql)
        pairs = []
        for t in tree.find_all(sqlglot.exp.Table):
            real = t.name
            alias = t.alias_or_name
            pairs.append((alias, real))
        return pairs
    except Exception:
        pattern = r"(?:from|join)\s+([\w_]+)(?:\s+as\s+)?([\w_]+)?"
        pairs = []
        for m in re.finditer(pattern, sql, flags=re.IGNORECASE):
            real = m.group(1)
            alias = m.group(2) or real
            pairs.append((alias, real))
        return pairs

def extract_all_columns(sql):
    cols = set()
    try:
        tree = parse_one(sql)
        for col in tree.find_all(sqlglot.expressions.Column):
            parts = [p.name for p in col.parts]
            cols.add(".".join(parts))
    except Exception:
        for m in re.finditer(r"([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+)", sql):
            cols.add(m.group(1))
    return cols

def flatten_colname(parts):
    if len(parts) == 1:
        return parts[0].lower()
    alias = parts[0]
    rest = parts[1:]
    return alias + "." + "_".join(p.lower() for p in rest)

def soql_field_from_parts(parts):
    return ".".join(parts[1:])

def describe_fields_for_object(sobject, sf):
    desc = getattr(sf, sobject).describe()
    return [f['name'] for f in desc['fields']]

def get_reference_fields_map(sobject, sf):
    desc = getattr(sf, sobject).describe()
    ref_map = {}
    for f in desc['fields']:
        if f['type'] == 'reference':
            ref_map[f['name']] = {
                'referenceTo': f.get('referenceTo', []),
                'relationshipName': f.get('relationshipName')
            }
    return ref_map

def fetch_object_data(sf, sobject, soql_fields_set, id_list_for_object=None, id_filter_field='Id', batch_size=2000):
    # expand __ALL__
    if '__ALL__' in soql_fields_set:
        fields = describe_fields_for_object(sobject, sf)
        soql_selects = list(fields)
    else:
        soql_selects = list(soql_fields_set)

    soql_selects = list(dict.fromkeys(soql_selects))
    if not soql_selects:
        soql_selects = ['Id']

    select_clause = ", ".join(soql_selects)
    all_records = []

    if id_list_for_object:
        for i in range(0, len(id_list_for_object), batch_size):
            chunk = id_list_for_object[i:i+batch_size]
            ids_str = ",".join(repr(x) for x in chunk)
            soql = f"SELECT {select_clause} FROM {sobject} WHERE {id_filter_field} IN ({ids_str})"
            try:
                recs = sf.query_all(soql)['records']
            except Exception:
                if len(chunk) > 100:
                    half = math.ceil(len(chunk)/2)
                    recs = []
                    for j in range(0, len(chunk), half):
                        sub = chunk[j:j+half]
                        ids_sub = ",".join(repr(x) for x in sub)
                        soql_sub = f"SELECT {select_clause} FROM {sobject} WHERE {id_filter_field} IN ({ids_sub})"
                        recs.extend(sf.query_all(soql_sub)['records'])
                else:
                    raise
            all_records.extend(recs)
    else:
        soql = f"SELECT {select_clause} FROM {sobject}"
        all_records = sf.query_all(soql)['records']

    if not all_records:
        return pd.DataFrame()

    df = pd.json_normalize(all_records)
    df = df.loc[:, [c for c in df.columns if not c.endswith('.attributes')]]
    rename_map = {c: re.sub(r'\W+', '_', c).lower() for c in df.columns}
    df = df.rename(columns=rename_map)
    return df

def build_fields_to_fetch(sql, alias_real_pairs, all_columns):
    mapping = {}
    wildcard_aliases = set()
    if re.search(r"\bselect\s+\*\s", sql, flags=re.IGNORECASE):
        wildcard_aliases.update([alias for alias, _ in alias_real_pairs])
    for m in re.finditer(r"([A-Za-z0-9_]+)\.\*", sql):
        wildcard_aliases.add(m.group(1))
    for alias, real in alias_real_pairs:
        mapping[alias] = set()
    for col in all_columns:
        parts = col.split(".")
        if parts[0] in mapping:
            alias = parts[0]
            if len(parts) == 1:
                continue
            mapping[alias].add(soql_field_from_parts(parts))
    for alias in wildcard_aliases:
        mapping[alias] = {"__ALL__"}
    return mapping

# The main runner logic (preserves your original function with minimal changes for Streamlit)
def run_user_query_streamlit(sf, uploaded_ids_df, id_object_value, sql_text):
    alias_real_pairs = parse_table_aliases(sql_text)
    if not alias_real_pairs:
        raise ValueError("No table references found in query.")

    sf_alias_real_pairs = [(alias, real) for alias, real in alias_real_pairs if real.lower() != 'uploaded_ids']
    all_cols = extract_all_columns(sql_text)
    mapping = build_fields_to_fetch(sql_text, sf_alias_real_pairs, all_cols)
    alias_to_real = {alias: real for alias, real in sf_alias_real_pairs}

    # id map build
    id_map = {}
    uploaded_ids_list = uploaded_ids_df['Id'].tolist() if (uploaded_ids_df is not None and not uploaded_ids_df.empty) else None

    # precompute ref maps (may raise permission errors which we let bubble up and show)
    ref_maps = {}
    for alias, real in sf_alias_real_pairs:
        try:
            ref_maps[real] = get_reference_fields_map(real, sf)
        except Exception:
            ref_maps[real] = {}

    if uploaded_ids_list and id_object_value:
        for alias, real in sf_alias_real_pairs:
            if real.lower() == id_object_value.lower():
                id_map[alias] = (uploaded_ids_list[:], 'Id')
            else:
                chosen_field = None
                for ref_field, meta in ref_maps.get(real, {}).items():
                    if id_object_value in meta.get('referenceTo', []) or id_object_value.lower() in [r.lower() for r in meta.get('referenceTo', [])]:
                        chosen_field = ref_field
                        break
                id_map[alias] = (uploaded_ids_list[:], chosen_field or 'Id')

    # Fetch SF data
    dfs = {}
    total = len(sf_alias_real_pairs) or 1
    step = 0
    for alias, real in sf_alias_real_pairs:
        soql_fields = mapping.get(alias, set())
        if '__ALL__' not in soql_fields:
            soql_fields.add('Id')
        ids_for_obj, id_filter_field = id_map.get(alias, (None, 'Id'))
        log.info(f"Fetching {real} (alias {alias}) ...")
        df_obj = fetch_object_data(sf, real, soql_fields, id_list_for_object=ids_for_obj, id_filter_field=id_filter_field)
        dfs[alias] = df_obj
        step += 1
        progress.progress(int(step/total*50))  # fetching step (50% of progress)

    # Prepare DuckDB
    con = duckdb.connect()
    if uploaded_ids_list:
        con.register("uploaded_ids", uploaded_ids_df)
    for alias, real in alias_real_pairs:
        if real.lower() == 'uploaded_ids':
            continue
        df_reg = dfs.get(alias, pd.DataFrame())
        # register both alias and real name
        con.register(alias, df_reg)
        try:
            con.register(real, df_reg)
        except Exception:
            pass

    # Rewrite SQL dotted references
    new_sql = sql_text
    replacements = {}
    for col in sorted(all_cols, key=lambda x: -len(x)):
        parts = col.split(".")
        if parts[0] in alias_to_real:
            flat = flatten_colname(parts)
            replacements[col] = f"{parts[0]}.{flat.split('.',1)[1]}" if "." in flat else f"{parts[0]}.{flat}"

    for orig in sorted(replacements.keys(), key=lambda x: -len(x)):
        esc = re.escape(orig)
        new_sql = re.sub(rf'\b{esc}\b', replacements[orig], new_sql, flags=re.IGNORECASE)

    # Expand SELECT * unqualified
    if re.search(r"SELECT\s+\*\s", new_sql, flags=re.IGNORECASE):
        expansions = []
        for alias, df_reg in dfs.items():
            cols = list(df_reg.columns)
            prefixed = [f"{alias}.{c}" for c in cols]
            expansions.extend(prefixed)
        if 'uploaded_ids' in sql_text.lower() and uploaded_ids_list:
            id_cols = [f"uploaded_ids.{c}" for c in uploaded_ids_df.columns]
            expansions.extend(id_cols)
        new_sql = re.sub(r"SELECT\s+\*\s", "SELECT " + ", ".join(expansions) + " ", new_sql, flags=re.IGNORECASE)

    # Expand alias.* patterns
    for m in re.finditer(r"([A-Za-z0-9_]+)\.\*", new_sql):
        alias = m.group(1)
        if alias in alias_to_real:
            df_reg = dfs.get(alias)
            if df_reg is not None and not df_reg.empty:
                cols = df_reg.columns
                prefixed = [f"{alias}.{c}" for c in cols]
                replacement = ", ".join(prefixed)
                new_sql = re.sub(rf"{re.escape(alias)}\.\*", replacement, new_sql)

    progress.progress(75)
    # Execute SQL
    try:
        result = con.execute(new_sql).df()
    except Exception as e:
        raise RuntimeError(f"Error executing SQL in DuckDB: {e}")

    progress.progress(100)
    return result

# ---------------------------
# Streamlit runtime: handle login & run
# ---------------------------
# Local session storage placeholders
if 'sf_conn' not in st.session_state:
    st.session_state.sf_conn = None
if 'sobjects' not in st.session_state:
    st.session_state.sobjects = []

# log helper
log = st.empty()

# Connect button UI & logic
if st.button("Connect to Salesforce"):
    try:
        if not (sf_username and sf_password and sf_token):
            st.error("Please supply Salesforce username, password, and security token before connecting.")
        else:
            with st.spinner("Connecting..."):
                sf_conn = Salesforce(username=sf_username, password=sf_password, security_token=sf_token)
                st.session_state.sf_conn = sf_conn
                # fetch sobjects for dropdown
                try:
                    sobjects = sf_conn.describe()['sobjects']
                    api_names = sorted([o['name'] for o in sobjects if o.get('queryable')])
                    st.session_state.sobjects = api_names
                    st.success(f"Connected. Found {len(api_names)} queryable sObjects.")
                except Exception:
                    st.session_state.sobjects = []
                    st.success("Connected (could not list sObjects).")
    except Exception as e:
        st.error(f"Salesforce login failed: {e}")

# update id_object selectbox after connect
if st.session_state.sobjects:
    # replace placeholder selectbox created earlier
    try:
        # maintain previous selection if present
        prev = id_object if id_object and id_object != "(not connected)" else None
    except Exception:
        prev = None
    id_object = st.selectbox("IDs belong to (select object for uploaded IDs)", options=["(none)"] + st.session_state.sobjects, index=0)
else:
    id_object = st.selectbox("IDs belong to (select object for uploaded IDs)", options=["(none)"], index=0)

# If user uploaded an ids CSV, read it
uploaded_ids_df = pd.DataFrame()
if ids_file is not None:
    try:
        uploaded_ids_df = pd.read_csv(ids_file)
        if 'Id' not in uploaded_ids_df.columns:
            uploaded_ids_df.columns = ['Id']
        uploaded_ids_df['Id'] = uploaded_ids_df['Id'].astype(str)
        st.info(f"Uploaded {len(uploaded_ids_df)} IDs from CSV.")
    except Exception as e:
        st.error(f"Failed to read uploaded IDs CSV: {e}")
elif ids_local_path:
    p = Path(ids_local_path)
    if p.exists():
        try:
            uploaded_ids_df = pd.read_csv(p)
            if 'Id' not in uploaded_ids_df.columns:
                uploaded_ids_df.columns = ['Id']
            uploaded_ids_df['Id'] = uploaded_ids_df['Id'].astype(str)
            st.info(f"Loaded {len(uploaded_ids_df)} IDs from local path.")
        except Exception as e:
            st.error(f"Failed to read IDs from local path: {e}")

# Run the query when user clicks Run
if run_button:
    if st.session_state.sf_conn is None:
        st.error("Please connect to Salesforce first.")
    elif not query_box or query_box.strip() == "":
        st.error("Please enter a SQL query.")
    else:
        try:
            log.text("Starting fetch & query...")
            progress.progress(5)
            result_df = run_user_query_streamlit(st.session_state.sf_conn, uploaded_ids_df, id_object if id_object and id_object != "(none)" else None, query_box)
            if result_df is None or result_df.empty:
                st.warning("Query returned no rows.")
            else:
                st.success(f"Query returned {len(result_df)} rows and {len(result_df.columns)} columns.")
                st.dataframe(result_df.head(200))

                # download buttons
                csv_bytes = result_df.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv_bytes, file_name="salesforce_sql_result.csv", mime="text/csv")

                # excel
                towrite = io.BytesIO()
                result_df.to_excel(towrite, index=False, engine="openpyxl")
                towrite.seek(0)
                st.download_button("Download Excel", towrite.read(), file_name="salesforce_sql_result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        except Exception as e:
            st.error(f"Error running query: {e}")
            st.exception(e)
