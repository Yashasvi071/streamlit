# app.py - Multi-tool Streamlit app with Name & Address Match plugged in
# pip install streamlit pandas openpyxl fuzzywuzzy python-Levenshtein simple-salesforce duckdb sqlglot requests beautifulsoup4 rapidfuzz ddgs

import streamlit as st
import pandas as pd
import io
from pathlib import Path
import traceback

# set larger upload limit (MB) if needed; still recommend local path for huge files
st.set_page_config(page_title="Tools Dashboard", layout="wide")
#st.set_option('server.maxUploadSize', 1024)

# -------------------------
# Helper functions
# -------------------------
def load_dataframe(file_uploader, local_path):
    """Prefer uploaded file (browser). Fall back to local path if provided."""
    if file_uploader is not None:
        try:
            if file_uploader.name.lower().endswith(".csv"):
                return pd.read_csv(file_uploader, dtype=str).fillna("")
            else:
                return pd.read_excel(file_uploader, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Failed to read uploaded file: {e}")
            st.exception(e)
            return pd.DataFrame()
    if local_path:
        p = Path(local_path)
        if p.exists():
            try:
                if p.suffix.lower() == ".csv":
                    return pd.read_csv(p, dtype=str, on_bad_lines='skip').fillna("")
                else:
                    return pd.read_excel(p, dtype=str).fillna("")
            except Exception as e:
                st.error(f"Failed to read local file: {e}")
                st.exception(e)
                return pd.DataFrame()
        else:
            st.warning("Local path does not exist.")
            return pd.DataFrame()
    return pd.DataFrame()

def show_and_download_df(df, filename_prefix="result"):
    if df is None or df.empty:
        st.info("No rows to show.")
        return
    st.dataframe(df.head(200))
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, file_name=f"{filename_prefix}.csv", mime="text/csv")
    # Excel download
    towrite = io.BytesIO()
    df.to_excel(towrite, index=False, engine="openpyxl")
    towrite.seek(0)
    st.download_button("Download Excel", towrite.read(), file_name=f"{filename_prefix}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -------------------------
# Sidebar: tool selector
# -------------------------
st.sidebar.title("Select a tool")
tool = st.sidebar.selectbox("Choose tool", [
    "-- Select --",
    "Name & Address Match",
    "Salesforce Report Automation",
    "Salesforce Table Joining (SQL Runner)",
    "URL & Address Scraper",
    "VID Extraction"
])

st.title("Tools Dashboard")
st.markdown("Select a tool from the sidebar. The Name & Address Match tool is implemented; paste your other tool code into the corresponding sections.")

# -------------------------
# 1) Name & Address Match (your exact script integrated)
# -------------------------
if tool == "Name & Address Match":
    st.header("🔎 Name & Address Matching")
    st.markdown(
        "Upload an Excel/CSV file or enter a local file path (recommended for large files). "
        "Then choose whether to run name matching, address matching or both."
    )

    col1, col2 = st.columns([1, 1])

    with col1:
        upload = st.file_uploader("Upload file (CSV / XLSX)", type=["csv", "xlsx", "xls"], key="nm_upload")
    with col2:
        local_path = st.text_input("Or enter local file path (use this for large files)", "", key="nm_local_path")

    # Load file into DataFrame (prefer upload if provided)
    df = None
    if upload is not None:
        try:
            if upload.name.lower().endswith(".csv"):
                df = pd.read_csv(upload, dtype=str).fillna("")
            else:
                df = pd.read_excel(upload, dtype=str).fillna("")
            st.success(f"Loaded uploaded file: {upload.name} ({len(df)} rows)")
        except Exception as e:
            st.error(f"Error reading uploaded file: {e}")
    elif local_path:
        try:
            p = Path(local_path)
            if not p.exists():
                st.error("Local path does not exist. Check the path and try again.")
            else:
                if str(p).lower().endswith(".csv"):
                    df = pd.read_csv(p, dtype=str).fillna("")
                else:
                    df = pd.read_excel(p, dtype=str).fillna("")
                st.success(f"Loaded local file: {p.name} ({len(df)} rows)")
        except Exception as e:
            st.error(f"Error reading local file: {e}")

    if df is None:
        st.info("Upload a file or enter a local path to continue.")
        st.stop()

    # Column selectors
    st.subheader("Select columns")
    cols = list(df.columns)
    st.write("Detected columns:", cols)

    do_name_matching = st.checkbox("Do name matching", value=True)
    do_address_matching = st.checkbox("Do address matching", value=True)

    if not do_name_matching and not do_address_matching:
        st.warning("Select at least one: name matching or address matching.")
        st.stop()

    name_col1 = name_col2 = addr_col1 = addr_col2 = None
    if do_name_matching:
        name_col1 = st.selectbox("Full Name 1 column", options=cols, index=0, key="nm_name1")
        name_col2 = st.selectbox("Full Name 2 column", options=cols, index=1 if len(cols)>1 else 0, key="nm_name2")
    if do_address_matching:
        addr_col1 = st.selectbox("Full Address 1 column", options=cols, index=0, key="nm_addr1")
        addr_col2 = st.selectbox("Full Address 2 column", options=cols, index=1 if len(cols)>1 else 0, key="nm_addr2")

    run_button = st.button("Run Matching", key="nm_run")

    # --- helper functions (identical logic) ---
    import re
    from fuzzywuzzy import fuzz as _fuzz

    def preprocess_text(text):
        if isinstance(text, str):
            text = text.lower().strip()
            text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
            text = re.sub(r'\s+', ' ', text)
            return text
        return ''

    def extract_building_number(address):
        if not isinstance(address, str):
            return None
        match = re.search(r"\d+", address)
        return match.group() if match else None

    def fuzzy_match_local(str1, str2):
        if not str1 or not str2:
            return 0
        return _fuzz.token_set_ratio(str1, str2)

    def compare_building_and_postal(addr1, addr2):
        if not addr1 or not addr2:
            return "No"
        building1 = extract_building_number(addr1)
        building2 = extract_building_number(addr2)
        address_perc = fuzzy_match_local(addr1, addr2)
        if building1 and building2:
            return "Yes" if (building1 == building2 and address_perc > 70) else "No"
        return "No"

    def determine_final_outcome(row):
        if row.get("Name Match", "No") == "Yes" and row.get("Address Match", "No") == "Yes":
            return "Yes"
        return "No"

    # ===============================
    # Run logic and show result
    # ===============================
    if run_button:
        st.info("Processing... this may take time for large files.")
        working = df.copy()

        try:
            if do_name_matching:
                if name_col1 not in working.columns or name_col2 not in working.columns:
                    st.error("Selected name columns not found in data.")
                    st.stop()
                working["Name Match Percentage"] = working.apply(
                    lambda r: fuzzy_match_local(preprocess_text(r[name_col1]), preprocess_text(r[name_col2])), axis=1
                )
                working["Name Match"] = working["Name Match Percentage"].apply(lambda x: "Yes" if x >= 90 else "No")

            if do_address_matching:
                if addr_col1 not in working.columns or addr_col2 not in working.columns:
                    st.error("Selected address columns not found in data.")
                    st.stop()
                working["Address Match Percentage"] = working.apply(
                    lambda r: fuzzy_match_local(preprocess_text(r[addr_col1]), preprocess_text(r[addr_col2])), axis=1
                )
                working["Address Match"] = working.apply(
                    lambda r: compare_building_and_postal(r[addr_col1], r[addr_col2]), axis=1
                )

            if "Name Match" not in working.columns:
                working["Name Match"] = "No"
            if "Address Match" not in working.columns:
                working["Address Match"] = "No"

            working['Final Outcome'] = working.apply(determine_final_outcome, axis=1)

            st.success("Matching completed.")
            st.dataframe(working.head(50))

            # Download links
            csv_bytes = working.to_csv(index=False).encode('utf-8')
            file_stem = Path(upload.name if upload else local_path).stem if (upload or local_path) else "result"
            st.download_button("Download CSV", csv_bytes, file_name=f"Result_{file_stem}.csv", mime="text/csv")

            # Excel download: write to buffer
            towrite = io.BytesIO()
            working.to_excel(towrite, index=False, engine="openpyxl")
            towrite.seek(0)
            st.download_button("Download Excel", towrite, file_name=f"Result_{file_stem}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        except Exception as e:
            st.error(f"Error during matching: {e}")
            st.exception(e)

# -------------------------
# 2) Salesforce Report Automation (placeholder)
# -------------------------
elif tool == "Salesforce Report Automation":
    st.header("Salesforce Report Automation")
    st.info("Paste your Salesforce report downloader function into this panel (see TODO below).")
    st.markdown("**TODO:** paste the `run_salesforce_reports(...)` implementation here and call it on Run.")
    # Inputs
    sf_user = st.text_input("Salesforce username (email)", key="sfra_user")
    sf_pass = st.text_input("Salesforce password", type="password", key="sfra_pass")
    sf_token = st.text_input("Security token", type="password", key="sfra_token")
    folder_id = st.text_input("Folder ID (OwnerId)", key="sfra_folder")
    if st.button("Run Report Downloader (placeholder)"):
        st.info("This is a placeholder. Paste your run_salesforce_reports(...) function here to enable.")

# -------------------------
# 3) Salesforce Table Joining (SQL Runner) (placeholder)
# -------------------------
elif tool == "Salesforce Table Joining (SQL Runner)":
    st.header("Salesforce → DuckDB SQL Runner")
    st.info("Paste your `run_user_query_streamlit(...)` function into this panel to enable the SQL Runner.")
    sql_user = st.text_input("Salesforce username", key="sql_user")
    sql_pass = st.text_input("Salesforce password", type="password", key="sql_pass")
    sql_token = st.text_input("Security token", type="password", key="sql_token")
    ids_upload = st.file_uploader("Optional IDs CSV", type=["csv"], key="sql_ids_upload")
    ids_local = st.text_input("Or local IDs CSV path", key="sql_ids_local")
    sql_text = st.text_area("SQL (use aliases and uploaded_ids table if you uploaded IDs)", height=220, key="sql_box")
    if st.button("Run SQL (placeholder)"):
        st.info("Placeholder: paste your run_user_query_streamlit(...) function into this runner to execute it.")

# -------------------------
# 4) URL & Address Scraper (placeholder)
# -------------------------
elif tool == "URL & Address Scraper":
    st.header("URL & Address Scraper")
    st.info("Paste your URL scraper functions (DDG search, fetch, prioritize) into this panel.")
    upload = st.file_uploader("Upload input (CSV/XLSX) for scraping", type=["csv", "xlsx"], key="url_upload")
    local = st.text_input("Or local file path", key="url_local")
    url_boost = st.checkbox("Prioritize URLs that contain name", value=True)
    if st.button("Run URL Scraper (placeholder)"):
        st.info("Placeholder: paste your URL scraper and prioritization function here.")

# -------------------------
# 5) VID Extraction
# -------------------------
elif tool == "VID Extraction":
    st.header("VID Extraction")
    st.info("Upload network/extract file and choose pattern to extract VID-like values.")
    upload = st.file_uploader("Upload file (CSV/XLSX) for VID extraction", type=["csv", "xlsx"], key="vid_upload")
    local = st.text_input("Or enter local file path", key="vid_local")
    pattern = st.text_input("Regex to extract VID (example: \\bVID-?(\\d+)\\b )", value=r"\bVID[-_]?(?P<vid>\d{4,})\b")
    if st.button("Run VID Extraction"):
        df = load_dataframe(upload, local)
        if df.empty:
            st.warning("No input file.")
        else:
            import re
            pat = re.compile(pattern)
            extracted = []
            for idx, row in df.iterrows():
                found = ""
                for col in df.columns:
                    val = str(row[col])
                    m = pat.search(val)
                    if m:
                        if 'vid' in m.groupdict():
                            found = m.group('vid')
                        elif m.groups():
                            found = m.group(1)
                        else:
                            found = m.group(0)
                        break
                extracted.append(found)
            df["Extracted_VID"] = extracted
            show_and_download_df(df, filename_prefix="vid_extraction_result")

# -------------------------
# default: help
# -------------------------
else:
    st.write("Select a tool from the sidebar to get started.")
    st.write("The Name & Address Match tool is fully implemented. Paste your other tools' runner functions into the corresponding panels (they are marked TODO / placeholder).")
