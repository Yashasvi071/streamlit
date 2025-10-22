# app.py - Multi-tool Streamlit app with Name & Address Match,
# Salesforce Report Automation, Salesforce Table Joining (SQL Runner),
# and URL Scraper & Fuzzy Matching
#
# pip install streamlit pandas openpyxl fuzzywuzzy python-Levenshtein simple-salesforce duckdb sqlglot requests beautifulsoup4 rapidfuzz ddgs

import streamlit as st
import pandas as pd
import io
from pathlib import Path
import traceback
import os
import warnings
warnings.filterwarnings("ignore")

# SQL runner / Salesforce imports
import re
import math
import duckdb
import sqlglot
from sqlglot import parse_one
from simple_salesforce import Salesforce

# Misc used by report automation
import requests
from datetime import datetime
import shutil
import json

# URL scraper imports (DDG + BeautifulSoup + fuzzy)
try:
    from ddgs import DDGS
except Exception:
    DDGS = None
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote
import string
from fuzzywuzzy import fuzz

# used by vid extraction
import numpy as np
from rapidfuzz import fuzz, process as rfproc
from tqdm import tqdm

# Ensure Streamlit page config
st.set_page_config(page_title="Tools Dashboard", layout="wide")
#   st.set_option('server.maxUploadSize', 1024)  # MB; increase if you need larger uploads

# -------------------------
# UI helpers
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
    "URL Scraper & Fuzzy Matching",
    "Salesforce Report Automation",
    "Salesforce Table Joining (SQL Runner)"
    "VID EXTRACTION"
])

st.title("Tools Dashboard")
st.markdown("Select a tool from the sidebar. Implemented: Name & Address Match, URL Scraper & Fuzzy Matching, Salesforce Report Automation, Salesforce Table Joining (SQL Runner).")

# -------------------------
# 1) Name & Address Match (your script)
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
    import re as _re
    from fuzzywuzzy import fuzz as _fuzz

    def preprocess_text(text):
        if isinstance(text, str):
            text = text.lower().strip()
            text = _re.sub(r'[^a-zA-Z0-9\s]', '', text)
            text = _re.sub(r'\s+', ' ', text)
            return text
        return ''

    def extract_building_number(address):
        if not isinstance(address, str):
            return None
        match = _re.search(r"\d+", address)
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

    # Run logic and show result
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
# 2) URL Scraper & Fuzzy Matching
# -------------------------
elif tool == "URL Scraper & Fuzzy Matching":
    st.header("📌 URL Scraper & Name/Address Matching")

    # Check ddgs availability
    #try:
        #DDGS  # noqa: F841
    #except Exception:
     #   st.error("Package `ddgs` not available. Install with `pip install ddgs` and restart the app.")
      #  st.stop()

    uploaded_file = st.file_uploader("Upload CSV/XLSX file", type=['csv','xls','xlsx'], key="url_upload")
    script_option = st.selectbox("Choose Script", ["Select","Script 1: DuckDuckGo","Script 2: Fuzzy Matching"], key="url_script_select")

    if uploaded_file is not None and script_option != "Select":
        # load dataframe
        try:
            if uploaded_file.name.lower().endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str).fillna("")
            else:
                xls = pd.ExcelFile(uploaded_file)
                sheet = st.selectbox("Select Sheet", xls.sheet_names, key="url_sheet_select")
                df = pd.read_excel(xls, sheet_name=sheet, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Could not read uploaded file: {e}")
            st.exception(e)
            st.stop()

        st.write("Columns detected:", df.columns.tolist())

        # config UI
        col1, col2 = st.columns(2)
        with col1:
            name_col = st.selectbox("Name Column", df.columns, key="url_name_col")
            addr_col = st.selectbox("Address Column", df.columns, key="url_addr_col")
        with col2:
            url_boost = st.checkbox("Prioritize URLs containing name", value=True, key="url_boost")
            max_results = st.number_input("DDG results per query", value=5, min_value=1, max_value=20, key="url_max_results")
            verify_fetch = st.checkbox("Verify candidates by fetching page", value=True, key="url_verify_fetch")

        # initialize per-run session state keys (preserve across reruns)
        if "url_running" not in st.session_state:
            st.session_state.url_running = False
        if "url_stop" not in st.session_state:
            st.session_state.url_stop = False
        if "url_index" not in st.session_state:
            st.session_state.url_index = 0
        if "url_rows" not in st.session_state:
            st.session_state.url_rows = []
        if "url_total" not in st.session_state:
            st.session_state.url_total = len(df)

        run_btn = st.button("Run URL Script", key="url_run")

        # Stop button (visible only while running)
        stop_col, resume_col, dl_col = st.columns([1,1,1])
        stop_clicked = False
        resume_clicked = False

        if st.session_state.url_running:
            # show stop button when running
            with stop_col:
                if st.button("STOP URL Script", key="stop_url_script"):
                    st.session_state.url_stop = True
                    st.warning("Stop requested — loop will stop at next iteration.")
                    stop_clicked = True

        # Resume button appears only after stop and there are remaining rows
        remaining = len(df) - st.session_state.url_index
        if (not st.session_state.url_running) and st.session_state.url_stop and st.session_state.url_index < len(df):
            with resume_col:
                if st.button("Resume URL Script", key="resume_url_script"):
                    st.session_state.url_stop = False
                    st.session_state.url_running = True
                    resume_clicked = True

        # Download processed rows mid-way (visible after some rows processed)
        if st.session_state.url_rows:
            with dl_col:
                if st.download_button("Download Processed Rows (XLSX)", data=pd.DataFrame(st.session_state.url_rows).to_excel(index=False, engine="openpyxl"), file_name="url_processed_rows.xlsx"):
                    pass  # streamlit handles download

        # local ddg and cache instance (kept per run)
        _local_ddgs = DDGS()
        _query_cache_local = {}

        # constants (use widget values)
        VERIFY_CANDIDATES = True
        MAX_RESULTS_DDG = 5
        FETCH_TIMEOUT = 8
        PHONE_BOOST = 20
        ADDRESS_BOOST = 15
        DOMAIN_PENALTY = 30
        NAME_CONTAINS_BOOST = 45
        NAME_IN_URL_BOOST = 40
        MIN_ACCEPT_SCORE = 50
        SCRAPE_ADDR_THRESHOLD = 55
        SCRAPE_NAME_THRESHOLD = 40

        ALWAYS_UNOFFICIAL_DOMAINS = {
            'webmd.com', 'www.webmd.com',
            'npiregistry.cms.hhs.gov', 'nppes.cms.hhs.gov', 'npiregistry', 'npi.registry', 'npi-registry',
            'healthgrades.com', 'www.healthgrades.com',
            'health.usnews.com', 'www.health.usnews.com',
            'npidb.com', 'www.npidb.com',
        }

        blocked_domains = {
            'www.mapquest.com', 'www.blockedsite.org', 'spam-site.com'
        }

        blocked_urls = {
            'https://specific-url-to-ignore.com/path',
            'https://another-url.com'
        }

        unofficial_domains =  {'www.yelp.com', 'www.nppes.cms.hhs.gov', 'www.opennpi.com', 'www.npidb.org', 'www.hipaaspace.com',
            'www.doctorsdig.com', 'doctor.webmd.com', 'www.caredash.com', 'www.ratemds.com', 'www.healthcare.com',
            'www.healthinsurance.org', 'www.medicarelist.com', 'www.medicarelawsuit.com', 'www.hospitalinspections.org',
            'www.malpracticecenter.com', 'www.healthprofs.com', 'www.naturalhealthfinder.com', 'www.docinfo.org',
            'www.sharecare.com', 'www.healthgrades.com', 'www.findatopdoc.com', 'www.ahrq.gov', 'www.cdc.gov',
            'www.hcai.ca.gov', 'www.hcup-us.ahrq.gov', 'www.toprntobsn.com', 'www.mymedicarematters.org',
            'www.consumeraffairs.com', 'www.patientadvocate.org', 'www.propublica.org', 'www.medlaw.com',
            'www.hospitalsafetygrade.org', 'www.sciencedirect.com', 'www.jamanetwork.com', 'www.nejm.org',
            'www.justdial.com', 'www.practo.com', 'www.lybrate.com', 'www.medigence.com', 'www.bing.com',
            'www.chamberofcommerce.com', 'www.realtor.com', 'www.mapquest.com', 'health.usnews.com', 'healthcarecomps.com','www.doximity.com'}

        # helper funcs (copied/preserved)
        def normalize_text_local(s):
            if not s:
                return ""
            s = str(s).lower()
            s = unquote(s)
            s = s.translate(str.maketrans(string.punctuation, " " * len(string.punctuation)))
            return re.sub(r'\s+', ' ', s).strip()

        def url_normalized_text_local(url):
            try:
                p = urlparse(url)
                parts = []
                if p.netloc:
                    parts.append(p.netloc)
                if p.path:
                    parts.append(p.path)
                if p.query:
                    parts.append(p.query)
                return normalize_text_local(" ".join(parts))
            except:
                return normalize_text_local(url)

        def fetch_page_local(url, timeout=FETCH_TIMEOUT):
            out = {"url": url, "title": "", "text": "", "phones": [], "org_name": None}
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                resp = requests.get(url, headers=headers, timeout=timeout)
                if resp.status_code != 200:
                    return out
                soup = BeautifulSoup(resp.text, "html.parser")
                out["title"] = soup.title.string.strip() if soup.title else ""
                out["text"] = soup.get_text(separator=" ", strip=True).lower()
                out["phones"] = list(dict.fromkeys(re.findall(r'\+?\d[\d\-\s().]{7,}\d', out["text"])))
                return out
            except:
                return out

        def search_duckduckgo_urls_data_local(query, max_results=MAX_RESULTS_DDG):
            key = f"{query}||{max_results}"
            if key in _query_cache_local:
                return _query_cache_local[key]
            urls_data = []
            try:
                results = _local_ddgs.text(query, max_results=max_results)
                for r in results:
                    urls_data.append({"url": r.get("href",""), "title": r.get("title",""), "body": r.get("body","")})
            except Exception as e:
                st.warning(f"DDG error for query '{query}': {e}")
            _query_cache_local[key] = urls_data
            return urls_data

        def get_domain_local(url):
            try:
                return urlparse(url).netloc.lower()
            except:
                return ''

        def get_building_number_local(text):
            street_keywords = ["Street","St","Avenue","Ave","Road","Rd","Drive","Dr","Boulevard","Blvd","Lane","Ln"]
            pattern = r'\b(\d{2,6})\s+(?:' + '|'.join(street_keywords) + r')\b'
            match = re.search(pattern, str(text), re.IGNORECASE)
            if match:
                return match.group(1)
            return ''

        def extract_phone_local(text):
            match = re.search(r'\+?\d[\d\-\s().]{7,}\d', str(text))
            return match.group() if match else ""

        def score_candidate_local(candidate, target_name, target_address, original_bldg, prioritize_name_in_url=True):
            snippet_title = candidate.get("title","")
            snippet_body = candidate.get("body","")
            name_score_title = fuzz.token_set_ratio(target_name.lower(), snippet_title.lower()) if snippet_title else 0
            name_score_body = fuzz.token_set_ratio(target_name.lower(), snippet_body.lower()) if snippet_body else 0
            addr_score_body = fuzz.token_set_ratio(target_address.lower(), snippet_body.lower()) if snippet_body else 0
            score = int(0.6 * max(name_score_title, name_score_body) + 0.3 * addr_score_body)
            norm_target = normalize_text_local(target_name)
            if norm_target and (norm_target in normalize_text_local(snippet_title) or norm_target in normalize_text_local(snippet_body)):
                score += NAME_CONTAINS_BOOST
            url = candidate.get("url","")
            if prioritize_name_in_url and url and norm_target and norm_target in url_normalized_text_local(url):
                score += NAME_IN_URL_BOOST
            if VERIFY_CANDIDATES and url:
                page = fetch_page_local(url)
                title_score = fuzz.token_set_ratio(target_name.lower(), (page.get("title") or "").lower()) if page.get("title") else 0
                body_score = fuzz.token_set_ratio(target_name.lower(), (page.get("text") or "").lower()) if page.get("text") else 0
                score = max(score, int(0.5*max(title_score, body_score)))
            domain = get_domain_local(url)
            if domain in unofficial_domains:
                score -= DOMAIN_PENALTY
            return score

        def prioritize_links_local(links, name="", address="", original_bldg=""):
            scored = []
            for l in links:
                sc = score_candidate_local(l, name, address, original_bldg, prioritize_name_in_url=url_boost)
                scored.append((sc, l.get("url","")))
            if not scored:
                return ""
            scored.sort(key=lambda x: x[0], reverse=True)
            return scored[0][1]

        # run/resume logic
        # When Run clicked: reset and start from 0
        if run_btn:
            st.session_state.url_running = True
            st.session_state.url_stop = False
            st.session_state.url_index = 0
            st.session_state.url_rows = []
            st.session_state.url_total = len(df)

        # When resume clicked the session_state.url_running will already be set above when clicked
        # Main processing loop: will run when url_running True
        if st.session_state.url_running:
            progress_text = st.empty()
            progress_bar = st.progress(0)
            total = len(df)
            # Ensure url_total sync
            st.session_state.url_total = total

            # Start processing from saved index
            start_idx = int(st.session_state.url_index or 0)
            rows_out = list(st.session_state.url_rows or [])

            for idx in range(start_idx, total):
                # check stop flag
                if st.session_state.url_stop:
                    progress_text.text(f"🛑 Stopped at row {idx}/{total}")
                    # save state
                    st.session_state.url_index = idx
                    st.session_state.url_rows = rows_out
                    st.session_state.url_running = False
                    break

                row = df.iloc[idx]
                name = str(row.get(name_col, "") or "")
                address = str(row.get(addr_col, "") or "")
                bldg = get_building_number_local(address)
                query = f"{name} {address}"
                urls = search_duckduckgo_urls_data_local(query, max_results=MAX_RESULTS_DDG)
                official_candidates = [u for u in urls if get_domain_local(u.get("url","")) not in unofficial_domains]
                unofficial_candidates = [u for u in urls if get_domain_local(u.get("url","")) in unofficial_domains]
                found_official = prioritize_links_local(official_candidates, name, address, bldg)
                found_unofficial = prioritize_links_local(unofficial_candidates, name, address, bldg)
                found_contact = ""
                if found_official:
                    p = fetch_page_local(found_official) if VERIFY_CANDIDATES else {}
                    found_contact = extract_phone_local(p.get("text","")) if p else ""
                if not found_contact and found_unofficial:
                    p = fetch_page_local(found_unofficial) if VERIFY_CANDIDATES else {}
                    found_contact = extract_phone_local(p.get("text","")) if p else ""

                out_row = row.to_dict()
                out_row["official link"] = found_official
                out_row["unofficial link"] = found_unofficial
                out_row["contact number"] = found_contact
                rows_out.append(out_row)

                # update UI and session progress
                st.session_state.url_index = idx + 1
                st.session_state.url_rows = rows_out
                progress_text.text(f"Processing {idx+1}/{total}: {name}")
                progress_bar.progress(int((idx+1)/total*100))

            else:
                # Completed loop without break
                st.session_state.url_running = False
                st.session_state.url_stop = False
                st.session_state.url_index = total

            # After loop finishes or stops, show result & download button
            result_df = pd.DataFrame(st.session_state.url_rows)
            if not result_df.empty:
                st.success("✅ Script finished or stopped.")
                st.dataframe(result_df.head(200))
                # downloads (XLSX)
                towrite = io.BytesIO()
                result_df.to_excel(towrite, index=False, engine="openpyxl")
                towrite.seek(0)
                st.download_button("Download Processed Rows (XLSX)", towrite.read(), file_name="url_processed_rows.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("No rows processed yet.")

    else:
        st.info("Upload a CSV/XLSX file and choose a script to run.")


# -------------------------
# 3) Salesforce Report Automation (your script)
# -------------------------
elif tool == "Salesforce Report Automation":
    st.header("📥 Salesforce Report Downloader")
    st.write(
        "Enter Salesforce credentials and the *Folder ID* containing reports. "
        "This will fetch each report's JSON, parse detail rows, save each report as XLSX and provide a zip to download."
    )

    # --- Inputs ---
    with st.form("sf_form"):
        col1, col2 = st.columns(2)
        with col1:
            sf_username = st.text_input("Salesforce username (email)", value="", placeholder="user@example.com")
            sf_password = st.text_input("Salesforce password", type="password")
        with col2:
            sf_token = st.text_input("Salesforce security token", type="password")
            folder_id = st.text_input("Salesforce Folder ID (e.g., 00l...)", placeholder="00lXXXXXXXXXXXXXXX")
        submit = st.form_submit_button("Connect & Run")

    # UI placeholders
    log_area = st.empty()
    progress_bar = st.progress(0)
    download_placeholder = st.empty()

    # Download helper (unchanged logic)
    def download_report_json(sf, headers, report_id):
        url = f"https://{sf.sf_instance}/services/data/v58.0/analytics/reports/{report_id}"
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        report_json = response.json()
        try:
            fact_map = report_json.get('factMap', {})
            detail_columns = report_json.get('reportMetadata', {}).get('detailColumns', [])
            # detail_columns may be a list of API names or dicts with label
            columns = [col['label'] if isinstance(col, dict) and 'label' in col else col for col in detail_columns]

            data_rows = None
            for key, value in fact_map.items():
                if isinstance(value, dict) and 'rows' in value and value['rows']:
                    data_rows = value['rows']
                    break

            if not data_rows:
                return None, "no_rows"

            data = []
            for row in data_rows:
                if isinstance(row, dict) and 'dataCells' in row:
                    data.append([cell.get('label', '') for cell in row['dataCells']])
            if not data:
                return None, "no_data_cells"

            df = pd.DataFrame(data, columns=columns)
            return df, None
        except Exception as e:
            return None, str(e)

    def run_salesforce_reports(username, password, token, folder_id, log_fn=None):
        """
        Connects to Salesforce, finds reports in folder (OwnerId), downloads each report JSON,
        parses detail rows, writes Excel files into an output folder and zips them.
        Returns path to zip or raises.
        """
        # Connect
        sf = Salesforce(username=username, password=password, security_token=token, domain='login')

        headers = {'Authorization': 'Bearer ' + sf.session_id, 'Content-Type': 'application/json'}

        today = datetime.today().strftime('%d-%m-%Y')
        output_folder = f"Salesforce_Reports_{today}"
        os.makedirs(output_folder, exist_ok=True)

        report_query = f"""
            SELECT Id, Name
            FROM Report
            WHERE OwnerId = '{folder_id}'
        """
        result = sf.query_all(report_query)
        report_results = result.get('records', [])

        if log_fn:
            log_fn(f"Found {len(report_results)} reports in folder (OwnerId={folder_id}).")

        if not report_results:
            # nothing to do
            return None, output_folder

        total = len(report_results)
        processed = 0
        for idx, report in enumerate(report_results, start=1):
            report_id = report.get("Id")
            report_name = (report.get("Name") or "unnamed").replace(" ", "_").replace("/", "-")
            if log_fn:
                log_fn(f"[{idx}/{total}] Processing: {report_name} ({report_id}) ...")

            df, err = download_report_json(sf, headers, report_id)
            if df is None:
                if log_fn:
                    log_fn(f"  ⚠️ Skipped (no usable data): {err}")
                processed += 1
                progress = int(processed / total * 100)
                progress_bar.progress(progress)
                continue

            # Format date columns
            for col in df.columns:
                try:
                    parsed = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                    if parsed.notna().sum() > 0:
                        df[col] = parsed.dt.strftime('%d/%m/%Y')
                except Exception:
                    pass

            # Rename column if needed
            df.rename(columns={"ACCOUNT_ID": "Salesforce ID", "ID": "Salesforce ID"}, inplace=True)

            # Handle 'Account.Active__c' if present
            if 'Account.Active__c' in df.columns:
                try:
                    if df['Account.Active__c'].dtype == object:
                        df['Account.Active__c'] = df['Account.Active__c'].str.strip().str.lower().replace({'yes': 1, 'no': 0})
                except Exception as e:
                    if log_fn:
                        log_fn(f"  ⚠️ Skipping 'Active' column conversion: {e}")

            output_filename = f"{report_name}_{today}.xlsx"
            output_path = os.path.join(output_folder, output_filename)
            try:
                df.to_excel(output_path, index=False, engine="openpyxl")
                if log_fn:
                    log_fn(f"  ✅ Saved: {output_path}")
            except Exception as e:
                if log_fn:
                    log_fn(f"  ❌ Failed to save {output_path}: {e}")

            processed += 1
            progress = int(processed / total * 100)
            progress_bar.progress(progress)

        # Zip the folder
        try:
            zip_base_name = os.path.abspath(output_folder)
            archive_path = shutil.make_archive(zip_base_name, 'zip', output_folder)
            if log_fn:
                log_fn(f"\n📦 Reports zipped to: {archive_path}")
            return archive_path, output_folder
        except Exception as e:
            if log_fn:
                log_fn(f"❌ Failed to create zip archive: {e}")
            raise

    # Run on submit
    if submit:
        # basic validation
        if not (sf_username and sf_password and sf_token and folder_id):
            st.error("Please fill username/password/security token and folder id.")
        else:
            log_lines = []
            def log_fn(msg):
                log_lines.append(str(msg))
                log_area.text("\n".join(log_lines[-50:]))

            try:
                with st.spinner("Connecting to Salesforce and fetching reports..."):
                    archive_path, out_folder = run_salesforce_reports(sf_username, sf_password, sf_token, folder_id, log_fn=log_fn)
                progress_bar.progress(100)
                if archive_path and os.path.exists(archive_path):
                    with open(archive_path, "rb") as f:
                        bytes_data = f.read()
                    download_placeholder.download_button(
                        label="⬇️ Download ZIP of reports",
                        data=bytes_data,
                        file_name=os.path.basename(archive_path),
                        mime="application/zip"
                    )
                    st.success(f"Completed — zip ready: {os.path.basename(archive_path)}")
                    st.info(f"Excel files are under folder: `{out_folder}` (on server)")
                else:
                    st.warning("No reports were found or no Excel outputs were created.")
            except Exception as e:
                st.error(f"Error: {e}")
                log_fn(str(e))

# -------------------------
# 4) Salesforce Table Joining (SQL Runner)
# -------------------------
elif tool == "Salesforce Table Joining (SQL Runner)":
    st.header("🔁 Salesforce → DuckDB SQL Runner")
    st.markdown(
        "Connect to Salesforce, optionally upload a CSV of IDs (or provide a local path), "
        "enter SQL (use aliases), and run. The app will fetch only necessary fields and run the SQL in DuckDB."
    )

    # ---------------------------
    # Inputs: Salesforce login
    # ---------------------------
    with st.expander("Salesforce credentials & IDs (expand) — required"):
        col1, col2, col3 = st.columns(3)
        with col1:
            sf_username = st.text_input("Username (email)", value="", placeholder="user@example.com", key="sql_user")
            sf_password = st.text_input("Password", type="password", key="sql_pass")
            sf_token = st.text_input("Security token", type="password", key="sql_token")
        with col2:
            st.markdown("**Optional: IDs CSV**")
            ids_file = st.file_uploader("Upload IDs CSV (optional)", type=["csv"], accept_multiple_files=False, key="ids_upload")
            ids_local_path = st.text_input("Or enter local IDs CSV path (optional)", value="", key="ids_local")
        with col3:
            st.markdown("**SQL Input**")
            query_box = st.text_area(
                "Write SQL here (use aliases and refer to uploaded_ids if you uploaded IDs).",
                height=220,
                key="sql_query_box"
            )
            run_button = st.button("Run Query", type="primary", key="sql_run")

    # Status / logs
    log_area = st.empty()
    progress = st.progress(0)

    # ---------- Small streamlit-aware logger with .info(...) used by runner ----------
    class StreamlitLogger:
        def __init__(self, placeholder):
            self.placeholder = placeholder
            self.lines = []
        def info(self, msg):
            self.lines.append(str(msg))
            self.placeholder.text("\n".join(self.lines[-200:]))

    log = StreamlitLogger(log_area)

    # ---------------------------
    # Helper functions (kept from your salesforce_sql_streamlit script)
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

    def describe_fields_for_object(sobject, sf_conn):
        desc = getattr(sf_conn, sobject).describe()
        return [f['name'] for f in desc['fields']]

    def get_reference_fields_map(sobject, sf_conn):
        desc = getattr(sf_conn, sobject).describe()
        ref_map = {}
        for f in desc['fields']:
            if f['type'] == 'reference':
                ref_map[f['name']] = {
                    'referenceTo': f.get('referenceTo', []),
                    'relationshipName': f.get('relationshipName')
                }
        return ref_map

    def fetch_object_data(sf_conn, sobject, soql_fields_set, id_list_for_object=None, id_filter_field='Id', batch_size=2000):
        # expand __ALL__
        if '__ALL__' in soql_fields_set:
            fields = describe_fields_for_object(sobject, sf_conn)
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
                    recs = sf_conn.query_all(soql)['records']
                except Exception:
                    if len(chunk) > 100:
                        half = math.ceil(len(chunk)/2)
                        recs = []
                        for j in range(0, len(chunk), half):
                            sub = chunk[j:j+half]
                            ids_sub = ",".join(repr(x) for x in sub)
                            soql_sub = f"SELECT {select_clause} FROM {sobject} WHERE {id_filter_field} IN ({ids_sub})"
                            recs.extend(sf_conn.query_all(soql_sub)['records'])
                    else:
                        raise
                all_records.extend(recs)
        else:
            soql = f"SELECT {select_clause} FROM {sobject}"
            all_records = sf_conn.query_all(soql)['records']

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

    # The main runner (slightly adapted to use the StreamlitLogger & progress)
    def run_user_query_streamlit(sf_conn, uploaded_ids_df, id_object_value, sql_text):
        alias_real_pairs = parse_table_aliases(sql_text)
        if not alias_real_pairs:
            raise ValueError("No table references found in query.")

        sf_alias_real_pairs = [(alias, real) for alias, real in alias_real_pairs if real.lower() != 'uploaded_ids']
        all_cols = extract_all_columns(sql_text)
        mapping = build_fields_to_fetch(sql_text, sf_alias_real_pairs, all_cols)
        alias_to_real = {alias: real for alias, real in sf_alias_real_pairs}

        id_map = {}
        uploaded_ids_list = uploaded_ids_df['Id'].tolist() if (uploaded_ids_df is not None and not uploaded_ids_df.empty) else None

        # precompute reference maps
        ref_maps = {}
        for alias, real in sf_alias_real_pairs:
            try:
                ref_maps[real] = get_reference_fields_map(real, sf_conn)
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
            log.info(f"⏳ Fetching {real} (alias {alias}) ...")
            df_obj = fetch_object_data(sf_conn, real, soql_fields, id_list_for_object=ids_for_obj, id_filter_field=id_filter_field)
            dfs[alias] = df_obj
            step += 1
            try:
                progress.progress(int(step/total*50))  # fetching step (50% of progress)
            except Exception:
                pass

        # Prepare DuckDB
        con = duckdb.connect()
        if uploaded_ids_list:
            try:
                con.register("uploaded_ids", uploaded_ids_df)
            except Exception:
                pass

        for alias, real in alias_real_pairs:
            if real.lower() == 'uploaded_ids':
                continue
            df_reg = dfs.get(alias, pd.DataFrame())
            try:
                con.register(alias, df_reg)
            except Exception:
                try:
                    con.unregister(alias)
                    con.register(alias, df_reg)
                except Exception:
                    pass
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

        try:
            progress.progress(75)
        except Exception:
            pass

        # Execute SQL
        try:
            result = con.execute(new_sql).df()
        except Exception as e:
            raise RuntimeError(f"Error executing SQL in DuckDB: {e}")

        try:
            progress.progress(100)
        except Exception:
            pass

        return result

    # ---------------------------
    # Streamlit runtime: handle login & run
    # ---------------------------
    if 'sf_conn' not in st.session_state:
        st.session_state.sf_conn = None
    if 'sobjects' not in st.session_state:
        st.session_state.sobjects = []

    # Connect button UI & logic
    if st.button("Connect to Salesforce", key="connect_sql"):
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
        id_object = st.selectbox("IDs belong to (select object for uploaded IDs)", options=["(none)"] + st.session_state.sobjects, index=0, key="id_object_after_connect")
    else:
        id_object = st.selectbox("IDs belong to (select object for uploaded IDs)", options=["(none)"], index=0, key="id_object_after_connect_none")

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
                log.info("Starting fetch & query...")
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
elif tool == 'VID EXTRACTION':
    st.header('VID Extraction using network and user file')
    st.markdown("Upload the user and the network file," \
    "Then select the columns for the VID number extraction") 
    # Helper Functions
# -----------------------------
def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = "".join(filter(str.isalnum, s))
    s = " ".join(s.split())
    return s.lower()

def safe_join(row, cols, sep=" "):
    return sep.join([str(row[c]) if pd.notnull(row[c]) else "" for c in cols]).strip()

def calculate_field_score(row_A, row_B, cols_A, cols_B):
    scores = []
    cols_A_safe = [c for c in cols_A if c in row_A]
    cols_B_safe = [c for c in cols_B if c in row_B]
    for col_A, col_B in zip(cols_A_safe, cols_B_safe):
        str_A = normalize_text(row_A[col_A])
        str_B = normalize_text(row_B[col_B])
        if str_A or str_B:
            score = fuzz.token_sort_ratio(str_A, str_B) if str_A and str_B else 0
            scores.append(score)
    return np.mean(scores) if scores else 0

def strict_postal_check(row_A, row_B, pc_A, pc_B):
    if not pc_A or not pc_B: 
        return True
    pc_A_norm = normalize_text(row_A.get(pc_A, ''))
    pc_B_norm = normalize_text(row_B.get(pc_B, ''))
    if not pc_A_norm and not pc_B_norm:
        return True
    return pc_A_norm == pc_B_norm

# -----------------------------
# Fuzzy Match Function
# -----------------------------
def perform_match(A_df, B_filtered, name_cols_A, name_cols_B, addr_cols_A, addr_cols_B, postal_col_A, postal_col_B, name_threshold, addr_threshold):
    matched_pairs = []

    # Blocking key
    def block_key(s):
        s = normalize_text(s)
        return s[:3] if len(s) >= 3 else s

    A_df["_block_key"] = A_df["_comp_key_A"].apply(block_key)
    B_filtered["_block_key"] = B_filtered["_comp_key_B"].apply(block_key)
    B_groups = B_filtered.groupby("_block_key")

    for bk in tqdm(A_df["_block_key"].unique(), desc="Fuzzy matching"):
        A_block = A_df[A_df["_block_key"] == bk]
        if bk not in B_groups.groups:
            continue
        B_block = B_filtered.loc[B_groups.groups[bk]]
        A_keys = A_block["_comp_key_A"].tolist()
        B_keys = B_block["_comp_key_B"].tolist()

        results = rfproc.extract_one_per_row(A_keys, B_keys, scorer=fuzz.token_sort_ratio,
                                             score_cutoff=min(name_threshold, addr_threshold))
        for a_idx, (score, b_idx_in_block) in enumerate(results):
            if score >= min(name_threshold, addr_threshold):
                arow = A_block.iloc[a_idx]
                brow = B_block.iloc[b_idx_in_block]

                if not strict_postal_check(arow, brow, postal_col_A, postal_col_B):
                    continue

                name_score = calculate_field_score(arow, brow, name_cols_A, name_cols_B)
                addr_score = calculate_field_score(arow, brow, addr_cols_A, addr_cols_B)

                if name_score >= name_threshold and addr_score >= addr_threshold:
                    matched_pairs.append({
                        "a_index": int(arow["_a_idx"]),
                        "b_index": int(brow["_b_idx"]),
                        "Name_Avg_%": name_score,
                        "Addr_Avg_%": addr_score
                    })

    matched_df = pd.DataFrame(matched_pairs)
    return matched_df

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("📊 Fuzzy Match Tool")

# File uploads
file_A = st.file_uploader("Upload User File (A)", type=["csv", "xlsx"])
file_B = st.file_uploader("Upload Network File (B)", type=["csv", "xlsx"])

if file_A and file_B:
    # Load data
    try:
        df_A = pd.read_csv(file_A, dtype=str) if str(file_A).endswith(".csv") else pd.read_excel(file_A, dtype=str)
        df_B = pd.read_csv(file_B, dtype=str) if str(file_B).endswith(".csv") else pd.read_excel(file_B, dtype=str)
        df_A.columns = df_A.columns.str.strip()
        df_B.columns = df_B.columns.str.strip()

        st.success("✅ Files loaded successfully.")
    except Exception as e:
        st.error(f"❌ Error loading files: {e}")
        st.stop()

    # Column selectors
    st.subheader("Select Columns for Matching")
    name_cols_A = st.multiselect("Name Columns (A)", df_A.columns.tolist())
    addr_cols_A = st.multiselect("Address Columns (A)", df_A.columns.tolist())
    postal_col_A = st.selectbox("Postal Code Column (A)", [""] + df_A.columns.tolist())
    postal_col_A = postal_col_A if postal_col_A else None

    name_cols_B = st.multiselect("Name Columns (B)", df_B.columns.tolist())
    addr_cols_B = st.multiselect("Address Columns (B)", df_B.columns.tolist())
    postal_col_B = st.selectbox("Postal Code Column (B)", [""] + df_B.columns.tolist())
    postal_col_B = postal_col_B if postal_col_B else None

    # Status filters
    st.subheader("Filter Network File (B)")
    status_col_hcp_B = st.selectbox("HCP Status Column (B)", [""] + df_B.columns.tolist())
    status_val_hcp_B = st.text_input("HCP Status Filter Value")
    status_col_addr_B = st.selectbox("Address Status Column (B)", [""] + df_B.columns.tolist())
    status_val_addr_B = st.text_input("Address Status Filter Value")

    # Threshold sliders
    st.subheader("Matching Thresholds")
    name_threshold = st.slider("Name Avg % ≥ ", 0, 100, 49)
    addr_threshold = st.slider("Address Avg % ≥ ", 0, 100, 52)

    if st.button("Run Fuzzy Match"):
        # Prepare Data
        df_A = df_A.reset_index().rename(columns={"index": "_a_idx"})
        df_B = df_B.reset_index().rename(columns={"index": "_b_idx"})

        all_cols_A = name_cols_A + addr_cols_A + ([postal_col_A] if postal_col_A else [])
        all_cols_B = name_cols_B + addr_cols_B + ([postal_col_B] if postal_col_B else [])

        df_A["_comp_key_A"] = df_A.apply(lambda r: normalize_text(" ".join([str(r[c]) for c in all_cols_A if c in r])), axis=1)
        df_B["_comp_key_B"] = df_B.apply(lambda r: normalize_text(" ".join([str(r[c]) for c in all_cols_B if c in r])), axis=1)

        # Filter B
        B_filtered = df_B[
            (df_B[status_col_hcp_B].astype(str).str.lower() == status_val_hcp_B.lower()) &
            (df_B[status_col_addr_B].astype(str).str.lower() == status_val_addr_B.lower())
        ].copy()

        st.write(f"Filtered Network File size: {len(B_filtered)}")

        # Run matching
        matches = perform_match(df_A, B_filtered, name_cols_A, name_cols_B, addr_cols_A, addr_cols_B, postal_col_A, postal_col_B, name_threshold, addr_threshold)
        st.write(f"Total matches found: {len(matches)}")

        # Merge back for final report
        report_df = df_A.merge(matches, left_on="_a_idx", right_on="a_index", how="left")
        report_df["Match_Status"] = np.where(report_df["b_index"].notna(), "Found", "Not Found")

        st.dataframe(report_df.head(20))

        # Download buttons
        csv = report_df.to_csv(index=False).encode('utf-8')
        excel = report_df.to_excel("fuzzy_match.xlsx", index=False)  # temp file for download

        st.download_button("Download CSV Report", data=csv, file_name="fuzzy_match_report.csv", mime="text/csv")
        st.download_button("Download Excel Report", data=excel, file_name="fuzzy_match_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -------------------------
# default
# -------------------------
else:
    st.write("Select a tool from the sidebar to get started.")
    st.write("Implemented: Name & Address Match, URL Scraper & Fuzzy Matching, Salesforce Report Automation, Salesforce Table Joining (SQL Runner).")
