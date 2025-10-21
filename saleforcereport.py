# salesforce_reports_streamlit.py
# pip install streamlit pandas openpyxl requests simple-salesforce

import streamlit as st
import pandas as pd
import requests
from simple_salesforce import Salesforce
from datetime import datetime
import warnings
import os
import shutil
import io
import json

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Salesforce Report Downloader", layout="wide")

st.title("📥 Salesforce Report Downloader")
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

# --- Run on submit ---
if submit:
    # basic validation
    if not (sf_username and sf_password and sf_token and folder_id):
        st.error("Please fill username/password/security token and folder id.")
    else:
        log_lines = []
        def log_fn(msg):
            log_lines.append(msg)
            # show last 20 log lines
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
