# app.py
# Streamlit wrapper for your name & address matching logic
# pip install streamlit pandas openpyxl fuzzywuzzy python-Levenshtein

import streamlit as st
import pandas as pd
import re
from fuzzywuzzy import fuzz
from pathlib import Path
import io

st.set_page_config(layout="wide", page_title="Name & Address Matcher")

# ===============================
# 🔧 Utility functions (same logic)
# ===============================
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

def fuzzy_match(str1, str2):
    if not str1 or not str2:
        return 0
    return fuzz.token_set_ratio(str1, str2)

def compare_building_and_postal(addr1, addr2):
    if not addr1 or not addr2:
        return "No"
    building1 = extract_building_number(addr1)
    building2 = extract_building_number(addr2)
    address_perc = fuzzy_match(addr1, addr2)
    if building1 and building2:
        return "Yes" if (building1 == building2 and address_perc > 70) else "No"
    return "No"

def determine_final_outcome(row):
    if row.get("Name Match", "No") == "Yes" and row.get("Address Match", "No") == "Yes":
        return "Yes"
    return "No"

# ===============================
# UI
# ===============================
st.title("🔎 Name & Address Matching")

st.markdown(
    "Upload an Excel/CSV file or enter a local file path (recommended for large files). "
    "Then choose whether to run name matching, address matching or both."
)

col1, col2 = st.columns([1, 1])

with col1:
    upload = st.file_uploader("Upload file (CSV / XLSX)", type=["csv", "xlsx", "xls"])
with col2:
    local_path = st.text_input("Or enter local file path (use this for large files)", "")

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

do_name_matching = st.checkbox("Do name matching")
do_address_matching = st.checkbox("Do address matching")

if not do_name_matching and not do_address_matching:
    st.warning("Select at least one: name matching or address matching.")
    st.stop()

name_col1 = name_col2 = addr_col1 = addr_col2 = None
if do_name_matching:
    name_col1 = st.selectbox("Full Name 1 column", options=cols, index=0)
    name_col2 = st.selectbox("Full Name 2 column", options=cols, index=1 if len(cols)>1 else 0)
if do_address_matching:
    addr_col1 = st.selectbox("Full Address 1 column", options=cols, index=0)
    addr_col2 = st.selectbox("Full Address 2 column", options=cols, index=1 if len(cols)>1 else 0)

run_button = st.button("Run Matching")

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
                lambda r: fuzzy_match(preprocess_text(r[name_col1]), preprocess_text(r[name_col2])), axis=1
            )
            working["Name Match"] = working["Name Match Percentage"].apply(lambda x: "Yes" if x >= 90 else "No")

        if do_address_matching:
            if addr_col1 not in working.columns or addr_col2 not in working.columns:
                st.error("Selected address columns not found in data.")
                st.stop()
            working["Address Match Percentage"] = working.apply(
                lambda r: fuzzy_match(preprocess_text(r[addr_col1]), preprocess_text(r[addr_col2])), axis=1
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
        st.download_button("Download CSV", csv_bytes, file_name=f"Result_{Path(upload.name if upload else local_path).stem}.csv", mime="text/csv")

        # Excel download: write to buffer
        towrite = io.BytesIO()
        working.to_excel(towrite, index=False, engine="openpyxl")
        towrite.seek(0)
        st.download_button("Download Excel", towrite, file_name=f"Result_{Path(upload.name if upload else local_path).stem}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        st.error(f"Error during matching: {e}")
        raise
