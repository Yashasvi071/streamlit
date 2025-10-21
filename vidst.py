# =============================
# Streamlit Fuzzy Match Web App
# =============================

# Install required packages if not already installed:
# pip install streamlit pandas openpyxl rapidfuzz tqdm numpy

import streamlit as st
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process as rfproc
from tqdm import tqdm
# -----------------------------
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
