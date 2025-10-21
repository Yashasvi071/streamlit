# STREAMLIT VERSION OF URL SCRAPER & FUZZY MATCHING

import streamlit as st
import pandas as pd
import os
import io
import time
import random
import string
import re
import requests
from urllib.parse import urlparse, unquote
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
from ddgs import DDGS
from pathlib import Path

# =============================
# Config / Constants
# =============================
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
    'www.chamberofcommerce.com', 'www.realtor.com', 'www.mapquest.com', 'health.usnews.com', 'healthcarecomps.com','www.doximity.com'
}
_global_ddgs = DDGS()
_query_cache = {}

# =============================
# Helpers
# =============================
def normalize_text(s):
    if not s:
        return ""
    s = str(s).lower()
    s = unquote(s)
    s = s.translate(str.maketrans(string.punctuation, " "*len(string.punctuation)))
    return re.sub(r'\s+', ' ', s).strip()

def url_normalized_text(url):
    try:
        p = urlparse(url)
        parts = []
        if p.netloc:
            parts.append(p.netloc)
        if p.path:
            parts.append(p.path)
        if p.query:
            parts.append(p.query)
        return normalize_text(" ".join(parts))
    except:
        return normalize_text(url)

def fetch_page(url, timeout=FETCH_TIMEOUT):
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

def search_duckduckgo_urls_data(query, max_results=MAX_RESULTS_DDG):
    key = f"{query}||{max_results}"
    if key in _query_cache:
        return _query_cache[key]
    urls_data = []
    try:
        results = _global_ddgs.text(query, max_results=max_results)
        for r in results:
            urls_data.append({"url": r.get("href",""), "title": r.get("title",""), "body": r.get("body","")})
    except Exception as e:
        st.warning(f"DDG error for query '{query}': {e}")
    _query_cache[key] = urls_data
    return urls_data

def get_domain(url):
    try:
        return urlparse(url).netloc.lower()
    except:
        return ''

def get_building_number(text):
    street_keywords = ["Street","St","Avenue","Ave","Road","Rd","Drive","Dr","Boulevard","Blvd","Lane","Ln"]
    pattern = r'\b(\d{2,6})\s+(?:' + '|'.join(street_keywords) + r')\b'
    match = re.search(pattern, str(text), re.IGNORECASE)
    if match:
        return match.group(1)
    return ''

def extract_phone(text):
    match = re.search(r'\+?\d[\d\-\s().]{7,}\d', str(text))
    return match.group() if match else ""

def score_candidate(candidate, target_name, target_address, original_bldg, prioritize_name_in_url=True):
    snippet_title = candidate.get("title","")
    snippet_body = candidate.get("body","")
    score = int(0.6 * fuzz.token_set_ratio(target_name.lower(), snippet_title.lower())
                + 0.3 * fuzz.token_set_ratio(target_address.lower(), snippet_body.lower()))
    norm_target = normalize_text(target_name)
    if norm_target in normalize_text(snippet_title) or norm_target in normalize_text(snippet_body):
        score += NAME_CONTAINS_BOOST
    url = candidate.get("url","")
    if prioritize_name_in_url and norm_target and url and norm_target in url_normalized_text(url):
        score += NAME_IN_URL_BOOST
    if VERIFY_CANDIDATES and url:
        page = fetch_page(url)
        title_score = fuzz.token_set_ratio(target_name.lower(), (page.get("title") or "").lower())
        body_score = fuzz.token_set_ratio(target_name.lower(), (page.get("text") or "").lower())
        score = max(score, int(0.5*max(title_score, body_score)))
    domain = get_domain(url)
    if domain in unofficial_domains:
        score -= DOMAIN_PENALTY
    return score

def prioritize_links(links, name="", address="", original_bldg=""):
    scored = [(score_candidate(l, name, address, original_bldg), l.get("url","")) for l in links]
    if not scored:
        return ""
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]

# =============================
# STREAMLIT APP
# =============================
st.title("📌 URL Scraper & Name/Address Matching")

uploaded_file = st.file_uploader("Upload CSV/XLSX file", type=['csv','xls','xlsx'])
script_option = st.selectbox("Choose Script", ["Select","Script 1: DuckDuckGo","Script 2: Fuzzy Matching"])

if uploaded_file and script_option != "Select":
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str).fillna("")
    else:
        xls = pd.ExcelFile(uploaded_file)
        sheet = st.selectbox("Select Sheet", xls.sheet_names)
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str).fillna("")

    st.write("Columns detected:", df.columns.tolist())

    if script_option == "Script 1: DuckDuckGo":
        name_col = st.selectbox("Name Column", df.columns)
        addr_col = st.selectbox("Address Column", df.columns)
        url_boost = st.checkbox("Prioritize URLs containing name", value=True)
        if st.button("Run Script 1"):
            output_rows = []
            progress_text = st.empty()
            for idx, row in df.iterrows():
                name = str(row[name_col])
                address = str(row[addr_col])
                bldg = get_building_number(address)
                urls = search_duckduckgo_urls_data(f"{name} {address}")
                official_candidates = [u for u in urls if get_domain(u["url"]) not in unofficial_domains]
                unofficial_candidates = [u for u in urls if get_domain(u["url"]) in unofficial_domains]
                found_official = prioritize_links(official_candidates, name, address, bldg)
                found_unofficial = prioritize_links(unofficial_candidates, name, address, bldg)
                found_contact = extract_phone(found_official) or extract_phone(found_unofficial)
                row["official link"] = found_official
                row["unofficial link"] = found_unofficial
                row["contact number"] = found_contact
                output_rows.append(row)
                progress_text.text(f"Processing row {idx+1}/{len(df)}: {name}")
            st.success("✅ Script completed!")
            result_df = pd.DataFrame(output_rows)
            st.download_button("Download Result", result_df.to_excel(index=False), file_name="output.xlsx")

    elif script_option == "Script 2: Fuzzy Matching":
        col_name1 = st.selectbox("Full Name 1", df.columns)
        col_name2 = st.selectbox("Full Name 2", df.columns)
        col_addr1 = st.selectbox("Address 1", df.columns)
        col_addr2 = st.selectbox("Address 2", df.columns)
        if st.button("Run Script 2"):
            def preprocess(text):
                text = str(text).lower()
                text = re.sub(r'[^a-z0-9\s]', '', text)
                return re.sub(r'\s+', ' ', text).strip()
            df_proc = df.copy()
            df_proc[col_name1] = df_proc[col_name1].apply(preprocess)
            df_proc[col_name2] = df_proc[col_name2].apply(preprocess)
            df_proc[col_addr1] = df_proc[col_addr1].apply(preprocess)
            df_proc[col_addr2] = df_proc[col_addr2].apply(preprocess)

            def fuzzy_match(a,b): return fuzz.token_set_ratio(a,b)
            def building_match(a,b):
                bld1 = re.search(r"\d+", a); bld2 = re.search(r"\d+", b)
                return "Yes" if bld1 and bld2 and bld1.group()==bld2.group() and fuzzy_match(a,b)>70 else "No"

            df_proc["Name Match Percentage"] = df_proc.apply(lambda r: fuzzy_match(r[col_name1], r[col_name2]), axis=1)
            df_proc["Name Match"] = df_proc["Name Match Percentage"].apply(lambda x: "Yes" if x>=90 else "No")
            df_proc["Address Match"] = df_proc.apply(lambda r: building_match(r[col_addr1], r[col_addr2]), axis=1)
            df_proc["Final Outcome"] = df_proc.apply(lambda r: "Yes" if r["Name Match"]=="Yes" and r["Address Match"]=="Yes" else "No", axis=1)
            st.success("✅ Script completed!")
            st.download_button("Download Result", df_proc.to_excel(index=False), file_name="fuzzy_output.xlsx")
