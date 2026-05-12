import streamlit as st
import pandas as pd
import math
from pdf2image import convert_from_bytes
import pytesseract
from google import genai
import json
import re
import io
import requests
import time
import warnings
from datetime import datetime
from urllib.parse import quote
from urllib.request import Request, urlopen
from streamlit_gsheets import GSheetsConnection
from thefuzz import process, fuzz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Import the Brain
from knowledge_base import GLOBAL_RULES_TEXT, SUPPLIER_RULEBOOK

# --- SUPPRESS GOOGLE WARNING ---
warnings.filterwarnings("ignore", category=FutureWarning, module="google.generativeai")

st.set_page_config(layout="wide", page_title="Brewery Invoice Parser")

# ==========================================
# CUSTOM STYLING
# ==========================================
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 15rem;
            padding-left: 1rem;
            padding-right: 1rem;
            max_width: 98%;
        }
        html, body,[class*="css"]  {
            font-size: 14px;
        }
        .score-green { color: #2e7d32; font-weight: 600; }
        .score-amber { color: #e65100; font-weight: 600; }
        .score-red   { color: #c62828; font-weight: 600; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 0. DEFAULT STATE — single source of truth
# ==========================================
DEFAULT_STATE = {
    'header_data': None,
    'line_items': None,
    'matrix_data': None,
    'upload_data': None,
    'shopify_logs': [],
    'untappd_logs': [],
    'cin7_logs': [],
    'shopify_check_results': None,
    'selected_drive_id': None,
    'selected_drive_name': None,
    'upload_generated': False,
    'po_success': False,
    'po_url': None,
    'price_check_data': None,
    'cin7_complete': False,
    'cin7_log_text': "",
    'shopify_log_text': "",
    'cin7_links': [],
    'shopify_links': [],
    'polykeg_selections': {},
    'lab_custom_rule': "",
}

# ==========================================
# 0B. AUTHENTICATION & HEADER
# ==========================================
def check_password():
    if "APP_PASSWORD" not in st.secrets: return True
    if "password_correct" not in st.session_state: st.session_state.password_correct = False
    if st.session_state.password_correct: return True
    st.title("🔒 Login Required")
    pwd_input = st.text_input("Enter Password", type="password")
    if st.button("Log In"):
        if pwd_input == st.secrets["APP_PASSWORD"]:
            st.session_state.password_correct = True
            st.rerun()
        else: st.error("Incorrect Password")
    return False

if not check_password(): st.stop()

# --- RESET LOGIC & HEADER ---
col_head_1, col_head_2 = st.columns([4, 1])

with col_head_1:
    st.title("Brewery Invoice Parser ⚡")

with col_head_2:
    st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
    if st.button("🔄 Reset / New Invoice"):
        # Use DEFAULT_STATE so no keys are ever missed
        for k, v in DEFAULT_STATE.items():
            st.session_state[k] = v
        st.session_state.line_items_key += 1
        st.session_state.matrix_key += 1
        st.rerun()

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================

# --- 1A. PRICING & GENERAL LOGIC ---
def clean_abv(abv_str):
    """
    Canonical ABV cleaner — single call point.
    - "4.0%" -> "4"
    - "4.5 %" -> "4.5"
    - "approx 4.52" -> "4.5"
    """
    if not abv_str: return ""
    s = str(abv_str)
    s_clean = re.sub(r"[^\d.]", "", s)
    try:
        if not s_clean: return ""
        val = round(float(s_clean), 1)
        return str(int(val)) if val.is_integer() else str(val)
    except:
        return ""

def apply_clean_abv_to_df(df, col='ABV'):
    """Apply clean_abv once at the canonical point — after AI parse or Untappd merge."""
    if col in df.columns:
        df[col] = df[col].fillna("").apply(clean_abv)
    return df

def ceil2(x):
    return math.ceil(x * 100) / 100

def calculate_sell_price(cost_price, product_type, fmt):
    try:
        cost = float(cost_price)
    except:
        return 0.00
    if cost == 0: return 0.00
    fmt_lower = str(fmt).lower()
    draft_triggers = ['keykeg', 'steel', 'poly', 'uni', 'cask', 'keg', 'firkin', 'pin']
    is_draft = any(t in fmt_lower for t in draft_triggers)
    if product_type == "Core Product":
        if is_draft and cost < 64: return ceil2(cost + 17)
        elif is_draft and cost > 151: return ceil2(cost + 40)
        elif cost > 142.50: return ceil2(cost + 37.75)
        else: return ceil2(cost * 1.265)
    else:
        if is_draft and cost < 70.25: return ceil2(cost + 20)
        elif is_draft and cost > 140.5: return ceil2(cost + 40)
        elif cost > 130: return ceil2(cost + 37.00)
        else: return ceil2(cost * 1.285)

# --- 1B. GOOGLE DRIVE ---
def get_drive_service():
    if "connections" in st.secrets and "gsheets" in st.secrets["connections"]:
        creds_dict = st.secrets["connections"]["gsheets"]
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    return None

def list_files_in_folder(folder_id):
    service = get_drive_service()
    if not service: return []
    try:
        query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
        results = service.files().list(q=query, pageSize=100, fields="files(id, name)").execute()
        files = results.get('files', [])
        files.sort(key=lambda x: x['name'].lower())
        return files
    except Exception as e:
        st.error(f"Drive List Error: {e}")
        return []

def download_file_from_drive(file_id):
    service = get_drive_service()
    if not service: return None
    try:
        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        file_stream.seek(0)
        return file_stream
    except Exception as e:
        st.error(f"Download Error: {e}")
        return None

# --- 1C. UNTAPPD LOGIC ---
def search_untappd_item(supplier, product, manual_id=None):
    if "untappd" not in st.secrets: return None
    creds = st.secrets["untappd"]
    base_url = creds.get("base_url", "https://business.untappd.com/api/v1")
    token = creds.get("api_token")
    headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

    clean_manual_id = None
    if manual_id:
        raw_id = str(manual_id).strip()
        match = re.search(r'(\d+)$', raw_id)
        if match: clean_manual_id = int(match.group(1))
        elif raw_id.isdigit(): clean_manual_id = int(raw_id)

    raw_supp = str(supplier).replace("&", " and ")
    raw_prod = str(product).replace("&", " and ")
    clean_supp = re.sub(r'(?i)\b(ltd|limited|llp|plc|brewing|brewery|co\.?)\b', '', raw_supp).strip()
    clean_prod = raw_prod.strip()
    query_str = " ".join(f"{clean_supp} {clean_prod}".split())
    safe_q = quote(query_str)

    def parse_item(best, q_used):
        return {
            "untappd_id": best.get("untappd_id"),
            "name": best.get("name"),
            "brewery": best.get("brewery"),
            "abv": best.get("abv"),
            "ibu": best.get("ibu", 0),
            "style": best.get("style"),
            "description": best.get("description"),
            "label_image_thumb": best.get("label_image_thumb"),
            "brewery_location": best.get("brewery_location"),
            "brewery_country": best.get("country", "") or best.get("brewery_country", ""),
            "query_used": q_used
        }

    # 1. Try manual ID directly
    if clean_manual_id:
        try:
            url_id = f"{base_url}/items/search?q={clean_manual_id}"
            response = requests.get(url_id, headers=headers)
            if response.status_code == 200:
                items = response.json().get('items', [])
                for item in items:
                    if item.get("untappd_id") == clean_manual_id:
                        return parse_item(item, str(clean_manual_id))
        except: pass

    # 2. String search with name validation — never blindly accept items[0]
    url = f"{base_url}/items/search?q={safe_q}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items:
                if clean_manual_id:
                    for item in items:
                        if item.get("untappd_id") == clean_manual_id:
                            return parse_item(item, query_str)
                    return {
                        "untappd_id": clean_manual_id,
                        "name": clean_prod,
                        "brewery": clean_supp,
                        "query_used": query_str
                    }
                else:
                    # Validate top result — must score ≥ 85 against our query
                    best = items[0]
                    candidate_str = f"{best.get('brewery', '')} {best.get('name', '')}"
                    confidence = fuzz.token_sort_ratio(query_str, candidate_str)
                    result = parse_item(best, query_str)
                    result['confidence'] = confidence
                    if confidence >= 85:
                        result['ut_confidence_flag'] = "✅ High"
                    else:
                        result['ut_confidence_flag'] = "⚠️ Low Confidence"
                    return result
    except: pass

    if clean_manual_id:
        return {"untappd_id": clean_manual_id, "query_used": query_str, "name": clean_prod, "brewery": clean_supp}

    return {"query_used": query_str}

def batch_untappd_lookup(matrix_df, status_box=None):
    if matrix_df.empty: return matrix_df, ["Matrix Empty"]

    cols = ['Untappd_Status', 'UT_Confidence', 'Untappd_ID', 'Untappd_Brewery', 'Untappd_Product',
            'Untappd_ABV', 'Untappd_IBU', 'Untappd_Style', 'Untappd_Desc',
            'Label_Thumb', 'Brewery_Loc', 'Untappd_Country', 'Match_Check', 'Retry', 'Manual_UT_ID', 'Ignore_UT']
    for c in cols:
        if c not in matrix_df.columns: matrix_df[c] = ""

    updated_rows = []
    logs = []

    def log_msg(msg):
        logs.append(msg)
        if status_box:
            status_box.code("\n".join(logs), language="text")

    prog_bar = st.progress(0)

    for idx, row in matrix_df.iterrows():
        prog_bar.progress((idx + 1) / len(matrix_df))

        current_status = str(row.get('Untappd_Status', ''))
        retry_flag = row.get('Retry', False)
        manual_id = str(row.get('Manual_UT_ID', '')).strip()
        ignore_flag = row.get('Ignore_UT', False)

        if ignore_flag:
            log_msg(f"⏭️ Ignored: {row['Product_Name']} (Moved to Manual Entry)")
            row['Untappd_Status'] = "⚠️ Manual Entry"
            row['UT_Confidence'] = "Skipped"
            row['Match_Check'] = "Skipped Untappd Search"
            row['Untappd_ID'] = ""
            row['Untappd_Brewery'] = row.get('Supplier_Name', '')
            row['Untappd_Product'] = row.get('Product_Name', '')
            row['Untappd_ABV'] = clean_abv(row.get('ABV', ''))
            row['Untappd_Style'] = ""
            row['Untappd_Desc'] = ""
            row['Label_Thumb'] = ""
            row['Retry'] = False
            row['Manual_UT_ID'] = ""
            row['Ignore_UT'] = False

        elif current_status != "✅ Found" or retry_flag or manual_id:
            res = search_untappd_item(row['Supplier_Name'], row['Product_Name'], manual_id)

            if res and "untappd_id" in res:
                confidence = res.get('confidence', 100)
                confidence_flag = res.get('ut_confidence_flag', '✅ High')
                log_msg(f"✅ Found: {res.get('name', 'Manual Item')} ({res['untappd_id']}) [{confidence_flag}]")

                row['Untappd_Status'] = "✅ Found"
                row['UT_Confidence'] = confidence_flag
                row['Untappd_ID'] = res['untappd_id']
                row['Untappd_Brewery'] = res.get('brewery') or row.get('Supplier_Name', '')
                row['Untappd_Product'] = res.get('name') or row.get('Product_Name', '')

                # ABV — canonical clean point after Untappd merge
                fetched_abv = res.get('abv')
                row['Untappd_ABV'] = clean_abv(fetched_abv) if fetched_abv else clean_abv(row.get('ABV', ''))

                row['Untappd_IBU'] = res.get('ibu', 0)
                row['Untappd_Style'] = res.get('style', '')
                row['Untappd_Desc'] = res.get('description', '')
                row['Label_Thumb'] = res.get('label_image_thumb', '')
                row['Brewery_Loc'] = res.get('brewery_location', '')
                row['Untappd_Country'] = res.get('brewery_country', '')

                name_match_pct = fuzz.token_sort_ratio(
                    str(row.get('Product_Name', '')),
                    str(row.get('Untappd_Product', ''))
                )
                row['Match_Check'] = (
                    f"{row['Untappd_Brewery']} / {row['Untappd_Product']} / "
                    f"{clean_abv(row['Untappd_ABV'])}% | Name match: {name_match_pct}%"
                )

            else:
                used_q = res.get('query_used', 'Unknown') if res else 'Error'
                log_msg(f"❌ No match: {row['Product_Name']} | Query:[{used_q}]")
                row['Untappd_Status'] = "❌ Not Found"
                row['UT_Confidence'] = "N/A"
                row['Match_Check'] = "No Match Found"
                row['Untappd_ID'] = ""
                row['Untappd_Brewery'] = row.get('Supplier_Name', '')
                row['Untappd_Product'] = row.get('Product_Name', '')
                row['Untappd_ABV'] = clean_abv(row.get('ABV', ''))
                row['Untappd_Style'] = ""
                row['Untappd_Desc'] = ""
                row['Label_Thumb'] = ""

            row['Retry'] = False
            row['Manual_UT_ID'] = ""

        updated_rows.append(row)

    return pd.DataFrame(updated_rows), logs

# --- 1D. SHOPIFY & CIN7 ---
def get_cin7_headers():
    if "cin7" not in st.secrets: return None
    creds = st.secrets["cin7"]
    return {
        "api-auth-accountid": creds.get("account_id"),
        "api-auth-applicationkey": creds.get("api_key"),
        "Content-Type": "application/json"
    }

def get_cin7_base_url():
    if "cin7" not in st.secrets: return None
    return st.secrets["cin7"].get("base_url", "https://inventory.dearsystems.com/ExternalApi/v2")

def make_cin7_request(method, url, headers=None, status_placeholder=None, **kwargs):
    """
    Capped backoff (max 8s). Surfaces retry progress if status_placeholder provided.
    """
    if not headers: headers = get_cin7_headers()
    max_retries = 6
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            if response.status_code == 429:
                wait = min(backoff, 8.0)   # cap at 8 seconds
                if status_placeholder:
                    status_placeholder.warning(f"⏳ Rate limited — retrying in {wait:.0f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                backoff = min(backoff * 2, 8.0)
                continue
            return response
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(min(backoff, 8.0))
            backoff = min(backoff * 2, 8.0)
    return response

# --- PRICE CHECKING & UPDATING HELPERS ---
def fetch_cin7_product_details_by_sku(sku):
    headers = get_cin7_headers()
    if not headers: return None, 0.0, "", "Rotational Product", ""
    safe_sku = quote(sku)
    url = f"{get_cin7_base_url()}/product?Sku={safe_sku}"
    try:
        r = make_cin7_request("GET", url, headers=headers)
        if r.status_code == 200:
            prods = r.json().get("Products", [])
            if prods:
                p = prods[0]
                return (
                    p.get("ID"),
                    float(p.get("PriceTier1", 0.0)),
                    str(p.get("Name", "")),
                    str(p.get("AdditionalAttribute5", "Rotational Product")),
                    str(p.get("AdditionalAttribute10", "")),
                )
    except: pass
    return None, 0.0, "", "Rotational Product", ""

def update_cin7_product_details(product_id, cin7_full_name, old_product, new_product, old_variant, new_variant, old_abv, new_abv):
    headers = get_cin7_headers()
    if not headers: return False, "No headers found."
    base_url = get_cin7_base_url()
    # Fetch full product so the PUT includes all existing fields (Cin7 replaces on PUT)
    try:
        r_get = make_cin7_request("GET", f"{base_url}/product?ID={product_id}", headers=headers)
        if r_get.status_code != 200:
            return False, f"GET failed: {r_get.text[:100]}"
        prods = r_get.json().get("Products", [])
        if not prods:
            return False, "Product not found"
        payload = prods[0].copy()
        for ro in ("CreatedDate", "ModifiedDate", "BrandID"):
            payload.pop(ro, None)
    except Exception as e:
        return False, f"GET error: {e}"
    # Apply name changes via string replacement on the live name
    current_name = str(payload.get("Name", "")) or cin7_full_name
    updated_name = current_name
    if new_product and old_product and old_product != new_product:
        updated_name = updated_name.replace(old_product, new_product, 1)
    if new_variant and old_variant and old_variant != new_variant:
        updated_name = updated_name.replace(old_variant, new_variant, 1)
    if new_abv and old_abv and str(old_abv).strip() != str(new_abv).strip():
        old_abv_str = str(old_abv).replace("%", "").strip() + "%"
        new_abv_str = str(new_abv).replace("%", "").strip() + "%"
        updated_name = updated_name.replace(old_abv_str, new_abv_str, 1)
    if updated_name:
        payload["Name"] = updated_name
    if new_abv is not None and str(new_abv).strip() and str(new_abv).strip().lower() != 'nan':
        payload["AdditionalAttribute10"] = str(new_abv).replace("%", "").strip()
    try:
        r = make_cin7_request("PUT", f"{base_url}/product", headers=headers, json=payload)
        if r.status_code == 200:
            body = r.json() if r.text.strip() else {}
            errs = body.get("Errors", []) if isinstance(body, dict) else []
            if errs: return False, f"Cin7 errors: {errs}"
            return True, "OK"
        else: return False, r.text[:200]
    except Exception as e:
        return False, str(e)

def update_shopify_product_details(sku, new_product_title, new_variant_title, old_abv, new_abv, old_product=None):
    if "shopify" not in st.secrets: return False, "No secrets."
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    rest_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    gql_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    variant_gid, _ = fetch_shopify_price_by_sku(sku)
    if not variant_gid: return False, "SKU not found in Shopify"

    # Get numeric IDs and current product title in one query
    numeric_variant_id = variant_gid.split("/")[-1]
    query_prod = """query($id: ID!) { productVariant(id: $id) { product { id title } } }"""
    try:
        r = requests.post(gql_endpoint, json={"query": query_prod, "variables": {"id": variant_gid}}, headers=gql_headers)
        prod_data = r.json().get("data", {}).get("productVariant", {}).get("product", {})
        product_gid = prod_data.get("id", "")
        current_title = prod_data.get("title", "")
        numeric_product_id = product_gid.split("/")[-1]
    except Exception as e:
        return False, f"Could not resolve product GID: {e}"

    errors = []

    # Reconstruct title via string replacement rather than overwriting
    updated_title = current_title
    if new_product_title and old_product and old_product != new_product_title:
        updated_title = updated_title.replace(old_product, new_product_title, 1)
    if new_abv and old_abv and str(old_abv).strip() != str(new_abv).strip():
        old_abv_str = str(old_abv).replace("%", "").strip() + "%"
        new_abv_str = str(new_abv).replace("%", "").strip() + "%"
        updated_title = updated_title.replace(old_abv_str, new_abv_str, 1)

    if updated_title and updated_title != current_title:
        try:
            r = requests.put(
                f"https://{shop_url}/admin/api/{version}/products/{numeric_product_id}.json",
                json={"product": {"id": int(numeric_product_id), "title": updated_title}},
                headers=rest_headers
            )
            if r.status_code != 200: errors.append(f"Title: {r.text[:100]}")
        except Exception as e:
            errors.append(f"Title: {e}")

    if new_variant_title:
        try:
            r = requests.put(
                f"https://{shop_url}/admin/api/{version}/variants/{numeric_variant_id}.json",
                json={"variant": {"id": int(numeric_variant_id), "option1": new_variant_title}},
                headers=rest_headers
            )
            if r.status_code != 200: errors.append(f"Variant: {r.text[:100]}")
        except Exception as e:
            errors.append(f"Variant: {e}")

    # Update ABV metafield only if it already exists on this product
    if new_abv is not None and str(new_abv).strip() and str(new_abv).strip().lower() != 'nan' and product_gid:
        abv_clean = str(new_abv).replace("%", "").strip()
        check_query = """
        query($id: ID!) {
          product(id: $id) {
            metafield(namespace: "custom", key: "abv") { id }
          }
        }
        """
        try:
            r = requests.post(gql_endpoint, json={"query": check_query, "variables": {"id": product_gid}}, headers=gql_headers)
            existing = r.json().get("data", {}).get("product", {}).get("metafield")
            if existing:
                mutation = """
                mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
                  metafieldsSet(metafields: $metafields) {
                    metafields { key namespace value }
                    userErrors { field message code }
                  }
                }
                """
                variables = {"metafields": [{
                    "ownerId": product_gid,
                    "namespace": "custom",
                    "key": "abv",
                    "value": abv_clean,
                    "type": "number_decimal"
                }]}
                r2 = requests.post(gql_endpoint, json={"query": mutation, "variables": variables}, headers=gql_headers)
                gql_errors = r2.json().get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
                if gql_errors: errors.append(f"ABV metafield: {gql_errors}")
        except Exception as e:
            errors.append(f"ABV metafield: {e}")

    if errors: return False, " | ".join(errors)
    return True, "OK"

def update_cin7_price(product_id, new_price):
    headers = get_cin7_headers()
    if not headers: return False, "No headers found."
    url = f"{get_cin7_base_url()}/product"
    payload = {
        "ID": product_id,
        "PriceTier1": new_price,
        "PriceTiers": {"Tier 1": new_price}
    }
    try:
        r = make_cin7_request("PUT", url, headers=headers, json=payload)
        if r.status_code == 200: return True, "OK"
        else: return False, r.text
    except Exception as e:
        return False, str(e)

def fetch_shopify_price_by_sku(sku):
    if "shopify" not in st.secrets: return None, 0.0
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    query = """
    query($query: String!) {
      productVariants(first: 1, query: $query) {
        edges { node { id price } }
      }
    }
    """
    try:
        r = requests.post(endpoint, json={"query": query, "variables": {"query": f"sku:'{sku}'"}}, headers=headers)
        if r.status_code == 200:
            edges = r.json().get("data", {}).get("productVariants", {}).get("edges", [])
            if edges:
                node = edges[0]["node"]
                return node["id"], float(node["price"])
    except: pass
    return None, 0.0

def update_shopify_price(variant_gid, new_price):
    if "shopify" not in st.secrets: return False, "No secrets found."
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    query_prod = """
    query getProduct($id: ID!) {
      productVariant(id: $id) { product { id } }
    }
    """
    try:
        r_prod = requests.post(endpoint, json={"query": query_prod, "variables": {"id": variant_gid}}, headers=headers)
        if r_prod.status_code == 200:
            product_gid = r_prod.json().get("data", {}).get("productVariant", {}).get("product", {}).get("id")
            if not product_gid: return False, "Could not resolve parent Product ID."
        else: return False, f"Failed to fetch parent product: {r_prod.text}"
    except Exception as e:
        return False, f"Exception fetching product ID: {str(e)}"

    mutation = """
    mutation UpdateVariantPrice($productId: ID!, $variants:[ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants { id price }
        userErrors { field message }
      }
    }
    """
    variables = {
        "productId": product_gid,
        "variants": [{"id": variant_gid, "price": str(new_price)}]
    }
    try:
        r = requests.post(endpoint, json={"query": mutation, "variables": variables}, headers=headers)
        if r.status_code == 200:
            errors = r.json().get("data", {}).get("productVariantsBulkUpdate", {}).get("userErrors", [])
            if not errors: return True, "OK"
            else: return False, str(errors)
        else: return False, r.text
    except Exception as e:
        return False, str(e)

@st.cache_data(ttl=3600)
def fetch_cin7_brands():
    if "cin7" not in st.secrets: return []
    creds = st.secrets["cin7"]
    headers = {
        'Content-Type': 'application/json',
        'api-auth-accountid': creds.get("account_id"),
        'api-auth-applicationkey': creds.get("api_key")
    }
    base_url = creds.get("base_url", "https://inventory.dearsystems.com/ExternalApi/v2")
    all_brands = []
    page = 1
    try:
        while True:
            url = f"{base_url}/ref/brand?Page={page}&Limit=100"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                brand_list = data.get("BrandList", [])
                if not brand_list: break
                for b in brand_list:
                    if b.get("Name"): all_brands.append(str(b["Name"]))
                if len(brand_list) < 100: break
                page += 1
            else: break
    except Exception: pass
    return sorted(list(set(all_brands)), key=str.lower)

@st.cache_data(ttl=3600)
def fetch_all_cin7_suppliers_cached():
    if "cin7" not in st.secrets: return []
    creds = st.secrets["cin7"]
    headers = {
        'Content-Type': 'application/json',
        'api-auth-accountid': creds.get("account_id"),
        'api-auth-applicationkey': creds.get("api_key")
    }
    base_url = creds.get("base_url", "https://inventory.dearsystems.com/ExternalApi/v2")
    all_suppliers = []
    page = 1
    try:
        while True:
            url = f"{base_url}/supplier?Page={page}&Limit=100"
            req = Request(url, headers=headers)
            with urlopen(req) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read())
                    key = "SupplierList" if "SupplierList" in data else "Suppliers"
                    if key in data and data[key]:
                        for s in data[key]:
                            all_suppliers.append({"Name": s["Name"], "ID": s["ID"]})
                        if len(data[key]) < 100: break
                        page += 1
                    else: break
                else: break
    except: pass
    return sorted(all_suppliers, key=lambda x: x['Name'].lower())

def get_cin7_product_id(sku):
    headers = get_cin7_headers()
    if not headers: return None
    url = f"{get_cin7_base_url()}/product"
    params = {"Sku": sku}
    try:
        response = make_cin7_request("GET", url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if "Products" in data and len(data["Products"]) > 0:
                return data["Products"][0]["ID"]
    except: pass
    return None

def get_cin7_supplier(name):
    headers = get_cin7_headers()
    if not headers: return None
    safe_name = quote(name)
    url = f"{get_cin7_base_url()}/supplier?Name={safe_name}"
    try:
        response = make_cin7_request("GET", url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if "Suppliers" in data and len(data["Suppliers"]) > 0:
                return data["Suppliers"][0]
    except: pass
    if "&" in name: return get_cin7_supplier(name.replace("&", "and"))
    return None

def swap_polykeg_sku_end(sku, new_end):
    for old_end in ["KKT", "USDT", "ST"]:  # longest/most-specific first
        if sku.endswith(old_end):
            return sku[:-len(old_end)] + new_end
    return sku + new_end

def prepare_final_po_lines(line_items_df):
    if line_items_df is None or line_items_df.empty:
        return pd.DataFrame()
    po_rows = []
    for _, row in line_items_df.iterrows():
        if row.get('Shopify_Status') != "✅ Match": continue
        prod_name = row['Product_Name']
        matched_sku = row.get('Matched_Variant', '')
        raw_qty = float(row.get('Quantity', 0))
        raw_price = float(row.get('Item_Price', 0))
        # Invoice Line_Total is the authoritative figure — use it to anchor the PO total
        line_total = float(row.get('Line_Total') or (raw_qty * raw_price))
        if row.get('Use_Split', False):
            final_qty = raw_qty * 2
            final_price = raw_price / 2
            notes = "⚠️ Split Case (Half Size)"
        else:
            final_qty = raw_qty
            final_price = raw_price
            notes = ""
        l_id = row.get('Cin7_London_ID', '')
        g_id = row.get('Cin7_Glou_ID', '')
        po_rows.append({
            "Product": prod_name, "Variant_Match": matched_sku,
            "Format": str(row.get('Format', '')),
            "London_SKU": str(row.get('London_SKU', '')),
            "Gloucester_SKU": str(row.get('Gloucester_SKU', '')),
            "PO_Qty": final_qty, "PO_Cost": final_price,
            "Invoice_Line_Total": line_total,
            "Total": final_qty * final_price, "Notes": notes,
            "Cin7_London_ID": l_id, "Cin7_Glou_ID": g_id
        })
    return pd.DataFrame(po_rows)

@st.cache_data(ttl=3600)
def fetch_fallback_images():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="MasterData", usecols=[0, 4])
        if not df.empty:
            df = df.dropna()
            return dict(zip(df.iloc[:, 0].astype(str).str.lower().str.strip(), df.iloc[:, 1].astype(str).str.strip()))
    except Exception: pass
    return {}

def fetch_shopify_products_by_vendor(vendor):
    if "shopify" not in st.secrets: return []
    if not vendor or not isinstance(vendor, str): return []
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    query = """query ($query: String!, $cursor: String) { products(first: 50, query: $query, after: $cursor) { pageInfo { hasNextPage endCursor } edges { node { id title status featuredImage { url } format_meta: metafield(namespace: "custom", key: "Format") { value } abv_meta: metafield(namespace: "custom", key: "ABV") { value } keg_meta: metafield(namespace: "custom", key: "Keg_Type") { value } variants(first: 20) { edges { node { id title sku inventoryQuantity } } } } } } }"""
    search_vendor = vendor.replace("'", "\\'")
    variables = {"query": f"vendor:'{search_vendor}'"}
    all_products = []
    cursor = None
    has_next = True
    while has_next:
        vars_curr = variables.copy()
        if cursor: vars_curr['cursor'] = cursor
        try:
            response = requests.post(endpoint, json={"query": query, "variables": vars_curr}, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if "data" in data and "products" in data["data"]:
                    p_data = data["data"]["products"]
                    all_products.extend(p_data["edges"])
                    has_next = p_data["pageInfo"]["hasNextPage"]
                    cursor = p_data["pageInfo"]["endCursor"]
                else: has_next = False
            else: has_next = False
        except: has_next = False
    return all_products

def check_shopify_title(title):
    if "shopify" not in st.secrets: return None, None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    url = f"https://{shop_url}/admin/api/{version}/products.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    try:
        response = requests.get(url, headers=headers, params={"title": title})
        if response.status_code == 200:
            products = response.json().get("products", [])
            for p in products:
                if p["title"] == title:
                    v_id = p["variants"][0]["id"] if p["variants"] else None
                    return p["id"], v_id
    except Exception: pass
    return None, None

def fetch_publication_ids():
    if "shopify" not in st.secrets: return None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    pub_map = {'london': None, 'gloucester': None}
    query_catalogs = """{ catalogs(first: 25) { nodes { id title publication { id } } } }"""
    try:
        r = requests.post(endpoint, json={"query": query_catalogs}, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if "data" in data and "catalogs" in data["data"]:
                for node in data["data"]["catalogs"]["nodes"]:
                    title = node['title'].lower()
                    pub_id = node['publication']['id']
                    if "london" in title: pub_map['london'] = pub_id
                    if "gloucester" in title: pub_map['gloucester'] = pub_id
    except: pass
    if not pub_map['london'] or not pub_map['gloucester']:
        query_pubs = """{ publications(first: 25) { edges { node { id name } } } }"""
        try:
            r = requests.post(endpoint, json={"query": query_pubs}, headers=headers)
            if r.status_code == 200:
                data = r.json()
                if "data" in data and "publications" in data["data"]:
                    for edge in data["data"]["publications"]["edges"]:
                        node = edge['node']
                        name = node['name'].lower()
                        pid = node['id']
                        if "london" in name and not pub_map['london']: pub_map['london'] = pid
                        if "gloucester" in name and not pub_map['gloucester']: pub_map['gloucester'] = pid
        except: pass
    return pub_map

def publish_product_to_app(product_id_numeric, publication_id_gql):
    if not product_id_numeric or not publication_id_gql: return False
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    product_gid = f"gid://shopify/Product/{product_id_numeric}"
    mutation = """mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) { publishablePublish(id: $id, input: $input) { userErrors { field message } } }"""
    variables = {"id": product_gid, "input": [{"publicationId": publication_id_gql}]}
    try:
        r = requests.post(endpoint, json={"query": mutation, "variables": variables}, headers=headers)
        if r.status_code == 200:
            errors = r.json().get("data", {}).get("publishablePublish", {}).get("userErrors", [])
            if not errors: return True
    except: pass
    return False

def get_abv_category(abv_str):
    try: val = float(abv_str)
    except: return ""
    if val <= 3.0: return "0% - 3%"
    elif val <= 4.5: return "3% - 4.5%"
    elif val <= 6.5: return "4.6% - 6.5%"
    elif val < 10.0: return "6.6% - 9.9%"
    else: return "Over 10%"

def split_untappd_style(full_style):
    if not full_style: return "", ""
    parts = str(full_style).split("-", 1)
    primary = parts[0].strip()
    secondary = parts[1].strip() if len(parts) > 1 else ""
    return primary, secondary

def get_filter_group(row):
    valid_options = ["6 Packs", "12 Packs", "24 Packs", "KeyKeg Coupler", "Sankey Coupler", "US Sankey D-Type Coupler"]
    connector = str(row.get('Keg_Connector', '')).strip()
    if connector in valid_options: return connector
    try: pack = int(float(row.get('pack_size', 0)))
    except: pack = 0
    pack_str = f"{pack} Packs"
    if pack_str in valid_options: return pack_str
    return None

def create_shopify_variant_payload(row, location_prefix):
    is_london = location_prefix == "L"
    prefix = "L-" if is_london else "G-"
    sku = f"{prefix}{row['Variant_SKU']}"
    price = str(row['Sales_Price'])
    title = row['Variant_Name']
    weight = float(row.get('Weight', 0))
    filter_val = get_filter_group(row)
    metafields = [{"key": "split_case", "value": "false", "type": "boolean", "namespace": "custom"}]
    if filter_val:
        metafields.append({"key": "filter_group", "value": filter_val, "type": "single_line_text_field", "namespace": "custom"})
    return {
        "sku": sku, "price": price, "title": title, "weight": weight, "weight_unit": "kg",
        "option1": title, "inventory_management": "shopify", "fulfillment_service": "manual",
        "inventory_policy": "deny", "metafields": metafields
    }

def create_shopify_product_payload(row, location_prefix, variants_list):
    is_london = location_prefix == "L"
    prefix = "L-" if is_london else "G-"
    loc_name = "London" if is_london else "Gloucester"
    family_base = row['Family_Name']
    full_title = f"{prefix}{family_base}"
    vendor = row['untappd_brewery']
    body_html = row.get('description', '')
    prod_type = loc_name
    abv_val = clean_abv(row.get('untappd_abv', ''))
    abv_cat = get_abv_category(abv_val)
    style_prim, style_sec = split_untappd_style(row.get('untappd_style', ''))
    try: ibu_val = float(row.get('untappd_ibu', 0))
    except: ibu_val = 0.0
    untappd_id = row.get('Untappd_ID', '') or row.get('untappd_id', '')
    is_match = bool(untappd_id)
    ignore_val = "false" if is_match else "true"
    filter_val = get_filter_group(row)
    tags_list = [loc_name, "Wholesale", vendor, row.get('Type', 'Beer'), row.get('format', ''),
                 style_prim, style_sec, abv_cat, row.get('Attribute_5', 'Rotational Product'), filter_val]
    tags_str = ",".join([str(t) for t in tags_list if t])
    images = []
    if row.get('Label_Thumb'):
        img_url = row['Label_Thumb']
        if "Icon.png" in img_url: img_url = img_url.replace("Icon.png", "HD.png") + "?size=hd"
        images.append({"src": img_url})
    metafields = []
    def add_meta(key, value, type_def, namespace="custom"):
        if value is not None and str(value).strip() != "":
            metafields.append({"key": key, "value": str(value), "type": type_def, "namespace": namespace})
    add_meta("abv", abv_val, "number_decimal")
    add_meta("depot", loc_name, "single_line_text_field")
    add_meta("format", row.get('format', ''), "single_line_text_field")
    add_meta("primary_style", style_prim, "single_line_text_field")
    add_meta("secondary_style", style_sec, "single_line_text_field")
    add_meta("collaboration", row.get('collaborator', ''), "single_line_text_field")
    add_meta("keg_type", row.get('format', ''), "single_line_text_field")
    add_meta("ut_description", body_html, "multi_line_text_field")
    add_meta("brewery_location", row.get('Brewery_Loc', ''), "single_line_text_field")
    add_meta("abv_category", abv_cat, "single_line_text_field")
    add_meta("ut_brewery_country", row.get('untappd_country', ''), "single_line_text_field")
    add_meta("ut_ignore", ignore_val, "boolean")
    if is_match:
        add_meta("ut_ibu", ibu_val, "number_decimal")
        add_meta("ut_id", untappd_id, "number_integer")
        add_meta("ut_link", f"https://untappd.com/beer/{untappd_id}", "single_line_text_field")
        if row.get('Label_Thumb'):
            add_meta("ut_img_small", row['Label_Thumb'], "single_line_text_field")
            hd_url = row['Label_Thumb'].replace("Icon.png", "HD.png") + "?size=hd" if "Icon.png" in row['Label_Thumb'] else row['Label_Thumb']
            add_meta("ut_img_hd", hd_url, "single_line_text_field")
    return {
        "product": {
            "title": full_title, "body_html": body_html, "vendor": vendor, "product_type": prod_type,
            "status": "draft", "tags": tags_str, "variants": variants_list, "images": images, "metafields": metafields
        }
    }

def fetch_shopify_location_ids():
    if "shopify" not in st.secrets: return None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    url = f"https://{shop_url}/admin/api/{version}/locations.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    loc_map = {'london': creds.get('location_id_london'), 'gloucester': creds.get('location_id_gloucester'), 'all_ids': []}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            locations = r.json().get('locations', [])
            for loc in locations:
                lid = loc['id']
                lname = loc['name'].lower()
                loc_map['all_ids'].append(lid)
                if not loc_map['london'] and "london" in lname: loc_map['london'] = lid
                if not loc_map['gloucester'] and "gloucester" in lname: loc_map['gloucester'] = lid
        else: st.error(f"⚠️ Shopify Location API Error [{r.status_code}]: {r.text}")
    except Exception as e: st.error(f"⚠️ Location Fetch Exception: {e}")
    return loc_map

def set_variant_location(inventory_item_id, target_location_id, all_location_ids):
    if not inventory_item_id or not target_location_id: return False
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    base_url = f"https://{shop_url}/admin/api/{version}/inventory_levels"
    payload = {"location_id": target_location_id, "inventory_item_id": inventory_item_id, "available": 0}
    try: requests.post(f"{base_url}/set.json", json=payload, headers=headers)
    except: pass
    for loc_id in all_location_ids:
        if loc_id != target_location_id:
            try: requests.delete(f"{base_url}.json", headers=headers, params={"inventory_item_id": inventory_item_id, "location_id": loc_id})
            except: pass
    return True

def check_cin7_exists(endpoint, name_or_sku, is_sku=False):
    headers = get_cin7_headers()
    if not headers: return None
    param = "Sku" if is_sku else "Name"
    safe_val = quote(name_or_sku)
    url = f"{get_cin7_base_url()}/{endpoint}?{param}={safe_val}"
    try:
        response = make_cin7_request("GET", url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            key = "Products" if endpoint == "product" else "ProductFamilies"
            items = data.get(key, [])
            for i in items:
                target_val = i["SKU"] if is_sku else i["Name"]
                if target_val.lower() == name_or_sku.lower(): return i["ID"]
    except Exception: pass
    return None

def create_cin7_family_node(family_base_sku, family_base_name, brand_name, location_prefix):
    prefix = "L-" if location_prefix == "L" else "G-"
    location_name = "London" if location_prefix == "L" else "Gloucester"
    full_sku = f"{prefix}{family_base_sku}"
    full_name = f"{prefix}{family_base_name}"
    existing_id = check_cin7_exists("productFamily", full_sku, is_sku=True)
    if existing_id: return existing_id, f"✅ Family Exists (SKU Match) [ID: {existing_id}]"
    existing_id = check_cin7_exists("productFamily", full_name, is_sku=False)
    if existing_id: return existing_id, f"✅ Family Exists (Name Match)[ID: {existing_id}]"
    tags = f"{location_name},Wholesale,{brand_name}"
    payload = {
        "Products": [], "SKU": full_sku, "Name": full_name, "Category": location_name,
        "DefaultLocation": location_name, "Brand": brand_name, "CostingMethod": "FIFO - Batch",
        "UOM": "each", "MinimumBeforeReorder": 0.0000, "ReorderQuantity": 0.0000, "PriceTier1": 0.0000,
        "Tags": tags, "COGSAccount": "5101", "RevenueAccount": "4000", "InventoryAccount": "1001",
        "DropShipMode": "No Drop Ship", "Option1Name": "Variant", "Option1Values": ""
    }
    url = f"{get_cin7_base_url()}/productFamily"
    headers = get_cin7_headers()
    try:
        response = make_cin7_request("POST", url, headers=headers, json=payload)
        if response.status_code == 200:
            resp_data = response.json()
            new_id = resp_data.get('ID')
            if not new_id and "ProductFamilies" in resp_data and len(resp_data["ProductFamilies"]) > 0:
                new_id = resp_data["ProductFamilies"][0].get("ID")
            if new_id: return new_id, f"🆕 Created Family {full_sku} (ID: {new_id})"
            else: return None, f"⚠️ HTTP 200 but No ID. Response: {json.dumps(resp_data)}"
        else: return None, f"❌ Failed Family {full_sku} [HTTP {response.status_code}]: {response.text}"
    except Exception as e: return None, f"💥 Exception Family: {str(e)}"

def create_cin7_product_only(row_data, family_id, family_base_sku, family_base_name, location_prefix):
    prefix = "L-" if location_prefix == "L" else "G-"
    location_name = "London" if location_prefix == "L" else "Gloucester"
    var_sku_raw = row_data['Variant_SKU']
    var_name_raw = row_data['Variant_Name']
    full_var_sku = f"{prefix}{var_sku_raw}"
    full_var_name = f"{prefix}{family_base_name} / {var_name_raw}"
    headers = get_cin7_headers()
    base_url = get_cin7_base_url()
    check_url = f"{base_url}/product?Sku={quote(full_var_sku)}"
    try:
        r_check = make_cin7_request("GET", check_url, headers=headers)
        if r_check.status_code == 200:
            data = r_check.json()
            if data.get("Products"):
                return data["Products"][0]["ID"], f"🔍 Found Existing Product: {full_var_sku}"
    except Exception as e:
        return None, f"💥 Check Ex: {e}"
    brand_name = row_data['untappd_brewery']
    weight = float(row_data['Weight'])
    internal_note = f"{full_var_sku} *** {full_var_name} *** {var_name_raw} *** {family_id}"
    tags = f"{location_name},Wholesale,{brand_name}"
    fmt = row_data.get('format', '')
    parent_format_map = fetch_parent_formats()
    clean_fmt = str(fmt).lower().strip()
    if clean_fmt in parent_format_map: attr1_val = parent_format_map[clean_fmt]
    elif "keg" in clean_fmt: attr1_val = "Keg"
    else: attr1_val = fmt
    style = row_data.get('untappd_style', '')
    abv = row_data.get('untappd_abv', '')
    keg_connector = row_data.get('Keg_Connector', '')
    prod_name_only = row_data.get('untappd_product', '')
    attr_5 = row_data.get('Attribute_5', 'Rotational Product')
    prod_type = row_data.get('Type', 'Beer')
    cost_price = float(row_data.get('item_price', 0))
    sales_price = calculate_sell_price(cost_price, attr_5, fmt)
    payload_prod = {
        "SKU": full_var_sku, "Name": full_var_name, "Category": location_name, "Brand": brand_name,
        "Type": "Stock", "CostingMethod": "FIFO - Batch", "DropShipMode": "No Drop Ship",
        "DefaultLocation": location_name, "Weight": weight, "UOM": "Each", "WeightUnits": "kg",
        "PriceTier1": sales_price, "PriceTiers": {"Tier 1": sales_price}, "InternalNote": internal_note,
        "Description": row_data['description'], "AdditionalAttribute1": attr1_val, "AdditionalAttribute2": style,
        "AdditionalAttribute3": fmt, "AdditionalAttribute4": prod_type, "AdditionalAttribute5": attr_5,
        "AdditionalAttribute6": var_sku_raw, "AdditionalAttribute7": var_name_raw, "AdditionalAttribute8": keg_connector,
        "AdditionalAttribute9": prod_name_only, "AdditionalAttribute10": abv, "AttributeSet": "Products",
        "Tags": tags, "Status": "Active", "COGSAccount": "5101", "RevenueAccount": "4000",
        "InventoryAccount": "1001", "Sellable": True,
    }
    try:
        r_create = make_cin7_request("POST", f"{base_url}/product", headers=headers, json=payload_prod)
        if r_create.status_code == 200:
            resp_data = r_create.json()
            if "Products" in resp_data and resp_data["Products"]:
                return resp_data["Products"][0]["ID"], f"🆕 Created New Product: {full_var_sku}"
            elif "ID" in resp_data:
                return resp_data["ID"], f"🆕 Created New Product: {full_var_sku}"
            return None, f"⚠️ Created but no ID returned: {full_var_sku}"
        else: return None, f"❌ Create Failed {full_var_sku}: {r_create.text}"
    except Exception as e: return None, f"💥 Create Ex: {e}"

def sync_product_to_cin7(upload_df, status_box=None):
    log = []
    links = []  # {"label": ..., "url": ...} for newly created products only
    def update_log(message):
        log.append(message)
        if status_box: status_box.code("\n".join(log), language="text")
    families = upload_df.groupby('Family_SKU')
    total_families = len(families)
    update_log(f"🚀 Starting Bulk Sync for {total_families} Families...")
    headers = get_cin7_headers()
    base_url = get_cin7_base_url()
    for i, (fam_sku, group) in enumerate(families):
        first_row = group.iloc[0]
        fam_name = first_row['Family_Name']
        brand = first_row['untappd_brewery']
        update_log(f"\n🔄 Processing Family {i+1}/{total_families}: {fam_sku}")
        for loc in ["L", "G"]:
            fam_id, fam_msg = create_cin7_family_node(fam_sku, fam_name, brand, loc)
            update_log(f"   [{loc}] {fam_msg}")
            if fam_id:
                family_obj = None
                update_log(f"      📥 Fetching existing family structure...")
                try:
                    r_fam = make_cin7_request("GET", f"{base_url}/productFamily?ID={fam_id}", headers=headers)
                    if r_fam.status_code == 200:
                        fam_data = r_fam.json()
                        if "ProductFamilies" in fam_data and fam_data["ProductFamilies"]:
                            family_obj = fam_data["ProductFamilies"][0]
                        elif "ID" in fam_data:
                            family_obj = fam_data
                except Exception as e:
                    update_log(f"      💥 Family Fetch Error: {e}")
                    continue
                if not family_obj:
                    update_log("      ⚠️ Could not retrieve family structure. Skipping variants.")
                    continue
                current_products = family_obj.get("Products", []) or []
                family_needs_update = False
                for _, row in group.iterrows():
                    var_name_raw = row['Variant_Name']
                    already_in_fam = any(
                        str(p.get("Option1", "")).lower().strip() == str(var_name_raw).lower().strip()
                        for p in current_products
                    )
                    if already_in_fam:
                        update_log(f"      -> ⏭️ Skipped: '{var_name_raw}' is already linked to this Family.")
                        continue
                    prod_id, var_msg = create_cin7_product_only(row, fam_id, fam_sku, fam_name, loc)
                    update_log(f"      -> {var_msg}")
                    if prod_id:
                        current_products.append({"ID": prod_id, "Option1": var_name_raw})
                        family_needs_update = True
                        update_log(f"         ⚙️ Staged '{var_name_raw}' for bulk linking...")
                        if "🆕" in var_msg:
                            full_sku = var_msg.split(": ")[-1].strip()
                            links.append({
                                "label": f"{fam_name} / {var_name_raw} ({full_sku})",
                                "url": f"https://inventory.dearsystems.com/Product#{prod_id}",
                            })
                if family_needs_update:
                    update_log(f"      📤 Pushing bulk variant update to Family...")
                    family_obj["Products"] = current_products
                    for field in ['CreatedDate', 'LastModifiedOn']:
                        family_obj.pop(field, None)
                    try:
                        r_put = make_cin7_request("PUT", f"{base_url}/productFamily", headers=headers, json=family_obj)
                        if r_put.status_code == 200: update_log(f"      ✅ Successfully bulk-linked all variants to Family!")
                        else: update_log(f"      ❌ Bulk Link Failed: {r_put.text}")
                    except Exception as e: update_log(f"      💥 Bulk Link Ex: {e}")
                else:
                    update_log(f"      ✅ Family is fully up to date. No bulk link needed.")
            else:
                update_log(f"   🛑 HALT: Could not acquire Family ID. Skipping variants for {fam_sku} ({loc}).")
    update_log("\n✅ Sync Process Complete.")
    return log, links

def create_cin7_purchase_order(header_df, lines_df, location_choice):
    headers = get_cin7_headers()
    if not headers: return False, "Cin7 Secrets missing.", [], None
    logs = []
    supplier_id = None
    if 'Cin7_Supplier_ID' in header_df.columns and header_df.iloc[0]['Cin7_Supplier_ID']:
        supplier_id = header_df.iloc[0]['Cin7_Supplier_ID']
    else:
        supplier_name = header_df.iloc[0]['Payable_To']
        supplier_data = get_cin7_supplier(supplier_name)
        if supplier_data: supplier_id = supplier_data['ID']
    if not supplier_id: return False, "Supplier not linked.", logs, None
    order_lines = []
    id_col = 'Cin7_London_ID' if location_choice == 'London' else 'Cin7_Glou_ID'
    for _, row in lines_df.iterrows():
        prod_id = row.get(id_col)
        if pd.notna(prod_id) and str(prod_id).strip():
            qty = float(row.get('PO_Qty', 0))
            # Derive price from invoice line total so qty * price = exact invoice total
            invoice_total = float(row.get('Invoice_Line_Total') or row.get('PO_Cost', 0))
            price = round(invoice_total / qty, 10) if qty else 0
            total = invoice_total
            order_lines.append({
                "ProductID": prod_id, "Quantity": qty, "Price": price, "Total": total,
                "TaxRule": "20% (VAT on Expenses)", "Discount": 0, "Tax": 0
            })
    if not order_lines: return False, "No valid lines found to export.", logs, None
    url_create = f"{get_cin7_base_url()}/advanced-purchase"
    payload_header = {
        "SupplierID": supplier_id, "Location": location_choice,
        "Date": pd.to_datetime('today').strftime('%Y-%m-%d'),
        "TaxRule": "20% (VAT on Expenses)", "Approach": "Stock",
        "BlindReceipt": False, "PurchaseType": "Advanced", "Status": "ORDERING",
        "SupplierInvoiceNumber": str(header_df.iloc[0].get('Invoice_Number', ''))
    }
    task_id = None
    try:
        r1 = make_cin7_request("POST", url_create, headers=headers, json=payload_header)
        if r1.status_code == 200: task_id = r1.json().get('ID')
        else: return False, f"Header Error: {r1.text}", logs, None
    except Exception as e: return False, f"Header Ex: {e}", logs, None
    if task_id:
        url_lines = f"{get_cin7_base_url()}/purchase/order"
        payload_lines = {
            "TaskID": task_id, "CombineAdditionalCharges": False,
            "Memo": "Streamlit Import", "Status": "DRAFT", "Lines": order_lines, "AdditionalCharges": []
        }
        try:
            r2 = make_cin7_request("POST", url_lines, headers=headers, json=payload_lines)
            if r2.status_code == 200: return True, f"✅ PO Created!", logs, task_id
            else: return False, f"Line Error: {r2.text}", logs, None
        except Exception as e: return False, f"Lines Ex: {e}", logs, None
    return False, "Unknown Error", logs, None

def normalize_vol_string(v_str):
    if not v_str: return "0"
    v_str = str(v_str).lower().strip()
    nums = re.findall(r'\d+\.?\d*', v_str)
    if not nums: return "0"
    val = float(nums[0])
    if "ml" in v_str: val = val / 10
    return str(int(val)) if val.is_integer() else str(val)

# ==========================================
# FORMAT COMPATIBILITY MATRIX
# Replaces scattered string-matching chains in run_reconciliation_check.
# ==========================================
FORMAT_COMPAT = {
    "steel":  {"required": ["steel", "stainless", "lss"],  "excluded": ["keykeg", "key keg", "poly", "dolium", "unikeg"]},
    "key":    {"required": ["key"],                         "excluded": ["steel", "stainless", "lss", "poly", "dolium", "unikeg"]},
    "poly":   {"required": ["poly"],                        "excluded": ["steel", "stainless", "keykeg", "key keg", "dolium", "unikeg"]},
    "dolium": {"required": ["dolium"],                      "excluded": ["steel", "stainless", "keykeg", "key keg", "poly", "unikeg"]},
    "uni":    {"required": ["uni"],                         "excluded": ["steel", "stainless", "keykeg", "key keg", "poly", "dolium"]},
}

def _format_is_compatible(inv_fmt, shop_keg_meta, combined_shop_tags):
    """Single lookup instead of 20 conditional branches."""
    if "keg" not in inv_fmt:
        return True
    for key, rules in FORMAT_COMPAT.items():
        if key in inv_fmt:
            if shop_keg_meta:
                if not any(r in shop_keg_meta for r in rules["required"]):
                    return False
            if any(e in combined_shop_tags for e in rules["excluded"]):
                return False
    return True

def run_reconciliation_check(lines_df, recheck_only=False):
    if lines_df.empty: return lines_df, ["No Lines to check."]
    logs = []
    df = lines_df.copy()
    if 'Use_Split' not in df.columns: df['Use_Split'] = False
    if 'Strict_Search' not in df.columns: df['Strict_Search'] = False
    if 'Recheck' not in df.columns: df['Recheck'] = True

    if recheck_only:
        # Ensure match columns exist for rows we'll skip
        for col in ('Shopify_Status', 'Match_Score', 'Matched_Product', 'Matched_Variant',
                    'Image', 'London_SKU', 'Cin7_London_ID', 'Gloucester_SKU', 'Cin7_Glou_ID'):
            if col not in df.columns: df[col] = ""
        recheck_mask = df['Recheck'].fillna(True).astype(bool)
        suppliers = [s for s in df.loc[recheck_mask, 'Supplier_Name'].unique()
                     if isinstance(s, str) and s.strip()]
        logs.append(f"🔄 Rechecking {recheck_mask.sum()} selected row(s) across {len(suppliers)} supplier(s).")
    else:
        df['Shopify_Status'] = "Pending"
        df['Match_Score'] = ""
        df['Matched_Product'] = ""
        df['Matched_Variant'] = ""
        df['Image'] = ""
        df['London_SKU'] = ""
        df['Cin7_London_ID'] = ""
        df['Gloucester_SKU'] = ""
        df['Cin7_Glou_ID'] = ""
        suppliers = [s for s in df['Supplier_Name'].unique() if isinstance(s, str) and s.strip()]

    shopify_cache = {}
    progress_bar = st.progress(0)
    for i, supplier in enumerate(suppliers):
        progress_bar.progress((i) / max(len(suppliers), 1))
        logs.append(f"🔎 **Fetching Shopify Data:** `{supplier}`")
        shopify_cache[supplier] = fetch_shopify_products_by_vendor(supplier)
    progress_bar.progress(1.0)

    results = []
    for _, row in df.iterrows():
        # In recheck_only mode, pass through rows not marked for recheck
        if recheck_only and not bool(row.get('Recheck', True)):
            results.append(row)
            continue
        status = "❓ Vendor Not Found"
        match_score_val = ""
        london_sku, glou_sku, cin7_l_id, cin7_g_id, img_url = "", "", "", "", ""
        matched_prod_name, matched_var_name = "", ""

        supplier = str(row.get('Supplier_Name', ''))
        inv_prod_name = row['Product_Name']
        use_split = row.get('Use_Split', False)
        is_strict = row.get('Strict_Search', False)
        match_threshold = 95 if is_strict else 65

        raw_pack_str = str(row.get('Pack_Size', '1'))
        pack_nums = re.findall(r'\d+', raw_pack_str)
        original_pack = float(pack_nums[0]) if pack_nums else 1.0

        if use_split and original_pack > 1:
            target_pack = int(original_pack / 2)
            logs.append(f"   ✂️ Splitting: Invoice {int(original_pack)} -> Looking for {target_pack}")
        else:
            target_pack = int(original_pack)

        inv_vol = normalize_vol_string(row.get('Volume', ''))
        inv_fmt = str(row.get('Format', '')).lower()
        # Strip numbers from product names before fuzzy matching —
        # suppliers often use shorthand that omits ABV/batch numbers.
        # ABV is checked separately via the Shopify metafield below.
        def strip_nums(s):
            return re.sub(r'\b\d+[\d.,]*\b', '', s).strip()

        inv_prod_name_clean = strip_nums(inv_prod_name)
        inv_abv = clean_abv(str(row.get('ABV', '')))

        debug_mode = "(Strict)" if is_strict else "(Fuzzy)"
        logs.append(f"Checking: **{inv_prod_name}** {debug_mode} | Target Pack: {target_pack} | ABV: {inv_abv or 'unknown'}")

        # Manual SKU override — bypasses fuzzy matching entirely
        manual_sku = str(row.get('Manual_Shopify_SKU', '')).strip()
        if manual_sku:
            base_sku = manual_sku[2:] if manual_sku[:2] in ("L-", "G-") else manual_sku
            london_sku = f"L-{base_sku}"
            glou_sku = f"G-{base_sku}"
            status = "✅ Match (Manual)"
            match_score_val = "Manual"
            logs.append(f"   📌 Manual override: `{london_sku}`")
            if "shopify" in st.secrets:
                try:
                    creds = st.secrets["shopify"]
                    gql_ep = f"https://{creds['shop_url']}/admin/api/{creds.get('api_version','2024-04')}/graphql.json"
                    gql_h = {"X-Shopify-Access-Token": creds["access_token"], "Content-Type": "application/json"}
                    # Single query: search by SKU and return variant title + product info in one call
                    q = """
                    query($q: String!) {
                      productVariants(first: 1, query: $q) {
                        edges { node {
                          title
                          product { title featuredImage { url } }
                        }}
                      }
                    }"""
                    r = requests.post(gql_ep, json={"query": q, "variables": {"q": f"sku:{london_sku}"}}, headers=gql_h)
                    edges = r.json().get("data", {}).get("productVariants", {}).get("edges", [])
                    if edges:
                        node = edges[0]["node"]
                        full_title = node.get("product", {}).get("title", "")
                        matched_prod_name = full_title[2:] if full_title[:2] in ("L-", "G-") else full_title
                        matched_var_name = node.get("title", "")
                        img_node = node.get("product", {}).get("featuredImage")
                        if img_node: img_url = img_node.get("url", "")
                        logs.append(f"   ✅ Shopify details: `{matched_prod_name}` / `{matched_var_name}`")
                    else:
                        logs.append(f"   ⚠️ No Shopify variant found for SKU `{london_sku}` — check the SKU is correct")
                except Exception as ex:
                    logs.append(f"   ⚠️ Could not fetch Shopify details: {ex}")
        elif supplier in shopify_cache and shopify_cache[supplier]:
            candidates = shopify_cache[supplier]
            scored_candidates = []

            for edge in candidates:
                prod = edge['node']
                shop_title_full = prod['title']
                shop_prod_name_clean = shop_title_full
                if "/" in shop_title_full:
                    parts = [p.strip() for p in shop_title_full.split("/")]
                    if len(parts) >= 2: shop_prod_name_clean = parts[1]

                # Match on number-stripped names — allows invoice shorthand to match
                score = fuzz.token_sort_ratio(inv_prod_name_clean, strip_nums(shop_prod_name_clean))

                # Separate ABV check using the Shopify metafield, not the product name.
                # Only penalise if BOTH sides have a known ABV and they differ by > 0.4%.
                shop_abv = clean_abv(str((prod.get('abv_meta') or {}).get('value', '')))
                if inv_abv and shop_abv:
                    try:
                        abv_diff = abs(float(inv_abv) - float(shop_abv))
                        if abv_diff > 0.4:
                            logs.append(f"   ⛔ ABV mismatch: invoice {inv_abv}% vs Shopify {shop_abv}% for '{shop_prod_name_clean}'")
                            continue
                    except ValueError:
                        pass

                if not is_strict:
                    inv_lower = inv_prod_name_clean.lower()
                    shop_lower = strip_nums(shop_prod_name_clean).lower()
                    # Full containment — invoice name is a substring of Shopify title (e.g. "Kokomo" in "Kokomo Weekday")
                    if inv_lower in shop_lower:
                        score += 20
                    # Partial containment — all invoice words appear in the Shopify title
                    elif all(word in shop_lower for word in inv_lower.split() if len(word) > 2):
                        score += 10

                if score > match_threshold:
                    scored_candidates.append((score, prod, shop_prod_name_clean))

            scored_candidates.sort(key=lambda x: x[0], reverse=True)
            match_found = False

            for score, prod, clean_name in scored_candidates:
                if score < match_threshold: continue

                shop_keg_meta = str((prod.get('keg_meta') or {}).get('value', '')).lower()
                shop_fmt_meta = str((prod.get('format_meta') or {}).get('value', '')).lower()
                combined_shop_tags = f"{shop_keg_meta} {shop_fmt_meta} {prod['title'].lower()}"

                if not _format_is_compatible(inv_fmt, shop_keg_meta, combined_shop_tags):
                    continue

                for v_edge in prod['variants']['edges']:
                    variant = v_edge['node']
                    v_title = variant['title'].lower()
                    v_sku = str(variant.get('sku', '')).strip()
                    v_tokens = re.findall(r'\d+|[a-z]+', v_title)

                    pack_match = False
                    if target_pack == 1:
                        pack_match = not ("x" in v_tokens and any(t.isdigit() and int(t) > 1 for t in v_tokens))
                    else:
                        pack_match = str(target_pack) in v_tokens

                    vol_match = (
                        inv_vol in v_title or
                        (inv_vol == "9" and "firkin" in v_title) or
                        (inv_vol in ("4", "4.5") and "pin" in v_title) or
                        inv_vol + "l" in v_title or
                        inv_vol + " l" in v_title
                    )

                    if pack_match and vol_match:
                        score_label = f"{score}/100"
                        logs.append(f"   ✅ MATCH [{score_label}]: `{variant['title']}` | SKU: `{v_sku}`")
                        status = "✅ Match"
                        match_score_val = score_label
                        match_found = True
                        full_title = prod['title']
                        matched_prod_name = full_title[2:] if full_title.startswith("L-") or full_title.startswith("G-") else full_title
                        matched_var_name = variant['title']
                        if prod.get('featuredImage'): img_url = prod['featuredImage']['url']
                        if v_sku and len(v_sku) > 2:
                            base_sku = v_sku[2:]
                            london_sku = f"L-{base_sku}"
                            glou_sku = f"G-{base_sku}"
                        break
                if match_found: break

            if not match_found: status = "🟥 Check and Upload"

        if london_sku: cin7_l_id = get_cin7_product_id(london_sku)
        if glou_sku: cin7_g_id = get_cin7_product_id(glou_sku)

        row['Shopify_Status'] = status
        row['Match_Score'] = match_score_val
        row['Matched_Product'] = matched_prod_name
        row['Matched_Variant'] = matched_var_name
        row['Image'] = img_url
        row['London_SKU'] = london_sku
        row['Cin7_London_ID'] = cin7_l_id
        row['Gloucester_SKU'] = glou_sku
        row['Cin7_Glou_ID'] = cin7_g_id
        # Clear recheck flag only when fully resolved (matched + both Cin7 IDs populated)
        fully_resolved = status in ("✅ Match", "✅ Match (Manual)") and bool(cin7_l_id) and bool(cin7_g_id)
        row['Recheck'] = not fully_resolved
        results.append(row)

    return pd.DataFrame(results), logs

def get_master_supplier_list():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        df = conn.read(worksheet="MasterData", ttl=600)
        return df['Supplier_Master'].dropna().astype(str).tolist()
    except: return []

@st.cache_data(ttl=3600)
def fetch_supplier_codes():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="MasterData", usecols=[0, 1])
        if not df.empty:
            df = df.dropna()
            return pd.Series(df.iloc[:, 1].values, index=df.iloc[:, 0]).to_dict()
    except Exception: pass
    return {}

@st.cache_data(ttl=3600)
def fetch_format_codes():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="SKU", usecols=[0, 1])
        if not df.empty:
            df = df.dropna()
            return dict(zip(df.iloc[:, 0].astype(str).str.lower(), df.iloc[:, 1].astype(str)))
    except Exception: pass
    return {}

@st.cache_data(ttl=3600)
def fetch_parent_formats():
    """Fetches the 'Parent Format' from Column C (Index 2) of the SKU sheet."""
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1J1TJHGtqft_HEU0Q-HavYM8RwrWbDtulcxBEU7YWOwA"
        df = conn.read(spreadsheet=sheet_url, worksheet="SKU", usecols=[0, 2])
        if not df.empty:
            df = df.dropna()
            return dict(zip(df.iloc[:, 0].astype(str).str.lower().str.strip(), df.iloc[:, 1].astype(str).str.strip()))
    except Exception: pass
    return {}

@st.cache_data(ttl=3600)
def fetch_weight_map():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="Weight", usecols=[0, 1, 3, 4])
        if not df.empty:
            df = df.dropna(how='all')
            weight_dict = {}
            size_code_dict = {}
            for _, row in df.iterrows():
                key = (str(row.iloc[0]).strip().lower(), str(row.iloc[1]).strip().lower())
                weight_dict[key] = float(row.iloc[2]) if pd.notna(row.iloc[2]) else 0.0
                size_code_dict[key] = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else ""
            return weight_dict, size_code_dict
    except Exception: pass
    return {}, {}

@st.cache_data(ttl=3600)
def fetch_keg_codes():
    """Returns {format_lower: {"connector": connector_name, "sku_end": sku_end_code}}"""
    sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
    try:
        gconn = st.connection("gsheets", type=GSheetsConnection)
        # Keg sheet: Connector -> SKU End
        sku_end_map = {}
        df_keg = gconn.read(spreadsheet=sheet_url, worksheet="Keg", usecols=[0, 1])
        if not df_keg.empty:
            for _, row in df_keg.dropna(how='all').iterrows():
                k = str(row.iloc[0]).strip().lower()
                v = str(row.iloc[1]).strip()
                if k and k != 'nan' and v and v != 'nan':
                    sku_end_map[k] = v
        # SKU sheet col 0 (Format) + col 3 (Default Connector)
        result = {}
        df_sku = gconn.read(spreadsheet=sheet_url, worksheet="SKU", usecols=[0, 3])
        if not df_sku.empty:
            for _, row in df_sku.dropna(how='all').iterrows():
                fmt = str(row.iloc[0]).strip().lower()
                connector = str(row.iloc[1]).strip()
                if fmt and fmt != 'nan' and connector and connector != 'nan':
                    result[fmt] = {
                        "connector": connector,
                        "sku_end": sku_end_map.get(connector.lower(), ""),
                    }
        return result
    except Exception: pass
    return {}

@st.cache_data(ttl=3600)
def get_beer_style_list():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="Style", usecols=[0])
        if not df.empty:
            return sorted(df.iloc[:, 0].dropna().astype(str).unique().tolist())
    except Exception: pass
    return ["IPA", "Pale Ale"]

def normalize_supplier_names(df, master_list):
    if df is None or df.empty or not master_list: return df
    def match_name(name):
        if not isinstance(name, str): return name
        match, score = process.extractOne(name, master_list)
        return match if score >= 88 else name
    if 'Supplier_Name' in df.columns: df['Supplier_Name'] = df['Supplier_Name'].apply(match_name)
    return df

def clean_product_names(df):
    if df is None or df.empty: return df
    def cleaner(name):
        if not isinstance(name, str): return name
        name = name.replace('|', '')
        name = re.sub(r'\b\d+x\d+cl\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\b\d+g\b', '', name, flags=re.IGNORECASE)
        return ' '.join(name.split())
    if 'Product_Name' in df.columns: df['Product_Name'] = df['Product_Name'].apply(cleaner)
    return df

def create_product_matrix(df):
    if df is None or df.empty: return pd.DataFrame()
    df = df.fillna("")
    if 'Shopify_Status' in df.columns:
        df = df[df['Shopify_Status'] != "✅ Match"]
    if df.empty: return pd.DataFrame()
    group_cols = ['Supplier_Name', 'Collaborator', 'Product_Name', 'ABV']
    grouped = df.groupby(group_cols, sort=False)
    matrix_rows = []
    for name, group in grouped:
        clean_abv_val = clean_abv(name[3])
        if clean_abv_val in ["0", "0.0"]: clean_abv_val = ""
        row = {
            'Supplier_Name': name[0], 'Type': '', 'Collaborator': name[1],
            'Product_Name': name[2], 'ABV': clean_abv_val
        }
        for i, (_, item) in enumerate(group.iterrows()):
            if i >= 3: break
            suffix = str(i + 1)
            row[f'Format{suffix}'] = item['Format']
            row[f'Pack_Size{suffix}'] = item['Pack_Size']
            row[f'Volume{suffix}'] = item['Volume']
            row[f'Item_Price{suffix}'] = item['Item_Price']
            row[f'Split_Case{suffix}'] = item.get('Use_Split', False)
        row['Retry'] = False
        row['Match_Check'] = ""
        row['Manual_UT_ID'] = ""
        row['Ignore_UT'] = False
        matrix_rows.append(row)
    matrix_df = pd.DataFrame(matrix_rows)
    if 'Untappd_Status' not in matrix_df.columns: matrix_df['Untappd_Status'] = ""
    base_cols = ['Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV', 'Untappd_Status', 'Match_Check', 'Retry', 'Manual_UT_ID', 'Ignore_UT']
    format_cols = []
    for i in range(1, 4):
        format_cols.extend([f'Format{i}', f'Pack_Size{i}', f'Volume{i}', f'Item_Price{i}', f'Split_Case{i}'])
    existing_format_cols = [c for c in format_cols if c in matrix_df.columns]
    final_cols = base_cols + existing_format_cols
    for col in final_cols:
        if col not in matrix_df.columns:
            matrix_df[col] = False if ("Split_Case" in col or "Retry" in col or "Ignore_UT" in col) else ""
    for col in final_cols:
        if "Split_Case" not in col and "Retry" not in col and "Ignore_UT" not in col and "Item_Price" not in col:
            if matrix_df[col].dtype == 'object':
                matrix_df[col] = matrix_df[col].fillna("").astype(str)
                if "ABV" not in col:
                    matrix_df[col] = matrix_df[col].str.replace(r'\.0$', '', regex=True)
    return matrix_df[final_cols]

def generate_sku_parts(product_name):
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', str(product_name)).upper()
    words = clean_name.split()
    if not words: return "XXXX"
    if len(words) >= 4: return "".join([w[0] for w in words[:4]])
    elif len(words) >= 2: return (words[0][:2] + words[1][:2]).ljust(4, 'X')[:4]
    else: return (words[0][:2] + "XX").ljust(4, 'X')[:4]

def stage_products_for_upload(matrix_df):
    if matrix_df.empty: return pd.DataFrame(), []
    new_rows = []
    errors = []
    fallback_map = fetch_fallback_images()
    required_manual = ['Untappd_ABV', 'Untappd_Style', 'Untappd_Desc']
    for idx, row in matrix_df.iterrows():
        brand_name = str(row.get('Untappd_Brewery', '')).strip() or str(row.get('Supplier_Name', '')).strip()
        prod_name = str(row.get('Untappd_Product', '')).strip() or str(row.get('Product_Name', '')).strip()
        img_url = str(row.get('Label_Thumb', '')).strip()
        if not img_url:
            lookup_key = str(row.get('Supplier_Name', '')).lower().strip()
            if lookup_key in fallback_map: img_url = fallback_map[lookup_key]
        missing_vals = [f for f in required_manual if not str(row.get(f, '')).strip()]
        if missing_vals:
            errors.append(f"Row {idx+1} ({prod_name}): Missing mandatory fields: {', '.join(missing_vals)}. Please fill in Tab 3.")
            continue
        # Canonical ABV clean before staging
        clean_abv_val = clean_abv(row.get('Untappd_ABV', ''))
        for i in range(1, 4):
            fmt_val = str(row.get(f'Format{i}', '')).strip()
            if fmt_val and fmt_val.lower() not in ['nan', 'none']:
                new_rows.append({
                    'untappd_brewery': brand_name, 'collaborator': row.get('Collaborator', ''),
                    'untappd_product': prod_name, 'untappd_abv': clean_abv_val,
                    'untappd_ibu': row.get('Untappd_IBU', 0), 'untappd_country': row.get('Untappd_Country', ''),
                    'untappd_style': row.get('Untappd_Style', ''), 'description': row.get('Untappd_Desc', ''),
                    'format': fmt_val, 'pack_size': row.get(f'Pack_Size{i}', ''),
                    'volume': row.get(f'Volume{i}', ''), 'item_price': row.get(f'Item_Price{i}', ''),
                    'is_split_case': row.get(f'Split_Case{i}', False), 'Label_Thumb': img_url,
                    'Untappd_ID': row.get('Untappd_ID', ''), 'Brewery_Loc': row.get('Brewery_Loc', ''),
                    'Family_SKU': '', 'Variant_SKU': '', 'Family_Name': '', 'Variant_Name': '',
                    'Weight': 0.0, 'Keg_Connector': '', 'Attribute_5': 'Rotational Product',
                    'Type': row.get('Type', '')
                })
    return pd.DataFrame(new_rows), errors

def build_price_check_from_matched_lines(line_items_df):
    """
    Auto-populate price check from reconciled matched lines.
    Returns a DataFrame ready for Tab 6 with invoice cost vs current Cin7 price.
    """
    if line_items_df is None or line_items_df.empty:
        return pd.DataFrame()
    matched = line_items_df[line_items_df.get('Shopify_Status', '') == "✅ Match"].copy() if 'Shopify_Status' in line_items_df.columns else pd.DataFrame()
    if matched.empty:
        return pd.DataFrame()
    # One price-check row per SKU — mirrors the final PO (no duplicates across pack sizes)
    matched = matched.drop_duplicates(subset='London_SKU', keep='first')
    rows = []
    for _, row in matched.iterrows():
        london_sku = str(row.get('London_SKU', '')).strip()
        if not london_sku:
            continue
        raw_price = float(row.get('Item_Price', 0))
        invoice_cost = raw_price / 2 if row.get('Use_Split', False) else raw_price
        # Emit one row for each depot prefix (L and G)
        for prefix, other in [("L-", "G-"), ("G-", "L-")]:
            if london_sku.startswith("L-"):
                sku = london_sku if prefix == "L-" else "G-" + london_sku[2:]
            else:
                sku = london_sku if prefix == "G-" else "L-" + london_sku[2:]
            prod_id, current_cin7_price, cin7_full_name, attr_5, cin7_abv = fetch_cin7_product_details_by_sku(sku)
            recommended_price = calculate_sell_price(invoice_cost, attr_5, str(row.get('Format', '')))
            price_diff = round(recommended_price - current_cin7_price, 2)
            pct_change = round((price_diff / current_cin7_price) * 100, 1) if current_cin7_price else 0
            flag = "⚠️ Review" if abs(pct_change) > 0 else "✅ OK"
            rows.append({
                "SKU": sku,
                "Product": str(row.get('Product_Name', '')),
                "Variant": str(row.get('Matched_Variant', '')),
                "ABV": cin7_abv,
                "Invoice_Cost": invoice_cost,
                "Current_Cin7_Price": current_cin7_price,
                "Recommended_Price": recommended_price,
                "Change_%": pct_change,
                "Flag": flag,
                "Cin7_ID": prod_id or "",
                "Cin7_Name": cin7_full_name,
                "Attr5": attr_5,
            })
    return pd.DataFrame(rows)

# ==========================================
# 2. SESSION & SIDEBAR
# ==========================================

# Initialise from DEFAULT_STATE — no key is ever missed
for k, v in DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Keys that need counters (not in DEFAULT_STATE)
if 'master_suppliers' not in st.session_state: st.session_state.master_suppliers = fetch_cin7_brands()
if 'drive_files' not in st.session_state: st.session_state.drive_files = []
if 'cin7_all_suppliers' not in st.session_state: st.session_state.cin7_all_suppliers = fetch_all_cin7_suppliers_cached()
if 'line_items_key' not in st.session_state: st.session_state.line_items_key = 0
if 'matrix_key' not in st.session_state: st.session_state.matrix_key = 0

with st.sidebar:
    st.header("Settings")
    if "GOOGLE_API_KEY" in st.secrets:
        api_key = st.secrets["GOOGLE_API_KEY"]
        st.success("API Key Loaded 🔑")
    else:
        api_key = st.text_input("Enter API Key", type="password")

    if st.button("🛠️ List Available Models"):
        if api_key:
            try:
                client = genai.Client(api_key=api_key)
                models = client.models.list()
                st.write("### Gemini Models Found:")
                for m in models:
                    if "gemini" in m.name.lower(): st.code(f"{m.name}")
            except Exception as e: st.error(f"Error: {e}")

    st.divider()
    with st.expander("🔌 Connection Status", expanded=False):
        st.write(f"**Gemini AI:** {'✅ Ready' if api_key else '❌ Missing'}")
        if "shopify" in st.secrets: st.write(f"**Shopify:** ✅ `{st.secrets['shopify'].get('shop_url', 'Unknown')}`")
        else: st.write("**Shopify:** ❌ Missing")
        if "cin7" in st.secrets: st.write(f"**Cin7:** ✅ Loaded")
        else: st.write("**Cin7:** ❌ Missing")
        if "untappd" in st.secrets: st.write("**Untappd:** ✅ Ready")
        else: st.write("**Untappd:** ❌ Missing")
        if "connections" in st.secrets and "gsheets" in st.secrets["connections"]:
            st.write("**GSheets Auth:** ✅ Connected")
        else: st.write("**GSheets Auth:** ❌ Missing")

    st.subheader("🧪 The Lab")
    with st.form("teaching_form"):
        st.caption("Test a new rule here. Press Ctrl+Enter to apply.")
        custom_rule = st.text_area("Inject Temporary Rule:", height=100, key="lab_custom_rule")
        st.form_submit_button("Set Rule")

    if custom_rule:
        st.markdown("---")
        st.caption("💾 **Save to Knowledge Base**")
        st.caption("Copy this snippet into `SUPPLIER_RULEBOOK`:")
        current_supplier = "Unknown Supplier"
        if st.session_state.header_data is not None and not st.session_state.header_data.empty:
            current_supplier = st.session_state.header_data.iloc[0].get('Payable_To', 'Unknown Supplier')
        formatted_rule = f'   "{current_supplier}": """\n   {custom_rule.strip()}\n   """,\n'
        st.code(formatted_rule, language="python")

    st.divider()
    if st.button("Log Out"):
        st.session_state.password_correct = False
        st.rerun()

# ==========================================
# 3. MAIN UI
# ==========================================

st.subheader("1. Select Invoice Source")
tab_upload, tab_drive = st.tabs(["⬆️ Manual Upload", "☁️ Google Drive"])

target_stream = None
source_name = "Unknown"

with tab_upload:
    uploaded_file = st.file_uploader("Drop PDF here", type="pdf")
    if uploaded_file:
        target_stream = uploaded_file
        source_name = uploaded_file.name

with tab_drive:
    col_d1, col_d2 = st.columns([3, 1])
    with col_d1:
        folder_id = st.text_input("Drive Folder ID", help="Copy the ID string from the URL")
    with col_d2:
        st.write("")
        st.write("")
        if st.button("🔍 Scan Folder"):
            if folder_id:
                try:
                    with st.spinner("Scanning..."):
                        files = list_files_in_folder(folder_id)
                        st.session_state.drive_files = files
                    if files: st.success(f"Found {len(files)} PDFs!")
                    else: st.warning("No PDFs found.")
                except Exception as e: st.error(f"Error: {e}")

    if st.session_state.drive_files:
        file_names = [f['name'] for f in st.session_state.drive_files]
        selected_name = st.selectbox("Select Invoice from Drive List:", options=file_names, index=None, placeholder="Choose a file...")
        if selected_name:
            file_data = next(f for f in st.session_state.drive_files if f['name'] == selected_name)
            st.session_state.selected_drive_id = file_data['id']
            st.session_state.selected_drive_name = file_data['name']
            if not uploaded_file: source_name = selected_name

if st.button("🚀 Process Invoice", type="primary"):
    if not uploaded_file and st.session_state.selected_drive_id:
        try:
            with st.status(f"Downloading {source_name}...", expanded=False) as status:
                target_stream = download_file_from_drive(st.session_state.selected_drive_id)
                status.update(label="Download Complete", state="complete")
        except Exception as e:
            st.error(f"Download Failed: {e}")
            st.stop()

    if target_stream and api_key:
        try:
            with st.status("Processing Document...", expanded=True) as status:
                client = genai.Client(api_key=api_key)
                st.write("1. Converting PDF to Images (OCR Prep)...")
                target_stream.seek(0)
                images = convert_from_bytes(target_stream.read(), dpi=200)

                st.write(f"2. Extracting Text from {len(images)} pages...")
                full_text = ""
                for i, img in enumerate(images):
                    st.write(f"   - Scanning page {i+1}...")
                    full_text += pytesseract.image_to_string(img, config='--psm 6') + "\n"

                st.write("3. Sending Text to AI Model...")
                injected = f"\n!!! USER OVERRIDE !!!\n{custom_rule}\n" if custom_rule else ""

                detected_supplier = ""
                for supplier_name in SUPPLIER_RULEBOOK:
                    if supplier_name.lower().replace("&", "and") in full_text.lower().replace("&", "and"):
                        detected_supplier = supplier_name
                        break

                supplier_rule = SUPPLIER_RULEBOOK.get(detected_supplier, "")
                supplier_rule_block = f"SUPPLIER SPECIFIC RULES FOR {detected_supplier}:\n{supplier_rule}" if supplier_rule else ""

                prompt = f"""
                Extract invoice data to JSON.

                RULES FOR ABV:
                1. IF ABV IS NOT FOUND, RETURN null (DO NOT RETURN 0 or "0").
                2. ONLY RETURN "0" IF THE PRODUCT IS EXPLICITLY "0%", "AF", "ALCOHOL FREE".
                3. EXTRACT AS A STRING (e.g. "4.5%", "0.5%").

                STRUCTURE:
                {{
                    "header": {{
                        "Payable_To": "Supplier Name", "Invoice_Number": "...", "Issue_Date": "...",
                        "Payment_Terms": "...", "Due_Date": "...", "Total_Net": 0.00,
                        "Total_VAT": 0.00, "Total_Gross": 0.00, "Total_Discount_Amount": 0.00, "Shipping_Charge": 0.00
                    }},
                    "line_items":[
                        {{
                            "Supplier_Name": "...", "Collaborator": "...", "Product_Name": "...",
                            "ABV": null,
                            "Format": "...", "Pack_Size": "...", "Volume": "...", "Quantity": 1, "Item_Price": 10.00, "Line_Total": 10.00
                        }}
                    ]
                }}
                {supplier_rule_block}
                GLOBAL RULES: {GLOBAL_RULES_TEXT}
                {injected}
                INVOICE TEXT:
                {full_text}
                """

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                        break
                    except Exception as e:
                        if "503" in str(e) and attempt < max_retries - 1:
                            time.sleep(2 ** (attempt + 1))
                            continue
                        else: raise e

                st.write("4. Parsing Response...")
                try:
                    json_text = response.text.strip().replace("```json", "").replace("```", "")
                    data = json.loads(json_text)
                except Exception as e:
                    st.error(f"AI returned invalid JSON: {response.text}")
                    st.stop()

                st.write("5. Finalizing Data...")
                st.session_state.header_data = pd.DataFrame([data['header']])
                st.session_state.header_data['Cin7_Supplier_ID'] = ""
                st.session_state.header_data['Cin7_Supplier_Name'] = ""

                df_lines = pd.DataFrame(data['line_items'])
                df_lines.columns = [c.strip() for c in df_lines.columns]
                df_lines.rename(columns=lambda x: 'ABV' if x.lower() == 'abv' else x, inplace=True)

                # Canonical ABV clean — single point after AI parse
                df_lines = apply_clean_abv_to_df(df_lines, 'ABV')

                df_lines = clean_product_names(df_lines)
                if st.session_state.master_suppliers:
                    df_lines = normalize_supplier_names(df_lines, st.session_state.master_suppliers)

                df_lines['Shopify_Status'] = "Pending"
                df_lines['Use_Split'] = False
                df_lines['Strict_Search'] = False
                df_lines['Manual_Shopify_SKU'] = ""
                df_lines['Recheck'] = True

                cols = ["Recheck", "Manual_Shopify_SKU", "Use_Split", "Strict_Search", "Supplier_Name",
                        "Collaborator", "Product_Name", "ABV", "Format", "Pack_Size", "Volume",
                        "Item_Price", "Line_Total", "Quantity"]
                existing = [c for c in cols if c in df_lines.columns]
                st.session_state.line_items = df_lines[existing]

                st.session_state.shopify_logs = []
                st.session_state.untappd_logs = []
                st.session_state.matrix_data = None
                st.session_state.upload_data = None
                st.session_state.upload_generated = False
                st.session_state.price_check_data = None
                st.session_state.polykeg_selections = {}
                st.session_state.line_items_key += 1

                status.update(label="Processing Complete!", state="complete", expanded=False)

        except Exception as e:
            st.error(f"Critical Error: {e}")
    else:
        st.warning("Please upload a file or select one from Google Drive first.")

# ==========================================
# 4. RESULTS DISPLAY
# ==========================================

if st.session_state.header_data is not None:
    if custom_rule: st.success("✅ Used Custom Rules")
    st.divider()

    df = st.session_state.line_items
    if 'Shopify_Status' in df.columns: unmatched_count = len(df[df['Shopify_Status'] != "✅ Match"])
    else: unmatched_count = len(df)
    all_matched = (unmatched_count == 0) and ('Shopify_Status' in df.columns)

    tabs = ["📝 1. Line Items", "🔍 2. Prepare Search", "🍺 3. Prepare Upload", "☁️ 4. Product Upload", "🚀 5. Finalize PO", "💰 6. Price Check"]
    current_tabs = st.tabs(tabs)

    # -------------------------
    # TAB 1: LINE ITEMS
    # -------------------------
    with current_tabs[0]:
        st.subheader("1. Review & Edit Lines")

        display_df = st.session_state.line_items.copy()

        ideal_order = [
            'Recheck', 'Manual_Shopify_SKU',
            'Use_Split', 'Strict_Search', 'Shopify_Status', 'Match_Score',
            'Matched_Product', 'Matched_Variant', 'Image',
            'Supplier_Name', 'Collaborator', 'Product_Name', 'ABV', 'Format',
            'Pack_Size', 'Volume', 'Quantity', 'Item_Price', 'Line_Total',
            'Shopify_Variant_ID', 'London_SKU', 'Gloucester_SKU'
        ]
        final_cols = [c for c in ideal_order if c in display_df.columns]
        rem = [c for c in display_df.columns if c not in final_cols]
        display_df = display_df[final_cols + rem]

        column_config = {
            "Image": st.column_config.ImageColumn("Img"),
            "Shopify_Status": st.column_config.TextColumn("Status", disabled=True),
            "Match_Score": st.column_config.TextColumn("Match %", disabled=True, width="small",
                help="Fuzzy match confidence. Green ≥ 90, amber 70–89, red < 70."),
            "Matched_Product": st.column_config.TextColumn("Shopify Match", disabled=True),
            "Matched_Variant": st.column_config.TextColumn("Variant Match", disabled=True),
            "Recheck": st.column_config.CheckboxColumn("Recheck?", width="small",
                help="Tick to include this line in Recheck Selected. Auto-ticked for unmatched rows."),
            "Manual_Shopify_SKU": st.column_config.TextColumn("Manual SKU Override", width="medium",
                help="Paste any L- or G- SKU from Shopify to force-match this line and skip fuzzy search"),
            "Use_Split": st.column_config.CheckboxColumn("Order Split?", width="small", help="Tick to order half-case"),
            "Strict_Search": st.column_config.CheckboxColumn("Strict?", width="small", help="Tick to force exact name matching"),
            "Line_Total": st.column_config.NumberColumn("Line Total", format="£%.2f")
        }

        with st.form(key=f"line_items_form_{st.session_state.line_items_key}"):
            st.info("✏️ **Make your edits below, then click 'Save Changes' before checking inventory.**")

            edited_lines = st.data_editor(
                display_df,
                num_rows="dynamic",
                width='stretch',
                key=f"line_editor_{st.session_state.line_items_key}",
                column_config=column_config
            )

            save_clicked = st.form_submit_button("💾 Save Changes", type="primary")

            if save_clicked:
                # Diff before save — show what changed
                original = st.session_state.line_items.copy()
                changed_cols = [c for c in edited_lines.columns if c in original.columns]
                diff_rows = []
                for i, (orig_row, new_row) in enumerate(zip(original.to_dict('records'), edited_lines.to_dict('records'))):
                    for col in changed_cols:
                        if str(orig_row.get(col, '')) != str(new_row.get(col, '')):
                            diff_rows.append({
                                "Row": i + 1,
                                "Field": col,
                                "Before": orig_row.get(col, ''),
                                "After": new_row.get(col, '')
                            })

                # Auto-recalculate Item_Price from Line_Total / Quantity
                if 'Line_Total' in edited_lines.columns and 'Quantity' in edited_lines.columns:
                    for idx, row in edited_lines.iterrows():
                        try:
                            qty = float(row['Quantity'])
                            lt = float(row['Line_Total'])
                            if qty > 0 and pd.notna(lt):
                                edited_lines.at[idx, 'Item_Price'] = round(lt / qty, 2)
                        except Exception:
                            pass

                st.session_state.line_items = edited_lines

                if diff_rows:
                    with st.expander(f"📋 {len(diff_rows)} field(s) changed — expand to review", expanded=True):
                        st.dataframe(pd.DataFrame(diff_rows), use_container_width=True)

                st.success("✅ Changes saved and Item Prices recalculated!")
                st.rerun()

        st.divider()
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if "shopify" in st.secrets:
                if st.button("🛒 Check Inventory", help="Full check — resets and rematches all lines"):
                    with st.spinner("Checking..."):
                        updated_lines, logs = run_reconciliation_check(st.session_state.line_items)
                        st.session_state.line_items = updated_lines
                        st.session_state.shopify_logs = logs
                        st.session_state.matrix_data = create_product_matrix(updated_lines)
                        st.session_state.line_items_key += 1
                        st.session_state.matrix_key += 1
                        st.session_state.price_check_data = build_price_check_from_matched_lines(updated_lines)
                        st.success("Check Complete!")
                        st.rerun()
        with col2:
            if "shopify" in st.secrets:
                recheck_count = int(st.session_state.line_items.get('Recheck', pd.Series(dtype=bool)).fillna(True).sum()) \
                    if 'Recheck' in st.session_state.line_items.columns else len(st.session_state.line_items)
                if st.button(f"🔄 Recheck Selected ({recheck_count})", help="Only recheck ticked rows — preserves existing matches"):
                    with st.spinner("Rechecking selected lines..."):
                        updated_lines, logs = run_reconciliation_check(st.session_state.line_items, recheck_only=True)
                        st.session_state.line_items = updated_lines
                        st.session_state.shopify_logs = logs
                        st.session_state.matrix_data = create_product_matrix(updated_lines)
                        st.session_state.line_items_key += 1
                        st.session_state.matrix_key += 1
                        st.session_state.price_check_data = build_price_check_from_matched_lines(updated_lines)
                        st.success("Recheck Complete!")
                        st.rerun()
        with col3:
            st.download_button("📥 Download Lines CSV", st.session_state.line_items.to_csv(index=False), "lines.csv")

        if st.session_state.shopify_logs:
            with st.expander("🕵️ Debug Logs", expanded=False):
                st.markdown("\n".join(st.session_state.shopify_logs))

    # -------------------------
    # TAB 2: PREPARE MISSING ITEMS
    # -------------------------
    with current_tabs[1]:
        st.subheader("2. Prepare Missing Items for Search")

        if all_matched:
            st.success("🎉 All products matched to Shopify! No action needed here.")
        elif st.session_state.matrix_data is not None and not st.session_state.matrix_data.empty:

            search_has_run = False
            if 'Untappd_Status' in st.session_state.matrix_data.columns:
                status_vals = st.session_state.matrix_data['Untappd_Status'].astype(str).unique()
                if any(v.strip() for v in status_vals): search_has_run = True

            if search_has_run:
                # Highlight low-confidence rows
                low_conf_count = len(st.session_state.matrix_data[
                    st.session_state.matrix_data.get('UT_Confidence', '') == "⚠️ Low Confidence"
                ]) if 'UT_Confidence' in st.session_state.matrix_data.columns else 0
                if low_conf_count:
                    st.warning(f"⚠️ **{low_conf_count} row(s)** have low Untappd confidence. Review Match Details carefully before proceeding.")
                st.info("👇 **Review matches.** If a match is wrong, paste the URL in 'Manual ID', OR tick 'Ignore UT' to type it yourself in Tab 3.")
            else:
                st.info("👇 Select the **Product Type** for each item below, then click Search.")

            type_options = ["Beer", "Cider", "Spirits", "Softs", "Wine", "Merch", "Dispense", "Snacks", "PoS", "Other", "Free Of Charge PoS"]

            prep_config = {
                "Type": st.column_config.SelectboxColumn("Product Type", options=type_options, required=True, width="medium"),
                "Untappd_Status": st.column_config.TextColumn("UT Status", disabled=True, width="small"),
                "UT_Confidence": st.column_config.TextColumn("Confidence", disabled=True, width="small"),
                "Match_Check": st.column_config.TextColumn("Match Details (Verify Here)", disabled=True, width="large"),
                "Retry": st.column_config.CheckboxColumn("Retry?", width="small"),
                "Manual_UT_ID": st.column_config.TextColumn("Manual ID/URL", width="medium"),
                "Ignore_UT": st.column_config.CheckboxColumn("Ignore UT?", width="small"),
            }

            for i in range(1, 4):
                prep_config[f"Format{i}"] = st.column_config.TextColumn(f"Format {i}", width="small")
                prep_config[f"Pack_Size{i}"] = st.column_config.TextColumn(f"Pack {i}", width="small")
                prep_config[f"Volume{i}"] = st.column_config.TextColumn(f"Vol {i}", width="small")
                prep_config[f"Item_Price{i}"] = st.column_config.NumberColumn(f"Cost {i}", format="£%.2f", width="small")
                prep_config[f"Split_Case{i}"] = st.column_config.CheckboxColumn(f"Split {i}?", width="small")

            if search_has_run:
                base_cols = ['Ignore_UT', 'Retry', 'Manual_UT_ID', 'Untappd_Status', 'UT_Confidence', 'Match_Check', 'Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']
            else:
                base_cols = ['Ignore_UT', 'Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']

            ordered_cols = base_cols.copy()
            for i in range(1, 4):
                if f"Format{i}" in st.session_state.matrix_data.columns:
                    ordered_cols.extend([f"Format{i}", f"Pack_Size{i}", f"Volume{i}", f"Item_Price{i}", f"Split_Case{i}"])

            display_cols = [c for c in ordered_cols if c in st.session_state.matrix_data.columns]

            for col in display_cols:
                if any(x in col for x in ["Pack_Size", "Volume", "Format"]):
                    st.session_state.matrix_data[col] = (
                        st.session_state.matrix_data[col]
                        .fillna("").astype(str)
                        .str.replace(r'\.0$', '', regex=True)
                        .replace("nan", "")
                    )

            with st.form(key=f"prep_form_{st.session_state.matrix_key}"):
                st.caption("✏️ **Make your edits below, then click 'Save Changes' before searching.**")
                edited_prep = st.data_editor(
                    st.session_state.matrix_data[display_cols],
                    num_rows="fixed",
                    width='stretch',
                    column_config=prep_config,
                    key=f"prep_editor_{st.session_state.matrix_key}"
                )
                save_prep_clicked = st.form_submit_button("💾 Save Changes", type="primary")
                if save_prep_clicked:
                    st.session_state.matrix_data.update(edited_prep)
                    st.success("✅ Changes saved successfully!")
                    st.rerun()

            st.divider()
            col_search, col_help = st.columns([1, 2])
            with col_help:
                st.markdown("**Search Logs:**")
                log_placeholder = st.empty()
                if st.session_state.untappd_logs:
                    log_placeholder.code("\n".join(st.session_state.untappd_logs), language="text")
                else:
                    log_placeholder.info("Ready to search.")
            with col_search:
                missing_types = st.session_state.matrix_data['Type'].replace('', pd.NA).isna().sum()
                btn_label = "🔎 Search Untappd Details" if not search_has_run else "🔎 Search Again / Retry"
                if st.button(btn_label):
                    if missing_types > 0:
                        st.error(f"⚠️ Please select a Product Type for all {missing_types} rows above before searching.")
                    elif "untappd" in st.secrets:
                        log_placeholder.empty()
                        with st.spinner("Searching Untappd API..."):
                            updated_matrix, u_logs = batch_untappd_lookup(
                                st.session_state.matrix_data,
                                status_box=log_placeholder
                            )
                            st.session_state.matrix_data = updated_matrix
                            st.session_state.untappd_logs = u_logs
                            st.session_state.matrix_key += 1
                            st.success("Search Complete!")
                            st.rerun()
                    else: st.error("Untappd Secrets Missing")

    # -------------------------
    # TAB 3: PREPARE UPLOAD
    # -------------------------
    with current_tabs[2]:
        st.subheader("3. Review matches and add missing product information")

        has_untappd_cols = 'Untappd_Status' in st.session_state.matrix_data.columns if st.session_state.matrix_data is not None else False

        if not has_untappd_cols:
            st.warning("⚠️ Please run the search in 'Tab 2. Prepare Search' first.")

        elif st.session_state.matrix_data is not None and not st.session_state.matrix_data.empty:
            st.info("👇 These details will be used to create products in Cin7. Invoice columns are shown read-only for comparison.")

            u_cols = ['Untappd_Status', 'UT_Confidence', 'Label_Thumb', 'Untappd_Brewery', 'Untappd_Product', 'Untappd_ABV', 'Untappd_Style', 'Untappd_Desc']
            # Read-only invoice columns alongside for comparison
            invoice_cols = ['Supplier_Name', 'Product_Name', 'ABV', 'Format1']
            full_view = u_cols + [c for c in invoice_cols if c in st.session_state.matrix_data.columns]

            column_config = {
                "Label_Thumb": st.column_config.ImageColumn("Label", width="small"),
                "Untappd_Status": st.column_config.TextColumn("Status", disabled=True),
                "UT_Confidence": st.column_config.TextColumn("Confidence", disabled=True, width="small"),
                "Untappd_Style": st.column_config.SelectboxColumn("Style", options=get_beer_style_list(), width="medium"),
                "Untappd_Desc": st.column_config.TextColumn("Description", width="large"),
                "Untappd_Brewery": st.column_config.TextColumn("Brand (Cin7)", width="medium"),
                "Untappd_Product": st.column_config.TextColumn("Product Name (Cin7)", width="medium"),
                # Invoice cols — locked, for reference
                "Supplier_Name": st.column_config.TextColumn("Invoice Supplier", disabled=True, width="medium"),
                "Product_Name": st.column_config.TextColumn("Invoice Product", disabled=True, width="medium"),
                "ABV": st.column_config.TextColumn("Invoice ABV", disabled=True, width="small"),
                "Format1": st.column_config.TextColumn("Invoice Format", disabled=True, width="small"),
            }

            with st.form(key=f"match_form_{st.session_state.matrix_key}"):
                st.caption("✏️ **Make your edits below, then click 'Save Changes' before validating.**")
                edited_matches = st.data_editor(
                    st.session_state.matrix_data,
                    column_order=full_view,
                    num_rows="fixed",
                    width='stretch',
                    key=f"match_editor_{st.session_state.matrix_key}",
                    column_config=column_config
                )
                save_match_clicked = st.form_submit_button("💾 Save Changes", type="primary")
                if save_match_clicked:
                    # Canonical ABV clean on save
                    if 'Untappd_ABV' in edited_matches.columns:
                        edited_matches['Untappd_ABV'] = edited_matches['Untappd_ABV'].apply(clean_abv)
                    st.session_state.matrix_data = edited_matches
                    st.success("✅ Changes saved successfully!")
                    st.rerun()

            st.divider()
            if st.button("✨ Validate & Stage for Upload", type="primary"):
                if 'Untappd_ABV' in st.session_state.matrix_data.columns:
                    st.session_state.matrix_data['Untappd_ABV'] = st.session_state.matrix_data['Untappd_ABV'].apply(clean_abv)
                staged_df, errors = stage_products_for_upload(st.session_state.matrix_data)
                if errors:
                    for e in errors: st.error(e)
                else:
                    st.session_state.upload_data = staged_df
                    st.session_state.upload_generated = False
                    st.success("Products staged successfully! Go to Tab 4.")

    # -------------------------
    # TAB 4: PRODUCT UPLOAD
    # -------------------------
    with current_tabs[3]:
        st.subheader("4. Product Upload Stage")

        if st.session_state.upload_data is not None and not st.session_state.upload_data.empty:
            if st.button("🛠️ Generate Upload Data"):
                supplier_map = fetch_supplier_codes()
                format_map = fetch_format_codes()
                weight_map, size_code_map = fetch_weight_map()
                keg_map = fetch_keg_codes()
                if not keg_map:
                    st.warning("⚠️ Keg connector map is empty — check the SKU worksheet in the reference spreadsheet.")
                today_str = datetime.now().strftime('%d%m%Y')
                processed_rows = []

                for idx, row in st.session_state.upload_data.iterrows():
                    supp_name = str(row.get('untappd_brewery', '')).strip()
                    prod_name = str(row.get('untappd_product', '')).strip()
                    collaborator = str(row.get('collaborator', '')).strip()
                    fmt_name = str(row.get('format', '')).strip()
                    vol_name = str(row.get('volume', '')).strip()
                    attr_5 = row.get('Attribute_5', 'Rotational Product')
                    prod_type = row.get('Type', '')
                    abv_val = clean_abv(row.get('untappd_abv', ''))

                    lookup_key = (fmt_name.lower(), vol_name.lower())
                    unit_weight = weight_map.get(lookup_key, 0.0)
                    size_code = size_code_map.get(lookup_key, "00")

                    s_code = supplier_map.get(supp_name, "XXXX")
                    p_code = generate_sku_parts(prod_name)
                    f_code = format_map.get(fmt_name.lower(), "UN")

                    family_sku = f"{s_code}{p_code}-{today_str}-{idx}-{f_code}"

                    if collaborator and collaborator.lower() not in ["", "nan", "none"]:
                        display_supplier = f"{supp_name} + {collaborator}"
                    else:
                        display_supplier = supp_name

                    pack_raw = str(row.get('pack_size', '1'))
                    pack_nums = re.findall(r'\d+', pack_raw)
                    pack_int = int(pack_nums[0]) if pack_nums else 1
                    is_split = bool(row.get('is_split_case', False))
                    if is_split: pack_int = pack_int * 2

                    keg_info = keg_map.get(fmt_name.lower(), {})
                    cost_price = float(str(row.get('item_price', 0)).replace('£', '').strip() or 0)
                    if is_split: cost_price = cost_price / 2
                    sales_price = calculate_sell_price(cost_price, attr_5, fmt_name)

                    abv_str = f"{abv_val}%" if abv_val else ""
                    family_name = f"{display_supplier} / {prod_name} / {abv_str} / {fmt_name}" if abv_str else f"{display_supplier} / {prod_name} / {fmt_name}"

                    # PolyKeg generates two variants (Sankey + KeyKeg); all others generate one
                    is_polykeg = fmt_name.lower() == "polykeg"
                    coupler_variants = [
                        {"connector": "Sankey Coupler", "sku_end": "ST"},
                        {"connector": "KeyKeg Coupler", "sku_end": "KKT"},
                    ] if is_polykeg else [
                        {"connector": keg_info.get("connector", ""), "sku_end": keg_info.get("sku_end", "")}
                    ]

                    for coupler in coupler_variants:
                        keg_connector = coupler["connector"]
                        keg_sku_end = coupler["sku_end"]
                        if pack_int and pack_int > 1:
                            variant_name = f"{pack_int}x{vol_name}"
                        elif keg_connector:
                            variant_name = f"{vol_name} - {keg_connector}"
                        else:
                            variant_name = vol_name
                        sku_size = f"{pack_int}X{size_code}" if pack_int > 1 else f"{size_code}{keg_sku_end}"
                        variant_sku_base = f"{family_sku}-{sku_size}"
                        processed_rows.append({
                            **row.to_dict(),
                            'Family_SKU': family_sku,
                            'Variant_SKU': variant_sku_base,
                            'Family_Name': family_name,
                            'Variant_Name': variant_name,
                            'Weight': unit_weight * pack_int,
                            'Keg_Connector': keg_connector,
                            'Sales_Price': sales_price,
                            'item_price': cost_price,
                            'untappd_abv': abv_val,
                        })

                st.session_state.upload_data = pd.DataFrame(processed_rows)
                st.session_state.upload_generated = True
                st.success("✅ Upload data generated!")

            if st.session_state.upload_generated and st.session_state.upload_data is not None:
                st.dataframe(st.session_state.upload_data, use_container_width=True)
                st.download_button("📥 Download Upload CSV", st.session_state.upload_data.to_csv(index=False), "upload.csv")

                col_c, col_s = st.columns(2)
                cin7_status_box = st.empty()
                shopify_status_box = st.empty()

                with col_c:
                    if st.button("🚀 Create Cin7 Products", type="primary"):
                        with st.spinner("Syncing to Cin7..."):
                            log, cin7_links = sync_product_to_cin7(st.session_state.upload_data, status_box=cin7_status_box)
                            st.session_state.cin7_complete = True
                            st.session_state.cin7_log_text = "\n".join(log)
                            st.session_state.cin7_links = cin7_links
                            st.rerun()

                with col_s:
                    if st.button("🛍️ Create Shopify Products", type="primary"):
                        if not st.session_state.cin7_complete:
                            st.warning("⚠️ Sync to Cin7 first before creating Shopify products.")
                        else:
                            loc_ids = fetch_shopify_location_ids()
                            pub_ids = fetch_publication_ids()
                            shopify_log = []
                            shopify_links = []
                            total_rows = len(st.session_state.upload_data)
                            prog = st.progress(0)
                            def shopify_update_log(msg):
                                shopify_log.append(msg)
                                shopify_status_box.code("\n".join(shopify_log), language="text")
                            shopify_update_log(f"🚀 Starting Shopify sync for {total_rows} rows...")
                            created_shopify_products = {}  # f"{loc}-{family_name}" -> product_id
                            creds = st.secrets["shopify"]
                            shop_url = creds.get("shop_url")
                            token = creds.get("access_token")
                            version = creds.get("api_version", "2024-04")
                            s_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
                            for i, (_, row) in enumerate(st.session_state.upload_data.iterrows()):
                                prog.progress((i + 1) / total_rows)
                                fam_name = row.get('Family_Name', row.get('Variant_SKU', f'Row {i+1}'))
                                for loc_prefix in ["L", "G"]:
                                    is_london = loc_prefix == "L"
                                    full_sku = f"{loc_prefix}-{row.get('Variant_SKU', '')}"
                                    product_key = f"{loc_prefix}-{fam_name}"
                                    shopify_update_log(f"\n🔄 [{loc_prefix}] {fam_name}")
                                    variant_payload = create_shopify_variant_payload(row, loc_prefix)
                                    try:
                                        if product_key in created_shopify_products:
                                            # PolyKeg second coupler: add variant to existing product
                                            existing_id = created_shopify_products[product_key]
                                            shopify_update_log(f"   🔀 PolyKeg: adding variant to product {existing_id}")
                                            r = requests.post(
                                                f"https://{shop_url}/admin/api/{version}/products/{existing_id}/variants.json",
                                                json={"variant": variant_payload},
                                                headers=s_headers
                                            )
                                            if r.status_code == 201:
                                                new_var = r.json().get('variant', {})
                                                variant_id = new_var.get('id')
                                                variant_title = new_var.get('title', '')
                                                shopify_update_log(f"   ✅ Variant added: {variant_title} (ID: {variant_id})")
                                                if variant_id:
                                                    shopify_links.append({"label": f"{fam_name} — {variant_title} ({loc_prefix})", "url": f"https://{shop_url}/admin/products/{existing_id}/variants/{variant_id}"})
                                                if loc_ids:
                                                    inv_item_id = new_var.get('inventory_item_id')
                                                    target_loc = loc_ids['london'] if is_london else loc_ids['gloucester']
                                                    loc_ok = set_variant_location(inv_item_id, target_loc, loc_ids['all_ids'])
                                                    shopify_update_log(f"   {'✅' if loc_ok else '❌'} Inventory location set to {'London' if is_london else 'Gloucester'}")
                                            else:
                                                shopify_update_log(f"   ❌ Add variant failed [{r.status_code}]: {r.text[:200]}")
                                        else:
                                            # Normal path: create new product
                                            product_payload = create_shopify_product_payload(row, loc_prefix, [variant_payload])
                                            r = requests.post(f"https://{shop_url}/admin/api/{version}/products.json", json=product_payload, headers=s_headers)
                                            if r.status_code == 201:
                                                new_prod = r.json().get('product', {})
                                                prod_id = new_prod.get('id')
                                                shopify_update_log(f"   ✅ Product created: {full_sku} (ID: {prod_id})")
                                                created_shopify_products[product_key] = prod_id
                                                variants = new_prod.get('variants', [])
                                                if prod_id and variants:
                                                    variant_id = variants[0].get('id')
                                                    variant_title = variants[0].get('title', '')
                                                    shopify_update_log(f"   📦 Variant: {variant_title} (ID: {variant_id})")
                                                    if variant_id:
                                                        shopify_links.append({"label": f"{fam_name} — {variant_title} ({loc_prefix})", "url": f"https://{shop_url}/admin/products/{prod_id}/variants/{variant_id}"})
                                                if prod_id and pub_ids:
                                                    pub_id = pub_ids['london'] if is_london else pub_ids['gloucester']
                                                    if pub_id:
                                                        pub_ok = publish_product_to_app(prod_id, pub_id)
                                                        shopify_update_log(f"   {'✅' if pub_ok else '❌'} Published to {'London' if is_london else 'Gloucester'} catalogue")
                                                    else:
                                                        shopify_update_log(f"   ⚠️ No publication ID found for {'London' if is_london else 'Gloucester'}")
                                                if loc_ids and variants:
                                                    inv_item_id = variants[0].get('inventory_item_id')
                                                    target_loc = loc_ids['london'] if is_london else loc_ids['gloucester']
                                                    loc_ok = set_variant_location(inv_item_id, target_loc, loc_ids['all_ids'])
                                                    shopify_update_log(f"   {'✅' if loc_ok else '❌'} Inventory location set to {'London' if is_london else 'Gloucester'}")
                                            else:
                                                shopify_update_log(f"   ❌ Create failed [{r.status_code}]: {r.text[:200]}")
                                    except Exception as e:
                                        shopify_update_log(f"   💥 Exception: {str(e)}")
                            st.session_state.shopify_log_text = "\n".join(shopify_log)
                            st.session_state.shopify_links = shopify_links
                            st.rerun()

                if st.session_state.cin7_log_text:
                    st.markdown("**Cin7 Sync Log**")
                    st.code(st.session_state.cin7_log_text, language="text")
                    if st.session_state.cin7_links:
                        st.markdown("**🔗 Cin7 — Created Products**")
                        for item in st.session_state.cin7_links:
                            st.markdown(f"- [{item['label']}]({item['url']})")

                if st.session_state.shopify_log_text:
                    st.markdown("**Shopify Creation Log**")
                    st.code(st.session_state.shopify_log_text, language="text")
                    if st.session_state.shopify_links:
                        st.markdown("**🔗 Shopify — Created Variants**")
                        for item in st.session_state.shopify_links:
                            st.markdown(f"- [{item['label']}]({item['url']})")

    # -------------------------
    # TAB 5: FINALIZE PO
    # -------------------------
    with current_tabs[4]:
        st.subheader("5. Finalize Purchase Order")

        if st.session_state.header_data is not None:
            st.markdown("**Invoice Header**")
            st.dataframe(st.session_state.header_data, use_container_width=True)

        po_lines = prepare_final_po_lines(st.session_state.line_items)

        if po_lines.empty:
            st.warning("No matched lines ready for PO. Run inventory check in Tab 1 first.")
        else:
            st.markdown("**PO Lines**")
            display_po = po_lines.drop(columns=[c for c in ['Format', 'London_SKU', 'Gloucester_SKU'] if c in po_lines.columns], errors='ignore')
            st.dataframe(display_po, use_container_width=True)

            # PolyKeg coupler gate
            polykeg_lines = po_lines[po_lines['Format'].str.lower() == 'polykeg'] if 'Format' in po_lines.columns else pd.DataFrame()
            polykeg_ready = True
            if not polykeg_lines.empty:
                st.warning("⚠️ This PO contains PolyKeg items. Select the coupler type for each before creating the PO.")
                for row_idx, pk_row in polykeg_lines.iterrows():
                    sel = st.selectbox(
                        f"Coupler type — {pk_row['Product']} ({pk_row.get('Variant_Match', '')})",
                        ["— select —", "Sankey Coupler", "KeyKeg Coupler"],
                        key=f"pk_sel_{row_idx}",
                        index=["— select —", "Sankey Coupler", "KeyKeg Coupler"].index(
                            st.session_state.polykeg_selections.get(row_idx, "— select —")
                        )
                    )
                    if sel != "— select —":
                        st.session_state.polykeg_selections[row_idx] = sel
                    elif row_idx in st.session_state.polykeg_selections:
                        del st.session_state.polykeg_selections[row_idx]
                polykeg_ready = all(
                    st.session_state.polykeg_selections.get(i, "— select —") != "— select —"
                    for i in polykeg_lines.index
                )
                if not polykeg_ready:
                    st.error("Please select a coupler type for every PolyKeg line before creating the PO.")

            all_suppliers = st.session_state.cin7_all_suppliers
            supplier_names = [s['Name'] for s in all_suppliers]
            selected_supplier = st.selectbox("Select Supplier for PO", options=supplier_names, index=None)
            location_choice = st.selectbox("Location", ["London", "Gloucester"])

            if selected_supplier:
                supplier_obj = next((s for s in all_suppliers if s['Name'] == selected_supplier), None)
                if supplier_obj:
                    st.session_state.header_data.at[0, 'Cin7_Supplier_ID'] = supplier_obj['ID']
                    st.session_state.header_data.at[0, 'Cin7_Supplier_Name'] = supplier_obj['Name']

            if st.button("📤 Create Purchase Order in Cin7", type="primary", disabled=not polykeg_ready):
                if not selected_supplier:
                    st.error("Please select a supplier.")
                else:
                    # Apply coupler selections: swap Cin7 IDs to the chosen connector's SKU
                    final_po_lines = po_lines.copy()
                    for row_idx, connector in st.session_state.polykeg_selections.items():
                        if row_idx not in final_po_lines.index: continue
                        new_end = "KKT" if connector == "KeyKeg Coupler" else "ST"
                        l_sku = final_po_lines.at[row_idx, 'London_SKU']
                        g_sku = final_po_lines.at[row_idx, 'Gloucester_SKU']
                        new_l = swap_polykeg_sku_end(l_sku, new_end)
                        new_g = swap_polykeg_sku_end(g_sku, new_end)
                        final_po_lines.at[row_idx, 'Cin7_London_ID'] = get_cin7_product_id(new_l)
                        final_po_lines.at[row_idx, 'Cin7_Glou_ID'] = get_cin7_product_id(new_g)
                    with st.spinner("Creating PO..."):
                        success, message, logs, task_id = create_cin7_purchase_order(
                            st.session_state.header_data, final_po_lines, location_choice
                        )
                        if success:
                            st.session_state.po_success = True
                            st.session_state.po_url = f"https://inventory.dearsystems.com/Purchase#{task_id}"
                            st.balloons()
                        else:
                            st.error(message)

            if st.session_state.po_success and st.session_state.po_url:
                st.success("✅ PO Created!")
                st.link_button("📄 View Purchase Order in Cin7", st.session_state.po_url)

    # -------------------------
    # TAB 6: PRICE CHECK
    # -------------------------
    with current_tabs[5]:
        st.subheader("6. Price Check")
        st.caption("Auto-populated from matched lines after inventory check. Review any flagged items before finalising the PO.")

        if st.button("🔄 Rerun Price Check", help="Re-fetch current Cin7 prices and recalculate recommendations"):
            updated_lines = st.session_state.get('line_items', None)
            if updated_lines is not None and not updated_lines.empty:
                st.session_state.price_check_data = build_price_check_from_matched_lines(updated_lines)
                st.rerun()
            else:
                st.warning("No matched lines found — run Check Inventory in Tab 1 first.")

        pc_df = st.session_state.price_check_data

        if pc_df is None or pc_df.empty:
            st.info("Run **Check Inventory** in Tab 1 to auto-populate price check from matched lines.")
        else:
            changed_count = len(pc_df[pc_df['Change_%'] != 0.0])
            if changed_count:
                st.warning(f"⚠️ **{changed_count} SKU(s)** have a price change — review and update as needed.")
            else:
                st.success("✅ All prices match current. No updates needed.")

            if 'Update' not in pc_df.columns:
                pc_df['Update'] = pc_df['Flag'] == "⚠️ Review"
                cols = ['Update'] + [c for c in pc_df.columns if c != 'Update']
                pc_df = pc_df[cols]
                st.session_state.price_check_data = pc_df

            col_cfg = {
                "Update":               st.column_config.CheckboxColumn("Update?", width="small"),
                "Product":              st.column_config.TextColumn("Product"),
                "Variant":              st.column_config.TextColumn("Variant"),
                "ABV":                  st.column_config.TextColumn("ABV"),
                "Invoice_Cost":         st.column_config.NumberColumn("Invoice Cost", format="£%.2f"),
                "Current_Cin7_Price":   st.column_config.NumberColumn("Current Price", format="£%.2f"),
                "Recommended_Price":    st.column_config.NumberColumn("Recommended", format="£%.2f"),
                "Change_%":             st.column_config.NumberColumn("Change %", format="%.1f%%"),
                "Flag":                 st.column_config.TextColumn("Flag", disabled=True),
                "Cin7_ID":              st.column_config.TextColumn("Cin7_ID", disabled=True),
                "Cin7_Name":            None,
                "Attr5":                st.column_config.TextColumn("Attr5", disabled=True),
            }

            edited_pc = st.data_editor(
                pc_df,
                column_config=col_cfg,
                column_order=["Update", "SKU", "Product", "Variant", "ABV", "Invoice_Cost", "Current_Cin7_Price", "Recommended_Price", "Change_%", "Flag"],
                num_rows="fixed",
                use_container_width=True,
                key="price_check_editor"
            )

            st.divider()
            btn_col1, btn_col2 = st.columns(2)

            with btn_col1:
                if st.button("💰 Update Prices in Cin7 & Shopify"):
                    update_log = []
                    prog = st.progress(0)
                    rows_to_update = edited_pc[edited_pc['Update'] == True]
                    for i, (_, row) in enumerate(rows_to_update.iterrows()):
                        prog.progress((i + 1) / max(len(rows_to_update), 1))
                        new_price = row['Recommended_Price']
                        old_price = row.get('Current_Cin7_Price', 0)
                        cin7_name = str(row.get('Cin7_Name', '')).strip()
                        if cin7_name:
                            readable = cin7_name
                        else:
                            abv = str(row.get('ABV', '')).strip()
                            abv_str = f" / {abv}%" if abv and abv.lower() not in ('', 'nan') else ""
                            readable = f"{row['SKU'].split('-')[0]}-{row.get('Product', row['SKU'])}{abv_str} / {row.get('Variant', '')}"
                        update_log.append(f"\n── {readable}  £{old_price:.2f} → £{new_price:.2f}")
                        prod_id = row.get('Cin7_ID')
                        if prod_id:
                            ok, msg = update_cin7_price(prod_id, new_price)
                            update_log.append(f"  {'✅' if ok else '❌'} Cin7:   {msg}")
                        variant_id, _ = fetch_shopify_price_by_sku(row['SKU'])
                        if variant_id:
                            ok, msg = update_shopify_price(variant_id, new_price)
                            update_log.append(f"  {'✅' if ok else '❌'} Shopify: {msg}")
                    st.code("\n".join(update_log), language="text")
                    st.success("Price update complete.")

            with btn_col2:
                if st.button("✏️ Update Product Details in Cin7 & Shopify"):
                    detail_log = []
                    prog2 = st.progress(0)
                    rows_to_update = edited_pc[edited_pc['Update'] == True]
                    orig = st.session_state.price_check_data
                    for i, (idx, row) in enumerate(rows_to_update.iterrows()):
                        prog2.progress((i + 1) / max(len(rows_to_update), 1))
                        sku = row['SKU']
                        orig_row = orig.loc[idx] if idx in orig.index else None
                        old_product = orig_row['Product'] if orig_row is not None else row['Product']
                        old_variant = orig_row['Variant'] if orig_row is not None else row['Variant']
                        old_abv     = orig_row.get('ABV', '') if orig_row is not None else row.get('ABV', '')
                        new_product = row['Product']
                        new_variant = row['Variant']
                        new_abv     = row.get('ABV', '')
                        cin7_name   = row.get('Cin7_Name', '')
                        prod_id     = row.get('Cin7_ID')
                        if prod_id:
                            if not cin7_name:
                                _, _, cin7_name, _, _ = fetch_cin7_product_details_by_sku(sku)
                            ok, msg = update_cin7_product_details(prod_id, cin7_name, old_product, new_product, old_variant, new_variant, old_abv, new_abv)
                            detail_log.append(f"{'✅' if ok else '❌'} Cin7 {sku}: {msg}")
                        ok, msg = update_shopify_product_details(sku, new_product, new_variant, old_abv, new_abv, old_product=old_product)
                        detail_log.append(f"{'✅' if ok else '❌'} Shopify {sku}: {msg}")
                    st.code("\n".join(detail_log), language="text")
                    st.success("Product detail update complete.")

            st.download_button("📥 Download Price Check CSV", edited_pc.to_csv(index=False), "price_check.csv")

