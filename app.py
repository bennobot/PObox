import streamlit as st
import pandas as pd
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
        html, body, [class*="css"]  {
            font-size: 14px;
        }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 0. AUTHENTICATION & HEADER
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
        keys_to_clear = [
            'header_data', 'line_items', 'matrix_data', 'upload_data', 
            'shopify_logs', 'untappd_logs', 'cin7_logs', 'shopify_check_results',
            'selected_drive_id', 'selected_drive_name', 
            'upload_generated', 'cin7_complete', 'cin7_log_text', 'shopify_log_text'
        ]
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
                
        st.session_state.header_data = None
        st.session_state.line_items = None
        st.session_state.matrix_data = None
        st.session_state.upload_data = None
        st.session_state.shopify_logs = []
        st.session_state.untappd_logs = []
        st.session_state.upload_generated = False
        st.session_state.cin7_complete = False
        
        st.session_state.line_items_key += 1
        st.session_state.matrix_key += 1
        
        st.rerun()

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================

# --- 1A. PRICING & GENERAL LOGIC ---

def clean_abv(abv_str):
    """
    Aggressively cleans ABV using Regex.
    Keeps ONLY digits and dots. Removes %, spaces, letters.
    Example: "5.8 % abv" -> "5.8"
    """
    if not abv_str: return ""
    
    s = str(abv_str)
    s_clean = re.sub(r"[^\d.]", "", s)
    
    try:
        val = float(s_clean)
        val = round(val, 1) # Force 1 decimal max
        if val.is_integer():
            return str(int(val)) 
        return str(val) 
    except:
        return ""

def calculate_sell_price(cost_price, product_type, fmt):
    try:
        cost = float(cost_price)
    except:
        return 0.00

    if cost == 0: return 0.00

    fmt_lower = str(fmt).lower()
    draft_triggers = ['keykeg', 'steel', 'poly', 'uni', 'cask', 'keg', 'firkin', 'pin']
    is_draft = any(t in fmt_lower for t in draft_triggers)

    if is_draft and cost > 140: return round(cost + 40, 2)
    if is_draft and cost < 63: return round(cost + 20, 2)
    if product_type == "Core Product": return round(cost * 1.265, 2)
    else: return round(cost * 1.285, 2)

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
def search_untappd_item(supplier, product):
    if "untappd" not in st.secrets: return None
    creds = st.secrets["untappd"]
    base_url = creds.get("base_url", "https://business.untappd.com/api/v1")
    token = creds.get("api_token")
    
    raw_supp = str(supplier).replace("&", " and ")
    raw_prod = str(product).replace("&", " and ")
    clean_supp = re.sub(r'(?i)\b(ltd|limited|llp|plc|brewing|brewery|co\.?)\b', '', raw_supp).strip()
    clean_prod = raw_prod.strip()
    full_string = f"{clean_supp} {clean_prod}"
    parts = full_string.split() 
    query_str = " ".join(parts)
    
    safe_q = quote(query_str)
    url = f"{base_url}/items/search?q={safe_q}"
    headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items:
                best = items[0] 
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
                    "query_used": query_str 
                }
    except: pass
    return {"query_used": query_str}

def batch_untappd_lookup(matrix_df, status_box=None):
    if matrix_df.empty: return matrix_df, ["Matrix Empty"]
    
    cols = ['Untappd_Status', 'Untappd_ID', 'Untappd_Brewery', 'Untappd_Product', 
            'Untappd_ABV', 'Untappd_IBU', 'Untappd_Style', 'Untappd_Desc', 
            'Label_Thumb', 'Brewery_Loc', 'Untappd_Country', 'Match_Check', 'Retry']
    
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
        
        if current_status != "✅ Found" or retry_flag:
            res = search_untappd_item(row['Supplier_Name'], row['Product_Name'])
            
            if res and "untappd_id" in res:
                log_msg(f"✅ Found: {res['name']}")
                row['Untappd_Status'] = "✅ Found"
                row['Untappd_ID'] = res['untappd_id']
                row['Untappd_Brewery'] = res['brewery']
                row['Untappd_Product'] = res['name']
                row['Untappd_ABV'] = res['abv'] 
                row['Untappd_IBU'] = res['ibu']
                row['Untappd_Style'] = res['style']
                row['Untappd_Desc'] = res['description']
                row['Label_Thumb'] = res['label_image_thumb']
                row['Brewery_Loc'] = res['brewery_location']
                row['Untappd_Country'] = res['brewery_country']
                
                clean_res_abv = clean_abv(res['abv'])
                row['Match_Check'] = f"{res['brewery']} / {res['name']} / {clean_res_abv}%"
                
            else:
                used_q = res.get('query_used', 'Unknown') if res else 'Error'
                log_msg(f"❌ No match: {row['Product_Name']} | Query: [{used_q}]")
                
                row['Untappd_Status'] = "❌ Not Found"
                row['Match_Check'] = "No Match Found"
                row['Untappd_ID'] = "" 
                
                row['Untappd_Brewery'] = row.get('Supplier_Name', '')
                row['Untappd_Product'] = row.get('Product_Name', '')
                
                # --- FIX: Use Invoice ABV if available, else Blank ---
                raw_invoice_abv = row.get('ABV', '')
                clean_val = clean_abv(raw_invoice_abv)
                row['Untappd_ABV'] = clean_val
                
                row['Untappd_Style'] = "" 
                row['Untappd_Desc'] = ""
                row['Label_Thumb'] = ""
            
            row['Retry'] = False
        
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

def make_cin7_request(method, url, headers=None, **kwargs):
    if not headers: headers = get_cin7_headers()
    max_retries = 6
    backoff = 1.0 
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            if response.status_code == 429:
                time.sleep(backoff)
                backoff *= 2 
                continue
            return response
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(backoff)
            backoff *= 2
    return response

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
                    if b.get("Name"):
                        all_brands.append(str(b["Name"]))
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

def prepare_final_po_lines(line_items_df):
    if line_items_df is None or line_items_df.empty:
        return pd.DataFrame()
        
    po_rows = []
    
    for _, row in line_items_df.iterrows():
        if row.get('Shopify_Status') != "✅ Match":
            continue
            
        prod_name = row['Product_Name']
        matched_sku = row.get('Matched_Variant', '')
        
        raw_qty = float(row.get('Quantity', 0))
        raw_price = float(row.get('Item_Price', 0))
        
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
            "Product": prod_name,
            "Variant_Match": matched_sku,
            "PO_Qty": final_qty,
            "PO_Cost": final_price,
            "Total": final_qty * final_price,
            "Notes": notes,
            "Cin7_London_ID": l_id,
            "Cin7_Glou_ID": g_id
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

# --- SHOPIFY HELPERS ---

def fetch_shopify_products_by_vendor(vendor):
    if "shopify" not in st.secrets: return []
    if not vendor or not isinstance(vendor, str): return []
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    query = """query ($query: String!, $cursor: String) { products(first: 50, query: $query, after: $cursor) { pageInfo { hasNextPage endCursor } edges { node { id title status format_meta: metafield(namespace: "custom", key: "Format") { value } abv_meta: metafield(namespace: "custom", key: "ABV") { value } keg_meta: metafield(namespace: "custom", key: "Keg_Type") { value } variants(first: 20) { edges { node { id title sku inventoryQuantity } } } } } } }"""
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

# --- SHOPIFY B2B CATALOG / PUBLICATION HELPERS ---

def fetch_publication_ids():
    if "shopify" not in st.secrets: return None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    pub_map = {'london': None, 'gloucester': None}
    
    query_catalogs = """
    {
      catalogs(first: 25) {
        nodes {
          id
          title
          publication {
            id
          }
        }
      }
    }
    """
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
        query_pubs = """
        {
          publications(first: 25) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
        """
        try:
            r = requests.post(endpoint, json={"query": query_pubs}, headers=headers)
            if r.status_code == 200:
                data = r.json()
                if "data" in data and "publications" in data["data"]:
                    for edge in data["data"]["publications"]["edges"]:
                        node = edge['node']
                        name = node['name'].lower()
                        pid = node['id']
                        if "london" in name and not pub_map['london']: 
                            pub_map['london'] = pid
                        if "gloucester" in name and not pub_map['gloucester']: 
                            pub_map['gloucester'] = pid
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
    
    mutation = """
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {"id": product_gid, "input": [{"publicationId": publication_id_gql}]}
    try:
        r = requests.post(endpoint, json={"query": mutation, "variables": variables}, headers=headers)
        if r.status_code == 200:
            data = r.json()
            errors = data.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
            if not errors: return True
    except: pass
    return False

# --- SHOPIFY PAYLOAD HELPERS ---

def get_abv_category(abv_str):
    try:
        val = float(abv_str)
    except:
        return ""
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
    
    raw_abv = str(row.get('untappd_abv', '0')).replace('%', '').strip()
    abv_val = clean_abv(raw_abv)
    try: float(abv_val)
    except ValueError: abv_val = "0"

    abv_cat = get_abv_category(abv_val)
    style_prim, style_sec = split_untappd_style(row.get('untappd_style', ''))
    
    raw_ibu = row.get('untappd_ibu', 0)
    try: ibu_val = float(raw_ibu)
    except (ValueError, TypeError): ibu_val = 0.0
        
    untappd_id = row.get('Untappd_ID', '') or row.get('untappd_id', '')
    is_match = bool(untappd_id)
    ignore_val = "false" if is_match else "true"
    
    filter_val = get_filter_group(row)
    tags_list = [
        loc_name, "Wholesale", vendor, row.get('Type', 'Beer'), row.get('format', ''), 
        style_prim, style_sec, abv_cat, row.get('Attribute_5', 'Rotational Product'), filter_val 
    ]
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
             if "Icon.png" in row['Label_Thumb']:
                 hd_url = row['Label_Thumb'].replace("Icon.png", "HD.png") + "?size=hd"
             else: hd_url = row['Label_Thumb'] 
             add_meta("ut_img_hd", hd_url, "single_line_text_field")

    return {
        "product": {
            "title": full_title, "body_html": body_html, "vendor": vendor, "product_type": prod_type,
            "status": "draft", "tags": tags_str, "variants": variants_list, "images": images, "metafields": metafields
        }
    }

# --- SHOPIFY INVENTORY LOCATION HELPERS ---

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
    except: pass
    return loc_map

def set_variant_location(inventory_item_id, target_location_id, all_location_ids):
    if not inventory_item_id or not target_location_id: return False
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    base_url = f"https://{shop_url}/admin/api/{version}/inventory_levels"

    set_url = f"{base_url}/set.json"
    payload = {"location_id": target_location_id, "inventory_item_id": inventory_item_id, "available": 0}
    try: requests.post(set_url, json=payload, headers=headers)
    except: pass

    del_url = f"{base_url}.json" 
    for loc_id in all_location_ids:
        if loc_id != target_location_id:
            try: requests.delete(del_url, headers=headers, params={"inventory_item_id": inventory_item_id, "location_id": loc_id})
            except: pass
    return True

# --- RECONCILIATION LOGIC ---

def run_reconciliation_check(lines_df):
    if lines_df.empty: return lines_df, ["No Lines to check."]
    logs = []
    df = lines_df.copy()
    
    if 'Use_Split' not in df.columns: df['Use_Split'] = False
    if 'Strict_Search' not in df.columns: df['Strict_Search'] = False

    df['Shopify_Status'] = "Pending"
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
        progress_bar.progress((i)/len(suppliers))
        logs.append(f"🔎 **Fetching Shopify Data:** `{supplier}`")
        products = fetch_shopify_products_by_vendor(supplier)
        shopify_cache[supplier] = products
    progress_bar.progress(1.0)

    results = []
    for _, row in df.iterrows():
        status = "❓ Vendor Not Found"
        london_sku, glou_sku, cin7_l_id, cin7_g_id, img_url = "", "", "", "", ""
        matched_prod_name, matched_var_name = "", ""
        
        supplier = str(row.get('Supplier_Name', ''))
        inv_prod_name = row['Product_Name']
        
        use_split = row.get('Use_Split', False)
        is_strict = row.get('Strict_Search', False)
        match_threshold = 95 if is_strict else 65
        
        raw_pack_str = str(row.get('Pack_Size', '1'))
        pack_nums = re.findall(r'\d+', raw_pack_str)
        if pack_nums: original_pack = float(pack_nums[0])
        else: original_pack = 1.0

        if use_split and original_pack > 1:
            target_pack = int(original_pack / 2)
            logs.append(f"   ✂️ Splitting: Invoice {int(original_pack)} -> Looking for {target_pack}")
        else: target_pack = int(original_pack)
        
        inv_vol = normalize_vol_string(row.get('Volume', ''))
        inv_fmt = str(row.get('Format', '')).lower()
        
        debug_mode = "(Strict)" if is_strict else "(Fuzzy)"
        logs.append(f"Checking: **{inv_prod_name}** {debug_mode} | Target Pack: {target_pack}")

        if supplier in shopify_cache and shopify_cache[supplier]:
            candidates = shopify_cache[supplier]
            scored_candidates = []
            inv_nums = set(re.findall(r'\d+', inv_prod_name))

            for edge in candidates:
                prod = edge['node']
                shop_title_full = prod['title']
                shop_prod_name_clean = shop_title_full
                if "/" in shop_title_full:
                    parts = [p.strip() for p in shop_title_full.split("/")]
                    if len(parts) >= 2: shop_prod_name_clean = parts[1]
                
                score = fuzz.token_sort_ratio(inv_prod_name, shop_prod_name_clean)
                shop_nums = set(re.findall(r'\d+', shop_prod_name_clean))
                if inv_nums != shop_nums: score -= 20 
                if not is_strict:
                    if inv_prod_name.lower() in shop_prod_name_clean.lower(): score += 5
                
                if score > match_threshold: 
                    scored_candidates.append((score, prod, shop_prod_name_clean))
            
            scored_candidates.sort(key=lambda x: x[0], reverse=True)
            match_found = False
            
            for score, prod, clean_name in scored_candidates:
                if score < match_threshold: continue 
                
                shop_keg_meta = str(prod.get('keg_meta', {}).get('value', '')).lower()
                shop_fmt_meta = str(prod.get('format_meta', {}).get('value', '')).lower()
                shop_title_lower = prod['title'].lower()
                combined_shop_tags = f"{shop_keg_meta} {shop_fmt_meta} {shop_title_lower}"
                
                is_compatible = True
                if "keg" in inv_fmt:
                    if "poly" in inv_fmt and ("steel" in combined_shop_tags or "stainless" in combined_shop_tags): is_compatible = False
                    if "steel" in inv_fmt and ("poly" in combined_shop_tags or "dolium" in combined_shop_tags): is_compatible = False
                    if "key" in inv_fmt and "sankey" in combined_shop_tags: is_compatible = False
                if not is_compatible: continue

                for v_edge in prod['variants']['edges']:
                    variant = v_edge['node']
                    v_title = variant['title'].lower()
                    v_sku = str(variant.get('sku', '')).strip()
                    v_tokens = re.findall(r'\d+|[a-z]+', v_title)
                    
                    pack_match = False
                    if target_pack == 1:
                        if "x" in v_tokens and any(t.isdigit() and int(t) > 1 for t in v_tokens): pack_match = False
                        else: pack_match = True
                    else:
                        if str(target_pack) in v_tokens: pack_match = True
                    
                    vol_match = False
                    if inv_vol in v_title: vol_match = True
                    elif inv_vol == "9" and "firkin" in v_title: vol_match = True
                    elif (inv_vol == "4" or inv_vol == "4.5") and "pin" in v_title: vol_match = True
                    elif inv_vol + "l" in v_title or inv_vol + " l" in v_title: vol_match = True
                    
                    if pack_match and vol_match:
                        logs.append(f"   ✅ MATCH: `{variant['title']}` | SKU: `{v_sku}`")
                        status = "✅ Match"
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
        row['Matched_Product'] = matched_prod_name
        row['Matched_Variant'] = matched_var_name
        row['Image'] = img_url
        row['London_SKU'] = london_sku
        row['Cin7_London_ID'] = cin7_l_id
        row['Gloucester_SKU'] = glou_sku
        row['Cin7_Glou_ID'] = cin7_g_id
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
                val_weight = float(row.iloc[2]) if pd.notna(row.iloc[2]) else 0.0
                val_code = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else ""
                weight_dict[key] = val_weight
                size_code_dict[key] = val_code
            return weight_dict, size_code_dict
    except Exception: pass
    return {}, {}

@st.cache_data(ttl=3600)
def fetch_keg_codes():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        df = conn.read(spreadsheet=sheet_url, worksheet="Keg", usecols=[0, 1])
        if not df.empty:
            df = df.dropna()
            return dict(zip(df.iloc[:, 0].astype(str).str.lower(), df.iloc[:, 1].astype(str)))
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
    if 'Supplier_Name' in df.columns:
        df['Supplier_Name'] = df['Supplier_Name'].apply(match_name)
    return df

def clean_product_names(df):
    if df is None or df.empty: return df
    def cleaner(name):
        if not isinstance(name, str): return name
        name = name.replace('|', '')
        name = re.sub(r'\b\d+x\d+cl\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\b\d+g\b', '', name, flags=re.IGNORECASE)
        return ' '.join(name.split())
    if 'Product_Name' in df.columns:
        df['Product_Name'] = df['Product_Name'].apply(cleaner)
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
            'Supplier_Name': name[0], 
            'Type': '', 
            'Collaborator': name[1], 
            'Product_Name': name[2], 
            'ABV': clean_abv_val 
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
            
        matrix_rows.append(row)
        
    matrix_df = pd.DataFrame(matrix_rows)
    
    if 'Untappd_Status' not in matrix_df.columns:
        matrix_df['Untappd_Status'] = "" 

    base_cols = ['Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV', 'Untappd_Status', 'Match_Check', 'Retry']
    format_cols = []
    for i in range(1, 4):
        format_cols.extend([f'Format{i}', f'Pack_Size{i}', f'Volume{i}', f'Item_Price{i}', f'Split_Case{i}'])
    
    existing_format_cols = [c for c in format_cols if c in matrix_df.columns]
    final_cols = base_cols + existing_format_cols
    
    for col in final_cols:
        if col not in matrix_df.columns:
            if "Split_Case" in col or "Retry" in col: matrix_df[col] = False
            else: matrix_df[col] = ""
            
    for col in final_cols:
        if "Split_Case" not in col and "Retry" not in col and "Item_Price" not in col:
            if matrix_df[col].dtype == 'object':
                matrix_df[col] = matrix_df[col].fillna("").astype(str)
                if "ABV" not in col:
                    matrix_df[col] = matrix_df[col].str.replace(r'\.0$', '', regex=True)

    return matrix_df[final_cols]

def generate_sku_parts(product_name):
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', str(product_name)).upper()
    words = clean_name.split()
    if not words: return "XXXX"
    if len(words) >= 4:
        return "".join([w[0] for w in words[:4]])
    elif len(words) >= 2:
        w1 = words[0][:2]
        w2 = words[1][:2]
        return (w1 + w2).ljust(4, 'X')[:4]
    else:
        w1 = words[0][:2]
        return (w1 + "XX").ljust(4, 'X')[:4]

def stage_products_for_upload(matrix_df):
    if matrix_df.empty: return pd.DataFrame(), []
    new_rows = []
    errors = []
    
    fallback_map = fetch_fallback_images()
    required_manual = ['Untappd_ABV', 'Untappd_Style', 'Untappd_Desc']
    
    for idx, row in matrix_df.iterrows():
        brand_name = str(row.get('Untappd_Brewery', '')).strip()
        if not brand_name: brand_name = str(row.get('Supplier_Name', '')).strip()
            
        prod_name = str(row.get('Untappd_Product', '')).strip()
        if not prod_name: prod_name = str(row.get('Product_Name', '')).strip()

        img_url = str(row.get('Label_Thumb', '')).strip()
        if not img_url:
            lookup_key = str(row.get('Supplier_Name', '')).lower().strip()
            if lookup_key in fallback_map: img_url = fallback_map[lookup_key]

        missing_vals = []
        for field in required_manual:
            val = str(row.get(field, '')).strip()
            if not val: missing_vals.append(field)
        
        if missing_vals:
            errors.append(f"Row {idx+1} ({prod_name}): Missing mandatory fields: {', '.join(missing_vals)}. Please fill in Tab 3.")
            continue

        raw_abv = row.get('Untappd_ABV', '')
        clean_abv_val = clean_abv(raw_abv)

        for i in range(1, 4):
            fmt_val = str(row.get(f'Format{i}', '')).strip()
            if fmt_val and fmt_val.lower() not in ['nan', 'none']:
                new_row = {
                    'untappd_brewery': brand_name,
                    'collaborator': row.get('Collaborator', ''),
                    'untappd_product': prod_name,
                    'untappd_abv': clean_abv_val,
                    'untappd_ibu': row.get('Untappd_IBU', 0),
                    'untappd_country': row.get('Untappd_Country', ''),
                    'untappd_style': row.get('Untappd_Style', ''),
                    'description': row.get('Untappd_Desc', ''),
                    'format': fmt_val,
                    'pack_size': row.get(f'Pack_Size{i}', ''),
                    'volume': row.get(f'Volume{i}', ''),
                    'item_price': row.get(f'Item_Price{i}', ''),
                    'is_split_case': row.get(f'Split_Case{i}', False),
                    'Label_Thumb': img_url,
                    'Untappd_ID': row.get('Untappd_ID', ''), 
                    'Brewery_Loc': row.get('Brewery_Loc', ''),
                    'Family_SKU': '',
                    'Variant_SKU': '', 
                    'Family_Name': '',
                    'Variant_Name': '', 
                    'Weight': 0.0,
                    'Keg_Connector': '',
                    'Attribute_5': 'Rotational Product',
                    'Type': row.get('Type', '') 
                }
                new_rows.append(new_row)
    return pd.DataFrame(new_rows), errors

def sync_product_to_cin7(upload_df, status_box=None):
    log = []
    def update_log(message):
        log.append(message)
        if status_box:
            status_box.code("\n".join(log), language="text")

    families = upload_df.groupby('Family_SKU')
    total_families = len(families)
    update_log(f"🚀 Starting Sync for {total_families} Families...")
    
    for i, (fam_sku, group) in enumerate(families):
        first_row = group.iloc[0]
        fam_name = first_row['Family_Name']
        brand = first_row['untappd_brewery']
        update_log(f"\n🔄 Processing Family {i+1}/{total_families}: {fam_sku}")
        
        for loc in ["L", "G"]:
            fam_id, fam_msg = create_cin7_family_node(fam_sku, fam_name, brand, loc)
            update_log(f"   [{loc}] {fam_msg}")
            
            if fam_id:
                for _, row in group.iterrows():
                    var_msg = create_cin7_variant(row, fam_id, fam_sku, fam_name, loc)
                    update_log(f"      -> Variant: {var_msg}")
            else:
                update_log(f"   🛑 HALT: Could not acquire Family ID. Skipping variants for {fam_sku} ({loc}).")
                
    update_log("\n✅ Sync Process Complete.")
    return log

# ==========================================
# 4. RESULTS DISPLAY
# ==========================================

if st.session_state.header_data is not None:
    if custom_rule: st.success("✅ Used Custom Rules")
    st.divider()
    
    df = st.session_state.line_items
    
    # --- SAFETY FIX: Handle Dictionary Corruption ---
    if isinstance(df, dict):
        try:
            df = pd.DataFrame.from_dict(df)
            st.session_state.line_items = df
        except Exception:
            st.error("⚠️ Data Error: Session state corrupted. Please click 'Reset / New Invoice'.")
            st.stop()

    if 'Shopify_Status' in df.columns:
        unmatched_count = len(df[df['Shopify_Status'] != "✅ Match"])
    else: unmatched_count = len(df) 
    all_matched = (unmatched_count == 0) and ('Shopify_Status' in df.columns)

    # --- UPDATED TAB TITLES ---
    tabs = ["📝 1. Line Items", "🔍 2. Prepare Search", "🍺 3. Prepare Upload", "☁️ 4. Product Upload", "🚀 5. Finalize PO"]
    current_tabs = st.tabs(tabs)
    
    # ==========================================
    # TAB 1: LINE ITEMS
    # ==========================================
    with current_tabs[0]:
        st.subheader("1. Review & Edit Lines")
        
        display_df = st.session_state.line_items.copy()
        
        ideal_order = [
            'Use_Split', 'Strict_Search', 'Shopify_Status', 
            'Matched_Product', 'Matched_Variant', 'Image', 
            'Supplier_Name', 'Product_Name', 'ABV', 
            'Format', 'Pack_Size', 'Volume', 'Quantity', 
            'Item_Price', 'Collaborator', 'Shopify_Variant_ID', 
            'London_SKU', 'Gloucester_SKU'
        ]
        
        final_cols = [c for c in ideal_order if c in display_df.columns]
        rem = [c for c in display_df.columns if c not in final_cols]
        display_df = display_df[final_cols + rem]
        
        column_config = {
            "Image": st.column_config.ImageColumn("Img"),
            "Shopify_Status": st.column_config.TextColumn("Status", disabled=True), 
            "Matched_Product": st.column_config.TextColumn("Shopify Match", disabled=True),
            "Matched_Variant": st.column_config.TextColumn("Variant Match", disabled=True),
            "Use_Split": st.column_config.CheckboxColumn("Order Split?", width="small", help="Tick to order half-case (e.g. 12x instead of 24x)"),
            "Strict_Search": st.column_config.CheckboxColumn("Strict?", width="small", help="Tick to force exact name matching")
        }

        key_lines = f"line_editor_{st.session_state.line_items_key}"
        
        edited_lines = st.data_editor(
            display_df, 
            num_rows="dynamic", 
            width='stretch',
            key=key_lines,
            column_config=column_config
        )
        
        if edited_lines is not None:
            st.session_state.line_items = edited_lines

        col1, col2 = st.columns([1, 4])
        with col1:
            if "shopify" in st.secrets:
                if st.button("🛒 Check Inventory"):
                    with st.spinner("Checking..."):
                        updated_lines, logs = run_reconciliation_check(st.session_state.line_items)
                        st.session_state.line_items = updated_lines
                        st.session_state.shopify_logs = logs
                        st.session_state.matrix_data = create_product_matrix(updated_lines)
                        st.session_state.line_items_key += 1
                        st.session_state.matrix_key += 1
                        st.success("Check Complete!")
                        st.rerun()
        with col2:
             st.download_button("📥 Download Lines CSV", st.session_state.line_items.to_csv(index=False), "lines.csv")
        
        if st.session_state.shopify_logs:
            with st.expander("🕵️ Debug Logs", expanded=False):
                st.markdown("\n".join(st.session_state.shopify_logs))

    # ==========================================
    # TAB 2: PREPARE SEARCH
    # ==========================================
    with current_tabs[1]:
        st.subheader("2. Prepare Missing Items for Search")
        
        if all_matched:
            st.success("🎉 All products matched to Shopify! No action needed here.")
        elif st.session_state.matrix_data is not None and not isinstance(st.session_state.matrix_data, dict) and not st.session_state.matrix_data.empty:
            
            search_has_run = False
            if 'Untappd_Status' in st.session_state.matrix_data.columns:
                status_vals = st.session_state.matrix_data['Untappd_Status'].astype(str).unique()
                if any(v.strip() for v in status_vals):
                    search_has_run = True

            if search_has_run:
                st.info("👇 Review matches below. If a match is incorrect, edit the Name/Supplier, tick 'Retry', and click Search again.")
            else:
                st.info("👇 Select the **Product Type** for each item below, then click Search.")
            
            type_options = ["Beer", "Cider", "Spirits", "Softs", "Wine", "Merch", "Dispense", "Snacks", "PoS", "Other", "Free Of Charge PoS"]
            
            prep_config = {
                "Type": st.column_config.SelectboxColumn("Product Type", options=type_options, required=True, width="medium"),
                "Untappd_Status": st.column_config.TextColumn("UT Status", disabled=True, width="small"),
                "Match_Check": st.column_config.TextColumn("Match Details (Verify Here)", disabled=True, width="large"),
                "Retry": st.column_config.CheckboxColumn("Retry?", width="small", help="Tick this and click Search to re-run lookup for this line.")
            }
            
            for i in range(1, 4):
                prep_config[f"Format{i}"] = st.column_config.TextColumn(f"Format {i}", width="small")
                prep_config[f"Pack_Size{i}"] = st.column_config.TextColumn(f"Pack {i}", width="small")
                prep_config[f"Volume{i}"] = st.column_config.TextColumn(f"Vol {i}", width="small")
                prep_config[f"Item_Price{i}"] = st.column_config.NumberColumn(f"Cost {i}", format="£%.2f", width="small")
                prep_config[f"Split_Case{i}"] = st.column_config.CheckboxColumn(f"Split {i}?", width="small")

            if search_has_run:
                base_cols = ['Retry', 'Untappd_Status', 'Match_Check', 'Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']
            else:
                base_cols = ['Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']
            
            ordered_cols = base_cols.copy()
            for i in range(1, 4):
                if f"Format{i}" in st.session_state.matrix_data.columns:
                    ordered_cols.extend([f"Format{i}", f"Pack_Size{i}", f"Volume{i}", f"Item_Price{i}", f"Split_Case{i}"])
            
            display_cols = [c for c in ordered_cols if c in st.session_state.matrix_data.columns]

            for col in display_cols:
                if "Pack_Size" in col or "Volume" in col or "Format" in col:
                    st.session_state.matrix_data[col] = (
                        st.session_state.matrix_data[col]
                        .fillna("")
                        .astype(str)
                        .str.replace(r'\.0$', '', regex=True)
                        .replace("nan", "")
                    )

            key_prep = f"prep_editor_{st.session_state.matrix_key}"
            
            edited_prep = st.data_editor(
                st.session_state.matrix_data[display_cols],
                num_rows="fixed",
                width='stretch',
                column_config=prep_config,
                key=key_prep
            )
            
            if edited_prep is not None:
                for col in edited_prep.columns:
                    st.session_state.matrix_data[col] = edited_prep[col]

            st.divider()
            col_search, col_help = st.columns([1, 2])
            
            with col_help:
                st.markdown("**Search Logs:**")
                log_placeholder = st.empty()
                if st.session_state.untappd_logs:
                    log_text = "\n".join(st.session_state.untappd_logs)
                    log_placeholder.code(log_text, language="text")
                else:
                    log_placeholder.info("Ready to search.")

            with col_search:
                missing_types = st.session_state.matrix_data['Type'].replace('', pd.NA).isna().sum()
                btn_label = "🔎 Search Untappd Details" if not search_has_run else "🔎 Search Again / Retry"
                
                if st.button(btn_label, type="primary"):
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
        else: st.info("Run 'Check Inventory' in Tab 1 first.")

    # ==========================================
    # TAB 3: PREPARE UPLOAD
    # ==========================================
    with current_tabs[2]:
        st.subheader("3. Review matches and add missing product information")
        
        has_untappd_cols = 'Untappd_Status' in st.session_state.matrix_data.columns if st.session_state.matrix_data is not None else False
        
        if not has_untappd_cols:
             st.warning("⚠️ Please run the search in 'Tab 2. Prepare Search' first.")
        
        elif st.session_state.matrix_data is not None and not st.session_state.matrix_data.empty:
            st.info("👇 These details will be used to create products in Cin7. Edit manually if the match is wrong or missing.")
            u_cols = ['Untappd_Status', 'Label_Thumb', 'Untappd_Brewery', 'Untappd_Product', 'Untappd_ABV', 'Untappd_Style', 'Untappd_Desc']
            invoice_cols = ['Supplier_Name', 'Product_Name', 'Format1'] 
            full_view = u_cols + [c for c in invoice_cols if c in st.session_state.matrix_data.columns]
            
            column_config = {
                "Label_Thumb": st.column_config.ImageColumn("Label", width="small"),
                "Untappd_Status": st.column_config.TextColumn("Status", disabled=True),
                "Untappd_Style": st.column_config.SelectboxColumn("Style", options=get_beer_style_list(), width="medium"),
                "Untappd_Desc": st.column_config.TextColumn("Description", width="large"),
                "Untappd_Brewery": st.column_config.TextColumn("Brand (Cin7)", width="medium"),
                "Untappd_Product": st.column_config.TextColumn("Product Name (Cin7)", width="medium"),
            }

            key_match = f"match_editor_{st.session_state.matrix_key}"
            
            edited_matches = st.data_editor(
                st.session_state.matrix_data,
                column_order=full_view,
                num_rows="fixed",
                width='stretch',
                key=key_match,
                column_config=column_config
            )
            
            if edited_matches is not None:
                st.session_state.matrix_data = edited_matches

            st.divider()
            if st.button("✨ Validate & Stage for Upload", type="primary"):
                staged_df, errors = stage_products_for_upload(st.session_state.matrix_data)
                if errors:
                    for e in errors: st.error(e)
                else:
                    st.session_state.upload_data = staged_df
                    st.session_state.upload_generated = False 
                    st.success("Products staged successfully! Go to Tab 4.")

    # ==========================================
    # TAB 4: PRODUCT UPLOAD
    # ==========================================
    with current_tabs[3]:
        st.subheader("4. Product Upload Stage")
        
        if st.session_state.upload_data is not None and not st.session_state.upload_data.empty:

            if st.button("🛠️ Generate Upload Data"):
                supplier_map = fetch_supplier_codes()
                format_map = fetch_format_codes()
                weight_map, size_code_map = fetch_weight_map() 
                keg_map = fetch_keg_codes() 
                today_str = datetime.now().strftime('%d%m%Y')
                processed_rows = []
                
                for idx, row in st.session_state.upload_data.iterrows():
                    supp_name = row.get('untappd_brewery', '')
                    prod_name = row.get('untappd_product', '')
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
                    family_name = f"{supp_name} / {prod_name} / {abv_val}% / {fmt_name}"

                    connectors = [""]
                    fmt_lower = fmt_name.lower()
                    if "dolium" in fmt_lower and "us" in fmt_lower: connectors = ["US Sankey D-Type Coupler"]
                    elif "poly" in fmt_lower: connectors = ["Sankey Coupler", "KeyKeg Coupler"]
                    elif "key" in fmt_lower: connectors = ["KeyKeg Coupler"]
                    elif "steel" in fmt_lower: connectors = ["Sankey Coupler"]
                    
                    variants_config = []
                    
                    raw_pack = row.get('pack_size', '1')
                    try: 
                        orig_pack = float(raw_pack) if raw_pack and str(raw_pack).lower() != 'nan' else 1.0
                    except: 
                        orig_pack = 1.0
                    
                    raw_price = row.get('item_price', 0)
                    try: orig_price = float(raw_price)
                    except: orig_price = 0.0
                    
                    variants_config.append({'pack': orig_pack, 'price': orig_price})
                    
                    if row.get('is_split_case', False) and orig_pack > 1:
                        split_pack = orig_pack / 2
                        split_price = orig_price / 2
                        variants_config.append({'pack': split_pack, 'price': split_price})

                    for v_conf in variants_config:
                        curr_pack = v_conf['pack']
                        curr_price = v_conf['price']
                        
                        pack_int = int(curr_pack)
                        is_multipack = curr_pack > 1.0
                        total_weight = unit_weight * curr_pack
                        
                        sell_price = calculate_sell_price(curr_price, attr_5, fmt_name)

                        for conn in connectors:
                            new_row = row.to_dict()
                            
                            new_row['Family_SKU'] = family_sku
                            new_row['Family_Name'] = family_name
                            new_row['Weight'] = total_weight
                            new_row['Keg_Connector'] = conn
                            new_row['Attribute_5'] = attr_5
                            new_row['Type'] = prod_type 
                            
                            new_row['item_price'] = curr_price
                            new_row['Sales_Price'] = sell_price
                            new_row['pack_size'] = curr_pack 
                            
                            var_name_base = vol_name
                            if is_multipack: var_name_base = f"{pack_int}x{vol_name}"
                            
                            if conn: new_row['Variant_Name'] = f"{var_name_base} - {conn}"
                            else: new_row['Variant_Name'] = var_name_base
                            
                            if is_multipack: sku_suffix = f"-{pack_int}X{size_code}"
                            else: sku_suffix = f"-{size_code}"
                            
                            if conn:
                                conn_code = keg_map.get(conn.lower(), "XX")
                                sku_suffix += f"-{conn_code}"
                            
                            new_row['Variant_SKU'] = f"{family_sku}{sku_suffix}"
                            
                            processed_rows.append(new_row)

                final_df = pd.DataFrame(processed_rows)
                
                all_cols = final_df.columns.tolist()
                desired_order = ['Attribute_5', 'Type', 'Sales_Price', 'item_price', 'Variant_Name', 'Variant_SKU', 'Family_Name']
                final_order = []
                for c in desired_order:
                    if c in all_cols: final_order.append(c)
                for c in all_cols:
                    if c not in final_order: final_order.append(c)
                
                st.session_state.upload_data = final_df[final_order]
                st.session_state.upload_generated = True 
                
                st.session_state.cin7_complete = False
                st.session_state.cin7_log_text = ""
                st.session_state.shopify_log_text = ""
                
                st.rerun()

            # --- VALIDATION & EDITOR ---
            missing_types = 0
            if 'Type' in st.session_state.upload_data.columns:
                missing_types = st.session_state.upload_data['Type'].replace('', pd.NA).isna().sum()

            if missing_types > 0:
                st.error(f"🛑 STOP: {missing_types} rows are missing a Product Type. Please select a Type in the table below.")

            upload_col_config = {
                "Attribute_5": st.column_config.SelectboxColumn("Core/Rotation", options=["Rotational Product", "Core Product"], required=True, width="medium"),
                "Type": st.column_config.SelectboxColumn("Product Type", options=["Beer", "Cider", "Spirits", "Softs", "Wine", "Merch", "Dispense", "Snacks", "PoS", "Other"], required=True, width="medium"),
                "Sales_Price": st.column_config.NumberColumn("Sales Price", format="£%.2f", disabled=True)
            }
            
            current_cols = st.session_state.upload_data.columns.tolist()
            disp_order = ['Attribute_5', 'Type', 'Sales_Price', 'item_price', 'Variant_Name', 'Variant_SKU', 'Family_Name']
            final_disp = []
            for c in disp_order:
                if c in current_cols: final_disp.append(c)
            for c in current_cols:
                if c not in final_disp: final_disp.append(c)

            key_upload = "upload_editor_final"
            
            edited_upload = st.data_editor(
                st.session_state.upload_data,
                width=2000,
                column_config=upload_col_config,
                column_order=final_disp, 
                key=key_upload
            )
            if edited_upload is not None:
                st.session_state.upload_data = edited_upload

            # --- SEQUENTIAL UPLOAD LAYOUT ---
            st.divider()
            st.markdown("### 🚀 Step 2: Execute Uploads")
            st.caption("These must be run in order. Cin7 acts as the master record.")
            
            col_cin7, col_shopify = st.columns(2)
            base_disabled = not st.session_state.upload_generated or missing_types > 0

            # --- LEFT: CIN7 (1st) ---
            with col_cin7:
                st.markdown("#### 1️⃣ Cin7 Upload")
                if st.button("🚀 Upload To Cin7", disabled=base_disabled, use_container_width=True):
                    if "cin7" in st.secrets:
                        unique_rows = st.session_state.upload_data.copy()
                        log_container = st.empty()
                        full_log = sync_product_to_cin7(unique_rows, status_box=log_container)
                        st.session_state.cin7_complete = True
                        st.session_state.cin7_log_text = "\n".join(full_log)
                        st.success("Cin7 Sync Complete! You can now proceed to Shopify.")
                        st.rerun()
                    else: st.error("Cin7 Secrets Missing")

                if st.session_state.cin7_log_text:
                    with st.expander("✅ Cin7 Log (Completed)", expanded=False):
                        st.code(st.session_state.cin7_log_text, language="text")
                elif base_disabled:
                    st.info("Waiting for data generation...")
                else:
                    st.info("Ready to upload.")

            # --- RIGHT: SHOPIFY (2nd) ---
            with col_shopify:
                st.markdown("#### 2️⃣ Shopify Upload")
                shop_disabled = base_disabled or not st.session_state.cin7_complete
                
                if st.button("🚀 Upload to Shopify (L & G)", disabled=shop_disabled, use_container_width=True):
                    if "shopify" not in st.secrets:
                        st.error("Shopify secrets missing.")
                        st.stop()
                    
                    creds = st.secrets["shopify"]
                    shop_url = creds.get("shop_url")
                    token = creds.get("access_token")
                    version = creds.get("api_version", "2023-04") 
                    base_url = f"https://{shop_url}/admin/api/{version}"
                    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
                    
                    status_ph = st.empty()
                    status_ph.info("Fetching Configuration...")
                    
                    loc_data = fetch_shopify_location_ids()
                    if not loc_data or not loc_data['london'] or not loc_data['gloucester']:
                        status_ph.error("❌ Location Error.")
                        st.stop()

                    pub_data = fetch_publication_ids()
                    if not pub_data or not pub_data['london'] or not pub_data['gloucester']:
                        status_ph.warning("⚠️ Catalogs not found. Publishing skipped.")
                    else:
                        status_ph.success("✅ Configuration Loaded")
                        
                    time.sleep(1)
                    status_ph.empty() 

                    log_container = st.empty()
                    logs = []
                    def log_s(msg):
                        logs.append(msg)
                        log_container.code("\n".join(logs))

                    grouped = st.session_state.upload_data.groupby('Family_Name')
                    total_groups = len(grouped)
                    
                    for i, (fam_name, group) in enumerate(grouped):
                        log_s(f"**Processing ({i+1}/{total_groups}): {fam_name}**")
                        
                        for loc_prefix in ["L", "G"]:
                            full_title = f"{loc_prefix}-{fam_name}"
                            pid, existing_vid = check_shopify_title(full_title)
                            
                            target_loc_id = loc_data['london'] if loc_prefix == "L" else loc_data['gloucester']
                            target_pub_id = pub_data['london'] if loc_prefix == "L" else pub_data['gloucester']
                            
                            if pid:
                                log_s(f"   🔹 {loc_prefix}: Found ID {pid}. Checking variants...")
                                if target_pub_id:
                                    published = publish_product_to_app(pid, target_pub_id)
                                    if published: log_s(f"      📖 Verified in Catalog")

                                for _, row in group.iterrows():
                                    var_payload = {"variant": create_shopify_variant_payload(row, loc_prefix)}
                                    url = f"{base_url}/products/{pid}/variants.json"
                                    try:
                                        r = requests.post(url, json=var_payload, headers=headers)
                                        if r.status_code in [200, 201]:
                                            v_data = r.json().get('variant', {})
                                            inv_item_id = v_data.get('inventory_item_id')
                                            var_title = row['Variant_Name']
                                            if inv_item_id:
                                                set_variant_location(inv_item_id, target_loc_id, loc_data['all_ids'])
                                                log_s(f"      ✅ Added Variant & Set Loc: {var_title}")
                                            else:
                                                log_s(f"      ⚠️ Added {var_title} but missed inventory ID.")
                                        elif r.status_code == 422 and "already exists" in r.text:
                                             log_s(f"      ⚠️ Variant Exists: {row['Variant_Name']}")
                                        else:
                                            log_s(f"      ❌ Variant Error: {r.text}")
                                    except Exception as e:
                                        log_s(f"      💥 Exception: {e}")     
                            else:
                                log_s(f"   🆕 {loc_prefix}: Creating New Product...")
                                variants_list = []
                                for _, row in group.iterrows():
                                    v_data = create_shopify_variant_payload(row, loc_prefix)
                                    variants_list.append(v_data)
                                
                                first_row = group.iloc[0]
                                prod_payload = create_shopify_product_payload(first_row, loc_prefix, variants_list)
                                
                                url = f"{base_url}/products.json"
                                try:
                                    r = requests.post(url, json=prod_payload, headers=headers)
                                    if r.status_code in [200, 201]:
                                        p_resp = r.json()['product']
                                        new_id = p_resp['id']
                                        log_s(f"      ✅ Created Product! ID: {new_id}")
                                        
                                        created_variants = p_resp.get('variants', [])
                                        for cv in created_variants:
                                            inv_id = cv.get('inventory_item_id')
                                            if inv_id:
                                                set_variant_location(inv_id, target_loc_id, loc_data['all_ids'])
                                        log_s(f"      📍 Location set to {'London' if loc_prefix=='L' else 'Gloucester'}")
                                        
                                        if target_pub_id:
                                            published = publish_product_to_app(new_id, target_pub_id)
                                            if published: log_s(f"      📖 Published to Catalog")
                                            else: log_s(f"      ⚠️ Catalog Publish Failed")
                                    else:
                                        log_s(f"      ❌ Create Error: {r.text}")
                                        log_s(json.dumps(prod_payload))
                                except Exception as e:
                                    log_s(f"      💥 Exception: {e}")
                            time.sleep(0.5)
                    
                    st.session_state.shopify_log_text = "\n".join(logs)
                    st.success("Shopify Process Complete!")
                    st.rerun()

                if st.session_state.shopify_log_text:
                    with st.expander("✅ Shopify Log (Completed)", expanded=False):
                        st.code(st.session_state.shopify_log_text, language="text")
                elif shop_disabled:
                    if not st.session_state.cin7_complete:
                        st.warning("⏳ Waiting for Cin7 Upload to complete...")
                    else:
                        st.info("Waiting for data generation...")

    # ==========================================
    # TAB 5: FINALIZE PO
    # ==========================================
    with current_tabs[4]:
        st.subheader("5. Finalize & Export")
        
        if not all_matched:
            st.error("🔒 **Tab Locked**")
            st.warning(f"You have **{unmatched_count} unmatched items**. Please resolve them in **Tab 1** (Check Inventory) or **Tab 2** (Search Untappd) before finalizing the Purchase Order.")
        else:
            st.markdown("#### A. Header Details")
            current_payee = "Unknown"
            if not st.session_state.header_data.empty:
                 current_payee = st.session_state.header_data.iloc[0]['Payable_To']
            
            cin7_list_names = [s['Name'] for s in st.session_state.cin7_all_suppliers]
            default_index = 0
            if cin7_list_names and current_payee:
                match, score = process.extractOne(current_payee, cin7_list_names)
                if score > 60:
                    try: default_index = cin7_list_names.index(match)
                    except ValueError: default_index = 0

            col_h1, col_h2 = st.columns([1, 2])
            with col_h1:
                selected_supplier = st.selectbox(
                    "Cin7 Supplier Link:", 
                    options=cin7_list_names,
                    index=default_index,
                    key="header_supplier_select"
                )
                
                if selected_supplier and not st.session_state.header_data.empty:
                    supp_data = next((s for s in st.session_state.cin7_all_suppliers if s['Name'] == selected_supplier), None)
                    if supp_data:
                        st.session_state.header_data.at[0, 'Cin7_Supplier_ID'] = supp_data['ID']
                        st.session_state.header_data.at[0, 'Cin7_Supplier_Name'] = supp_data['Name']
            
            with col_h2:
                st.write("") 
                if not st.session_state.header_data.empty:
                    st.caption(f"ID: {st.session_state.header_data.iloc[0].get('Cin7_Supplier_ID', 'N/A')}")

            edited_header = st.data_editor(
                st.session_state.header_data, 
                num_rows="fixed", 
                width='stretch'
            )
            
            st.divider()

            st.markdown("#### B. PO Line Preview (Calculated)")
            st.caption("Review the final quantities and costs below. Split cases have been calculated.")

            po_preview_df = prepare_final_po_lines(st.session_state.line_items)

            if not po_preview_df.empty:
                po_col_config = {
                    "PO_Qty": st.column_config.NumberColumn("Final Qty", format="%.2f"),
                    "PO_Cost": st.column_config.NumberColumn("Final Cost", format="£%.2f"),
                    "Total": st.column_config.NumberColumn("Line Total", format="£%.2f", disabled=True),
                    "Notes": st.column_config.TextColumn("Notes", disabled=True)
                }
                
                st.dataframe(
                    po_preview_df, 
                    use_container_width=True, 
                    column_config=po_col_config,
                    hide_index=True
                )
            else:
                st.warning("No matched lines to display.")

            st.divider()
            
            st.markdown("#### C. Export")
            po_location = st.selectbox("Select Delivery Location:", ["London", "Gloucester"], key="final_po_loc")
            
            if st.button(f"📤 Export PO to Cin7 ({po_location})", type="primary", disabled=po_preview_df.empty):
                if "cin7" in st.secrets:
                    with st.spinner("Creating Purchase Order..."):
                        success, msg, logs = create_cin7_purchase_order(
                            st.session_state.header_data, 
                            po_preview_df, 
                            po_location
                        )
                        st.session_state.cin7_logs = logs
                        
                        if success:
                            task_id = None
                            match = re.search(r'ID: ([a-f0-9\-]+)', msg)
                            if match: task_id = match.group(1)
                            
                            st.success(msg)
                            if task_id:
                                st.link_button("🔗 Open PO in Cin7", f"https://inventory.dearsystems.com/PurchaseAdvanced#{task_id}")
                            st.balloons()
                        else:
                            st.error(msg)
                            with st.expander("Error Details"):
                                for log in logs: st.write(log)
                else:
                    st.error("Cin7 Secrets missing.")
