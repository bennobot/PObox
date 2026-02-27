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
    # Use HTML to push the button down exactly 30px
    st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
    
    if st.button("🔄 Reset / New Invoice"):
        # List of keys to CLEAR
        keys_to_clear = [
            'header_data', 'line_items', 'matrix_data', 'upload_data', 
            'shopify_logs', 'untappd_logs', 'cin7_logs', 'shopify_check_results',
            'selected_drive_id', 'selected_drive_name', 
            'upload_generated'
        ]
        
        # We explicitly DO NOT clear 'drive_files' so the folder scan remains
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
                
        # Re-initialize essentials
        st.session_state.header_data = None
        st.session_state.line_items = None
        st.session_state.matrix_data = None
        st.session_state.upload_data = None
        st.session_state.shopify_logs = []
        st.session_state.untappd_logs = []
        st.session_state.upload_generated = False
        
        # Increment widget keys to force UI refresh
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
    
    # 1. Force string
    s = str(abv_str)
    
    # 2. Regex: Replace anything that is NOT a digit or a dot with empty string
    # This handles "5.8%", "5.8 %", "approx 5.8", etc.
    s_clean = re.sub(r"[^\d.]", "", s)
    
    # 3. Format logic
    try:
        val = float(s_clean)
        val = round(val, 1) # Force 1 decimal max
        if val.is_integer():
            return str(int(val)) # 4.0 -> "4"
        return str(val) # 4.5 -> "4.5"
    except:
        return "" # Return empty if no number found

def calculate_sell_price(cost_price, product_type, fmt):
    # ... (Keep existing pricing logic here) ...
    try:
        cost = float(cost_price)
    except:
        return 0.00

    if cost == 0: return 0.00

    fmt_lower = str(fmt).lower()
    draft_triggers = ['keykeg', 'steel', 'poly', 'uni', 'cask', 'keg', 'firkin', 'pin']
    is_draft = any(t in fmt_lower for t in draft_triggers)

    if is_draft and cost > 140:
        return round(cost + 40, 2)

    if is_draft and cost < 63:
        return round(cost + 20, 2)

    if product_type == "Core Product":
        return round(cost * 1.265, 2)
    else: 
        return round(cost * 1.285, 2)

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
                    "ibu": best.get("ibu", 0),  # --- NEW ---
                    "style": best.get("style"), 
                    "description": best.get("description"),
                    "label_image_thumb": best.get("label_image_thumb"),
                    "brewery_location": best.get("brewery_location"),
                    "brewery_country": best.get("country", "") or best.get("brewery_country", ""), # --- NEW ---
                    "query_used": query_str 
                }
    except: pass
    return {"query_used": query_str}

def batch_untappd_lookup(matrix_df):
    if matrix_df.empty: return matrix_df, ["Matrix Empty"]
    
    # Add 'Match_Check' and 'Retry' to columns list
    cols = ['Untappd_Status', 'Untappd_ID', 'Untappd_Brewery', 'Untappd_Product', 
            'Untappd_ABV', 'Untappd_IBU', 'Untappd_Style', 'Untappd_Desc', 
            'Label_Thumb', 'Brewery_Loc', 'Untappd_Country', 'Match_Check', 'Retry']
    
    for c in cols:
        if c not in matrix_df.columns: matrix_df[c] = ""
            
    updated_rows = []
    logs = []
    prog_bar = st.progress(0)
    
    for idx, row in matrix_df.iterrows():
        prog_bar.progress((idx + 1) / len(matrix_df))
        
        current_status = str(row.get('Untappd_Status', ''))
        retry_flag = row.get('Retry', False)
        
        # Search if not found OR if user requested retry
        if current_status != "✅ Found" or retry_flag:
            res = search_untappd_item(row['Supplier_Name'], row['Product_Name'])
            
            if res and "untappd_id" in res:
                # --- MATCH FOUND ---
                logs.append(f"✅ Found: {res['name']}")
                row['Untappd_Status'] = "✅ Found"
                row['Untappd_ID'] = res['untappd_id']
                row['Untappd_Brewery'] = res['brewery']
                row['Untappd_Product'] = res['name']
                row['Untappd_ABV'] = res['abv'] # Use Untappd ABV
                row['Untappd_IBU'] = res['ibu']
                row['Untappd_Style'] = res['style']
                row['Untappd_Desc'] = res['description']
                row['Label_Thumb'] = res['label_image_thumb']
                row['Brewery_Loc'] = res['brewery_location']
                row['Untappd_Country'] = res['brewery_country']
                
                # Create Readable Match Summary
                clean_res_abv = clean_abv(res['abv'])
                row['Match_Check'] = f"{res['brewery']} / {res['name']} / {clean_res_abv}%"
                
            else:
                # --- NO MATCH ---
                used_q = res.get('query_used', 'Unknown') if res else 'Error'
                logs.append(f"❌ No match: {row['Product_Name']} | Query Sent: [{used_q}]")
                
                row['Untappd_Status'] = "❌ Not Found"
                row['Match_Check'] = "No Match Found"
                row['Untappd_ID'] = "" 
                
                # Pre-fill with Invoice Data
                row['Untappd_Brewery'] = row.get('Supplier_Name', '')
                row['Untappd_Product'] = row.get('Product_Name', '')
                
                # --- FIX: Use Invoice ABV if available, else Blank ---
                raw_invoice_abv = row.get('ABV', '')
                # clean_abv returns "" if input is empty, or "4.5" if input is "4.5%"
                row['Untappd_ABV'] = clean_abv(raw_invoice_abv)
                
                row['Untappd_Style'] = "" 
                row['Untappd_Desc'] = ""
                row['Label_Thumb'] = ""
            
            # Reset Retry Flag
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
    """
    Transforms the raw invoice lines into the final PO structure.
    - Filters for Matched items only.
    - Applies Split Case logic (Qty x2, Price /2).
    """
    if line_items_df is None or line_items_df.empty:
        return pd.DataFrame()
        
    po_rows = []
    
    for _, row in line_items_df.iterrows():
        # 1. Only process matched items
        if row.get('Shopify_Status') != "✅ Match":
            continue
            
        # 2. Base Data
        prod_name = row['Product_Name']
        matched_sku = row.get('Matched_Variant', '') # Visual reference
        
        # 3. Calculate PO Values
        raw_qty = float(row.get('Quantity', 0))
        raw_price = float(row.get('Item_Price', 0))
        
        if row.get('Use_Split', False):
            # --- SPLIT LOGIC APPLIED HERE ---
            final_qty = raw_qty * 2
            final_price = raw_price / 2
            notes = "⚠️ Split Case (Half Size)"
        else:
            final_qty = raw_qty
            final_price = raw_price
            notes = ""

        # 4. IDs for Cin7
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
    """
    Fetches default brand images from MasterData Google Sheet.
    Column A: Brand Name
    Column E: Image URL
    """
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        sheet_url = "https://docs.google.com/spreadsheets/d/1Skd85vSu3e16z9iAVG8bZjhwqIWRnUxZXiVv1QbmPHA"
        # Read columns A (0) and E (4)
        df = conn.read(spreadsheet=sheet_url, worksheet="MasterData", usecols=[0, 4])
        if not df.empty:
            df = df.dropna()
            # return dict {BrandName: ImageURL}
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
    """
    Fetches Publication IDs for 'London' and 'Gloucester'.
    Checks both B2B Catalogs and Standard Publications (Sales Channels).
    """
    if "shopify" not in st.secrets: return None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    # Map to store findings
    pub_map = {'london': None, 'gloucester': None}
    
    # 1. Try B2B Catalogs Query (Preferred)
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

    # 2. If missing, try Standard Publications (Sales Channels)
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
                        # Only fill if empty (Catalogs take priority)
                        if "london" in name and not pub_map['london']: 
                            pub_map['london'] = pid
                        if "gloucester" in name and not pub_map['gloucester']: 
                            pub_map['gloucester'] = pid
        except: pass

    return pub_map

def publish_product_to_app(product_id_numeric, publication_id_gql):
    """
    Publishes a Product (numeric ID) to a specific Publication (GraphQL ID).
    """
    if not product_id_numeric or not publication_id_gql: return False
    
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    endpoint = f"https://{shop_url}/admin/api/{version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    # Convert numeric ID to GID
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
    
    variables = {
        "id": product_gid,
        "input": [{
            "publicationId": publication_id_gql
        }]
    }
    
    try:
        r = requests.post(endpoint, json={"query": mutation, "variables": variables}, headers=headers)
        if r.status_code == 200:
            data = r.json()
            errors = data.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
            if not errors:
                return True
    except: pass
    return False

# --- SHOPIFY PAYLOAD HELPERS (STRICT VALIDATION) ---

def clean_abv(abv_str):
    """
    Formats ABV:
    - 4.0 -> "4"
    - 4.5 -> "4.5"
    - 4.52 -> "4.5" (Rounds to 1 decimal)
    """
    try:
        if not abv_str: return "0"
        val = float(abv_str)
        val = round(val, 1) # Force 1 decimal max
        if val.is_integer():
            return str(int(val)) # 4.0 -> "4"
        return str(val) # 4.5 -> "4.5"
    except:
        return str(abv_str)

def get_abv_category(abv_str):
    """
    Generates ABV Category strings based on specific custom ranges.
    """
    try:
        val = float(abv_str)
    except:
        return ""

    if val <= 3.0:
        return "0% - 3%"
    elif val <= 4.5:
        # User defined: Above 3, less than 4.6
        return "3% - 4.5%"
    elif val <= 6.5:
        # User defined: Above 4.5, less than 6.6
        return "4.6% - 6.5%"
    elif val < 10.0:
        # User defined: Above 6.5, less than 10
        return "6.6% - 9.9%"
    else:
        # User defined: Above 9.9 (so 10 and up)
        return "Over 10%"

def split_untappd_style(full_style):
    if not full_style: return "", ""
    parts = str(full_style).split("-", 1)
    primary = parts[0].strip()
    secondary = parts[1].strip() if len(parts) > 1 else ""
    return primary, secondary

def get_filter_group(row):
    """
    STRICTLY MATCHES SHOPIFY ALLOWED VALUES.
    If it doesn't match the list, return None to avoid API errors.
    """
    valid_options = [
        "6 Packs", "12 Packs", "24 Packs", 
        "KeyKeg Coupler", "Sankey Coupler", "US Sankey D-Type Coupler"
    ]
    
    # 1. Try Connector First (Must match strict casing)
    connector = str(row.get('Keg_Connector', '')).strip()
    if connector in valid_options:
        return connector

    # 2. Try Pack Size
    try:
        pack = int(float(row.get('pack_size', 0)))
    except:
        pack = 0

    pack_str = f"{pack} Packs"
    if pack_str in valid_options:
        return pack_str

    # 3. Fail safe: Return None if not in strict list
    return None 

def create_shopify_variant_payload(row, location_prefix):
    is_london = location_prefix == "L"
    prefix = "L-" if is_london else "G-"
    sku = f"{prefix}{row['Variant_SKU']}"
    price = str(row['Sales_Price']) 
    title = row['Variant_Name']
    weight = float(row.get('Weight', 0))
    
    # Validation Logic
    filter_val = get_filter_group(row)
    
    metafields = [
        {"key": "split_case", "value": "false", "type": "boolean", "namespace": "custom"}
    ]
    
    # Only append if valid
    if filter_val:
        metafields.append({
            "key": "filter_group", 
            "value": filter_val, 
            "type": "single_line_text_field", 
            "namespace": "custom"
        })
    
    return {
        "sku": sku,
        "price": price,
        "title": title,
        "weight": weight,
        "weight_unit": "kg",
        "option1": title, 
        "inventory_management": "shopify", 
        "fulfillment_service": "manual",
        "inventory_policy": "deny",
        "metafields": metafields
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
    
    # 1. Clean ABV
    raw_abv = str(row.get('untappd_abv', '0')).replace('%', '').strip()
    abv_val = clean_abv(raw_abv)
    
    try:
        float(abv_val)
    except ValueError:
        abv_val = "0"

    abv_cat = get_abv_category(abv_val)
    style_prim, style_sec = split_untappd_style(row.get('untappd_style', ''))
    
    # 2. Safe IBU
    raw_ibu = row.get('untappd_ibu', 0)
    try:
        ibu_val = float(raw_ibu)
    except (ValueError, TypeError):
        ibu_val = 0.0
        
    untappd_id = row.get('Untappd_ID', '') or row.get('untappd_id', '')
    
    # --- DETERMINE IGNORE STATUS ---
    # If ID exists -> It is a match -> Ignore = false
    # If ID missing -> Manual/Fallback -> Ignore = true
    is_match = bool(untappd_id)
    ignore_val = "false" if is_match else "true"
    
    filter_val = get_filter_group(row)
    
    tags_list = [
        loc_name, "Wholesale", vendor, 
        row.get('Type', 'Beer'), 
        row.get('format', ''), 
        style_prim, style_sec, 
        abv_cat, 
        row.get('Attribute_5', 'Rotational Product'),
        filter_val 
    ]
    tags_str = ",".join([str(t) for t in tags_list if t])

    # --- MAIN PRODUCT IMAGES (Keep Fallbacks here) ---
    images = []
    if row.get('Label_Thumb'):
        img_url = row['Label_Thumb']
        if "Icon.png" in img_url:
            img_url = img_url.replace("Icon.png", "HD.png") + "?size=hd"
        images.append({"src": img_url})

    # --- METAFIELDS BUILDER ---
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

    # --- CONDITIONAL METAFIELDS (Match Only) ---
    if is_match:
        # Only add IBU if it's a match (otherwise leave blank)
        add_meta("ut_ibu", ibu_val, "number_decimal")
        
        # Only add ID/Link if matched
        add_meta("ut_id", untappd_id, "number_integer")
        add_meta("ut_link", f"https://untappd.com/beer/{untappd_id}", "single_line_text_field")

        # Only add UT Image Metafields if matched
        # (We still added the image to the main gallery above, but we don't flag it as UT-sourced in metafields)
        if row.get('Label_Thumb'):
             add_meta("ut_img_small", row['Label_Thumb'], "single_line_text_field")
             
             if "Icon.png" in row['Label_Thumb']:
                 hd_url = row['Label_Thumb'].replace("Icon.png", "HD.png") + "?size=hd"
             else:
                 hd_url = row['Label_Thumb'] 
                 
             add_meta("ut_img_hd", hd_url, "single_line_text_field")

    return {
        "product": {
            "title": full_title,
            "body_html": body_html,
            "vendor": vendor,
            "product_type": prod_type,
            "status": "draft",
            "tags": tags_str,
            "variants": variants_list,
            "images": images,
            "metafields": metafields
        }
    }

# --- SHOPIFY INVENTORY LOCATION HELPERS ---

def fetch_shopify_location_ids():
    """
    Fetches Location IDs. 
    Priority: 1. secrets.toml, 2. API Name Match ('london', 'gloucester')
    """
    if "shopify" not in st.secrets: return None
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    
    url = f"https://{shop_url}/admin/api/{version}/locations.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    # 1. Start with IDs from secrets if they exist
    loc_map = {
        'london': creds.get('location_id_london'), 
        'gloucester': creds.get('location_id_gloucester'), 
        'all_ids': []
    }
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            locations = r.json().get('locations', [])
            for loc in locations:
                lid = loc['id']
                lname = loc['name'].lower()
                loc_map['all_ids'].append(lid)
                
                # 2. If not in secrets, try to auto-detect by name
                if not loc_map['london'] and "london" in lname:
                    loc_map['london'] = lid
                if not loc_map['gloucester'] and "gloucester" in lname:
                    loc_map['gloucester'] = lid
        else:
            st.error(f"⚠️ Shopify Location API Error [{r.status_code}]: {r.text}")
            
    except Exception as e: 
        st.error(f"⚠️ Location Fetch Exception: {e}")
    
    return loc_map

def set_variant_location(inventory_item_id, target_location_id, all_location_ids):
    """
    Ensures the item is ONLY at the target location.
    1. Connects to Target.
    2. Disconnects from all others.
    """
    if not inventory_item_id or not target_location_id: return False
    
    creds = st.secrets["shopify"]
    shop_url = creds.get("shop_url")
    token = creds.get("access_token")
    version = creds.get("api_version", "2024-04")
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    base_url = f"https://{shop_url}/admin/api/{version}/inventory_levels"

    # 1. CONNECT to Target Location
    # We use 'set.json' with available=0 to ensure it is connected.
    # We can't use 'connect' endpoint because it throws error if already connected.
    # 'set' is safer: it connects AND sets level.
    set_url = f"{base_url}/set.json"
    payload = {
        "location_id": target_location_id,
        "inventory_item_id": inventory_item_id,
        "available": 0 # Start at 0 stock
    }
    try:
        requests.post(set_url, json=payload, headers=headers)
    except: pass

    # 2. DISCONNECT from others (e.g. remove Gloucester item from London)
    del_url = f"{base_url}.json" # DELETE endpoint
    for loc_id in all_location_ids:
        if loc_id != target_location_id:
            try:
                requests.delete(del_url, headers=headers, params={
                    "inventory_item_id": inventory_item_id,
                    "location_id": loc_id
                })
            except: pass
    return True

# --- CIN7 FAMILY & PRODUCT CREATION ---
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
                if target_val.lower() == name_or_sku.lower():
                    return i["ID"]
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
    if existing_id: return existing_id, f"✅ Family Exists (Name Match) [ID: {existing_id}]"

    tags = f"{location_name},Wholesale,{brand_name}"
    payload = {
        "Products": [],
        "SKU": full_sku,
        "Name": full_name,
        "Category": location_name,
        "DefaultLocation": location_name,
        "Brand": brand_name,
        "CostingMethod": "FIFO - Batch",
        "UOM": "each",
        "MinimumBeforeReorder": 0.0000,
        "ReorderQuantity": 0.0000,
        "PriceTier1": 0.0000,
        "Tags": tags,
        "COGSAccount": "5101",
        "RevenueAccount": "4000",
        "InventoryAccount": "1001",
        "DropShipMode": "No Drop Ship",
        "Option1Name": "Variant",
        "Option1Values": ""
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

def create_cin7_variant(row_data, family_id, family_base_sku, family_base_name, location_prefix):
    prefix = "L-" if location_prefix == "L" else "G-"
    location_name = "London" if location_prefix == "L" else "Gloucester"
    
    var_sku_raw = row_data['Variant_SKU']
    var_name_raw = row_data['Variant_Name']
    full_var_sku = f"{prefix}{var_sku_raw}"
    full_var_name = f"{prefix}{family_base_name} / {var_name_raw}"
    
    headers = get_cin7_headers()
    base_url = get_cin7_base_url()
    
    # 1. FETCH FAMILY FIRST (To check for existing Variant Value)
    family_obj = None
    fam_url = f"{base_url}/productFamily?ID={family_id}"
    try:
        r_fam = make_cin7_request("GET", fam_url, headers=headers)
        if r_fam.status_code == 200:
            fam_data_response = r_fam.json()
            if "ProductFamilies" in fam_data_response and fam_data_response["ProductFamilies"]:
                family_obj = fam_data_response["ProductFamilies"][0]
            elif "ID" in fam_data_response:
                family_obj = fam_data_response
    except Exception as e:
        return f"💥 Fetch Family Ex: {e}"

    if not family_obj:
        return "⚠️ Family object not found, cannot verify variant."

    # 2. CHECK IF VARIANT ALREADY EXISTS IN FAMILY
    current_products = family_obj.get("Products", [])
    if current_products is None: current_products = []
    
    for p in current_products:
        # Check Option1 (Variant Name)
        if str(p.get("Option1", "")).lower().strip() == str(var_name_raw).lower().strip():
            return f"⏭️ Skipped: Variant '{var_name_raw}' already exists in Family."

    # 3. GET OR CREATE PRODUCT (If we passed the check)
    product_id = None
    product_created = False

    check_url = f"{base_url}/product?Sku={quote(full_var_sku)}"
    try:
        r_check = make_cin7_request("GET", check_url, headers=headers)
        if r_check.status_code == 200:
            data = r_check.json()
            if data.get("Products"):
                product_id = data["Products"][0]["ID"]
    except Exception: pass

    if not product_id:
        brand_name = row_data['untappd_brewery']
        weight = float(row_data['Weight'])
        internal_note = f"{full_var_sku} *** {full_var_name} *** {var_name_raw} *** {family_id}"
        tags = f"{location_name},Wholesale,{brand_name}"
        
        fmt = row_data.get('format', '')
        style = row_data.get('untappd_style', '')
        abv = row_data.get('untappd_abv', '')
        keg_connector = row_data.get('Keg_Connector', '')
        prod_name_only = row_data.get('untappd_product', '')
        attr_5 = row_data.get('Attribute_5', 'Rotational Product')
        prod_type = row_data.get('Type', 'Beer')
        
        cost_price = float(row_data.get('item_price', 0))
        sales_price = calculate_sell_price(cost_price, attr_5, fmt)
        
        payload_prod = {
            "SKU": full_var_sku,
            "Name": full_var_name,
            "Category": location_name,
            "Brand": brand_name,
            "Type": "Stock",
            "CostingMethod": "FIFO - Batch",
            "DropShipMode": "No Drop Ship",
            "DefaultLocation": location_name,
            "Weight": weight,
            "UOM": "Each",
            "WeightUnits": "kg",
            "PriceTier1": sales_price, 
            "PriceTiers": {"Tier 1": sales_price},
            "InternalNote": internal_note,
            "Description": row_data['description'],
            "AdditionalAttribute1": fmt,
            "AdditionalAttribute2": style, 
            "AdditionalAttribute3": fmt,
            "AdditionalAttribute4": prod_type, 
            "AdditionalAttribute5": attr_5, 
            "AdditionalAttribute6": var_sku_raw, 
            "AdditionalAttribute7": var_name_raw,
            "AdditionalAttribute8": keg_connector, 
            "AdditionalAttribute9": prod_name_only, 
            "AdditionalAttribute10": abv,
            "AttributeSet": "Products",
            "Tags": tags,
            "Status": "Active",
            "COGSAccount": "5101",
            "RevenueAccount": "4000",
            "InventoryAccount": "1001",
            "Sellable": True,
        }
        
        try:
            r_create = make_cin7_request("POST", f"{base_url}/product", headers=headers, json=payload_prod)
            if r_create.status_code == 200:
                resp_data = r_create.json()
                if "Products" in resp_data and resp_data["Products"]:
                    product_id = resp_data["Products"][0]["ID"]
                elif "ID" in resp_data:
                    product_id = resp_data["ID"]
                product_created = True
            else:
                return f"❌ Create Failed {full_var_sku}: {r_create.text}"
        except Exception as e:
            return f"💥 Create Ex: {e}"

    if not product_id:
        return f"❌ Could not retrieve Product ID for {full_var_sku}"

    # 4. LINK TO FAMILY (Using the already fetched family object)
    # We append the new product to the list we retrieved in step 1
    current_products.append({
        "ID": product_id,
        "Option1": var_name_raw
    })

    family_obj["Products"] = current_products
    
    # Cleanup read-only fields before PUT
    read_only_fields = ['CreatedDate', 'LastModifiedOn']
    for field in read_only_fields:
        family_obj.pop(field, None)

    put_fam_url = f"{base_url}/productFamily"
    try:
        r_put = make_cin7_request("PUT", put_fam_url, headers=headers, json=family_obj)
        if r_put.status_code == 200:
            action = "Created & Linked" if product_created else "Linked Existing"
            return f"✅ {action} ({full_var_sku})"
        else:
            return f"❌ Link Failed [HTTP {r_put.status_code}]: {r_put.text}"
    except Exception as e:
        return f"💥 Link Ex: {e}"

def sync_product_to_cin7(upload_df):
    log = []
    families = upload_df.groupby('Family_SKU')
    for fam_sku, group in families:
        first_row = group.iloc[0]
        fam_name = first_row['Family_Name']
        brand = first_row['untappd_brewery']
        for loc in ["L", "G"]:
            log.append(f"🔄 Processing Family: {fam_sku} ({loc})")
            fam_id, fam_msg = create_cin7_family_node(fam_sku, fam_name, brand, loc)
            log.append(f"   -> {fam_msg}")
            if fam_id:
                for _, row in group.iterrows():
                    var_msg = create_cin7_variant(row, fam_id, fam_sku, fam_name, loc)
                    log.append(f"      -> Variant: {var_msg}")
            else:
                log.append(f"   🛑 HALT: Could not acquire Family ID. Skipping variants for {fam_sku} ({loc}).")
    return log

def create_cin7_purchase_order(header_df, lines_df, location_choice):
    headers = get_cin7_headers()
    if not headers: return False, "Cin7 Secrets missing.", []
    logs = []
    
    supplier_id = None
    if 'Cin7_Supplier_ID' in header_df.columns and header_df.iloc[0]['Cin7_Supplier_ID']:
        supplier_id = header_df.iloc[0]['Cin7_Supplier_ID']
    else:
        supplier_name = header_df.iloc[0]['Payable_To']
        supplier_data = get_cin7_supplier(supplier_name)
        if supplier_data: supplier_id = supplier_data['ID']

    if not supplier_id: return False, "Supplier not linked.", logs

    order_lines = []
    # Determine which ID column to use based on location
    id_col = 'Cin7_London_ID' if location_choice == 'London' else 'Cin7_Glou_ID'
    
    # Iterate through the PREPARED PO lines (from prepare_final_po_lines)
    for _, row in lines_df.iterrows():
        prod_id = row.get(id_col)
        
        # Validate ID exists
        if pd.notna(prod_id) and str(prod_id).strip():
            
            # Use the PRE-CALCULATED values from the preview table
            qty = float(row.get('PO_Qty', 0))
            price = float(row.get('PO_Cost', 0))
            
            total = round(qty * price, 2)
            
            order_lines.append({
                "ProductID": prod_id, 
                "Quantity": qty, 
                "Price": price, 
                "Total": total,
                "TaxRule": "20% (VAT on Expenses)",
                "Discount": 0,
                "Tax": 0
            })

    if not order_lines: return False, "No valid lines found to export.", logs

    url_create = f"{get_cin7_base_url()}/advanced-purchase"
    payload_header = {
        "SupplierID": supplier_id,
        "Location": location_choice,
        "Date": pd.to_datetime('today').strftime('%Y-%m-%d'),
        "TaxRule": "20% (VAT on Expenses)",
        "Approach": "Stock",
        "BlindReceipt": False,
        "PurchaseType": "Advanced",
        "Status": "ORDERING",
        "SupplierInvoiceNumber": str(header_df.iloc[0].get('Invoice_Number', ''))
    }
    
    task_id = None
    try:
        r1 = make_cin7_request("POST", url_create, headers=headers, json=payload_header)
        if r1.status_code == 200:
            task_id = r1.json().get('ID')
        else: return False, f"Header Error: {r1.text}", logs
    except Exception as e: return False, f"Header Ex: {e}", logs

    if task_id:
        url_lines = f"{get_cin7_base_url()}/purchase/order"
        payload_lines = {
            "TaskID": task_id,
            "CombineAdditionalCharges": False,
            "Memo": "Streamlit Import",
            "Status": "DRAFT", 
            "Lines": order_lines,
            "AdditionalCharges": []
        }
        try:
            r2 = make_cin7_request("POST", url_lines, headers=headers, json=payload_lines)
            if r2.status_code == 200:
                return True, f"✅ PO Created! ID: {task_id}", logs
            else: return False, f"Line Error: {r2.text}", logs
        except Exception as e: return False, f"Lines Ex: {e}", logs
            
    return False, "Unknown Error", logs

def normalize_vol_string(v_str):
    if not v_str: return "0"
    v_str = str(v_str).lower().strip()
    nums = re.findall(r'\d+\.?\d*', v_str)
    if not nums: return "0"
    val = float(nums[0])
    if "ml" in v_str: val = val / 10
    if val.is_integer(): return str(int(val))
    return str(val)

def run_reconciliation_check(lines_df):
    if lines_df.empty: return lines_df, ["No Lines to check."]
    logs = []
    df = lines_df.copy()
    
    # Ensure columns exist
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
        
        # --- 1. SETTINGS & THRESHOLDS ---
        use_split = row.get('Use_Split', False)
        is_strict = row.get('Strict_Search', False)
        
        # INCREASED THRESHOLDS: 65 for Fuzzy, 95 for Strict
        match_threshold = 95 if is_strict else 65
        
        # --- 2. DETERMINE TARGET PACK SIZE ---
        raw_pack_str = str(row.get('Pack_Size', '1'))
        pack_nums = re.findall(r'\d+', raw_pack_str)
        if pack_nums:
            original_pack = float(pack_nums[0])
        else:
            original_pack = 1.0

        if use_split and original_pack > 1:
            target_pack = int(original_pack / 2)
            logs.append(f"   ✂️ Splitting: Invoice {int(original_pack)} -> Looking for {target_pack}")
        else:
            target_pack = int(original_pack)
        
        inv_vol = normalize_vol_string(row.get('Volume', ''))
        inv_fmt = str(row.get('Format', '')).lower()
        
        debug_mode = "(Strict)" if is_strict else "(Fuzzy)"
        logs.append(f"Checking: **{inv_prod_name}** {debug_mode} | Target Pack: {target_pack}")

        if supplier in shopify_cache and shopify_cache[supplier]:
            candidates = shopify_cache[supplier]
            scored_candidates = []
            
            # --- 3. MATCH PRODUCT NAME ---
            # Extract numbers from Invoice Name for "Version Checking" (e.g. Vol 1 vs Vol 2)
            inv_nums = set(re.findall(r'\d+', inv_prod_name))

            for edge in candidates:
                prod = edge['node']
                shop_title_full = prod['title']
                
                # Clean Title (Remove "L-Supplier /")
                shop_prod_name_clean = shop_title_full
                if "/" in shop_title_full:
                    parts = [p.strip() for p in shop_title_full.split("/")]
                    if len(parts) >= 2: shop_prod_name_clean = parts[1]
                
                # Calculate Score
                score = fuzz.token_sort_ratio(inv_prod_name, shop_prod_name_clean)
                
                # SMART PENALTY: Number Mismatch
                # If invoice has "2" and Shopify doesn't (or vice versa), heavily penalize
                shop_nums = set(re.findall(r'\d+', shop_prod_name_clean))
                if inv_nums != shop_nums:
                    score -= 20 # Big penalty for version/number mismatch
                
                # Bonus for substring (Reduced from 15 to 5)
                if not is_strict:
                    if inv_prod_name.lower() in shop_prod_name_clean.lower(): score += 5
                
                if score > match_threshold: 
                    scored_candidates.append((score, prod, shop_prod_name_clean))
            
            scored_candidates.sort(key=lambda x: x[0], reverse=True)
            match_found = False
            
            for score, prod, clean_name in scored_candidates:
                # Double check score against threshold (in case penalties dropped it)
                if score < match_threshold: continue 
                
                # --- 4. FORMAT CHECK ---
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

                # --- 5. VARIANT MATCHING ---
                for v_edge in prod['variants']['edges']:
                    variant = v_edge['node']
                    v_title = variant['title'].lower()
                    v_sku = str(variant.get('sku', '')).strip()
                    v_tokens = re.findall(r'\d+|[a-z]+', v_title)
                    
                    # Pack Check
                    pack_match = False
                    if target_pack == 1:
                        if "x" in v_tokens and any(t.isdigit() and int(t) > 1 for t in v_tokens):
                            pack_match = False
                        else:
                            pack_match = True
                    else:
                        if str(target_pack) in v_tokens:
                            pack_match = True
                    
                    # Volume Check
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
            
            if not match_found: 
                status = "🟥 Check and Upload"
        
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
        # name tuple: (Supplier, Collab, Product, ABV)
        clean_abv_val = clean_abv(name[3])
        
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
        
        # --- NEW COLUMNS FOR TAB 2 FLOW ---
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
    
    # Initialize missing
    for col in final_cols:
        if col not in matrix_df.columns:
            if "Split_Case" in col or "Retry" in col:
                matrix_df[col] = False
            else:
                matrix_df[col] = ""
            
    # Clean types
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
        # Fallbacks
        brand_name = str(row.get('Untappd_Brewery', '')).strip()
        if not brand_name: brand_name = str(row.get('Supplier_Name', '')).strip()
            
        prod_name = str(row.get('Untappd_Product', '')).strip()
        if not prod_name: prod_name = str(row.get('Product_Name', '')).strip()

        img_url = str(row.get('Label_Thumb', '')).strip()
        if not img_url:
            lookup_key = str(row.get('Supplier_Name', '')).lower().strip()
            if lookup_key in fallback_map: img_url = fallback_map[lookup_key]

        # Validation
        missing_vals = []
        for field in required_manual:
            val = str(row.get(field, '')).strip()
            if not val: missing_vals.append(field)
        
        if missing_vals:
            errors.append(f"Row {idx+1} ({prod_name}): Missing mandatory fields: {', '.join(missing_vals)}. Please fill in Tab 3.")
            continue

        # Clean ABV
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
                    
                    # --- CHANGED: Default to empty string, not 'Beer' ---
                    'Type': row.get('Type', '') 
                }
                new_rows.append(new_row)
                
    return pd.DataFrame(new_rows), errors

# ==========================================
# 2. SESSION & SIDEBAR
# ==========================================

if 'header_data' not in st.session_state: st.session_state.header_data = None
if 'line_items' not in st.session_state: st.session_state.line_items = None
if 'matrix_data' not in st.session_state: st.session_state.matrix_data = None
if 'upload_data' not in st.session_state: st.session_state.upload_data = None 
if 'master_suppliers' not in st.session_state: st.session_state.master_suppliers = fetch_cin7_brands()
if 'drive_files' not in st.session_state: st.session_state.drive_files = []
if 'selected_drive_id' not in st.session_state: st.session_state.selected_drive_id = None
if 'selected_drive_name' not in st.session_state: st.session_state.selected_drive_name = None
if 'shopify_logs' not in st.session_state: st.session_state.shopify_logs = []
if 'untappd_logs' not in st.session_state: st.session_state.untappd_logs = []
if 'cin7_all_suppliers' not in st.session_state: st.session_state.cin7_all_suppliers = fetch_all_cin7_suppliers_cached()
if 'line_items_key' not in st.session_state: st.session_state.line_items_key = 0
if 'matrix_key' not in st.session_state: st.session_state.matrix_key = 0
if 'upload_generated' not in st.session_state: st.session_state.upload_generated = False 

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
        custom_rule = st.text_area("Inject Temporary Rule:", height=100)
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
                images = convert_from_bytes(target_stream.read(), dpi=300)
                
                st.write(f"2. Extracting Text from {len(images)} pages...")
                full_text = ""
                for i, img in enumerate(images):
                    st.write(f"   - Scanning page {i+1}...")
                    full_text += pytesseract.image_to_string(img) + "\n"

                st.write("3. Sending Text to AI Model...")
                injected = f"\n!!! USER OVERRIDE !!!\n{custom_rule}\n" if custom_rule else ""

                # --- UPDATED PROMPT: STRICT NULL HANDLING ---
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
                    "line_items": [
                        {{
                            "Supplier_Name": "...", "Collaborator": "...", "Product_Name": "...", 
                            "ABV": null,
                            "Format": "...", "Pack_Size": "...", "Volume": "...", "Quantity": 1, "Item_Price": 10.00
                        }}
                    ]
                }}
                SUPPLIER RULEBOOK: {json.dumps(SUPPLIER_RULEBOOK)}
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
                
                # --- ROBUST ABV CLEANING ---
                # 1. Normalize Header
                df_lines.columns = [c.strip() for c in df_lines.columns]
                df_lines.rename(columns=lambda x: 'ABV' if x.lower() == 'abv' else x, inplace=True)

                # 2. Apply cleaning
                if 'ABV' in df_lines.columns:
                    # Fill explicit Nones with empty string so clean_abv handles them correctly
                    df_lines['ABV'] = df_lines['ABV'].fillna("").apply(clean_abv)
                # ---------------------------

                df_lines = clean_product_names(df_lines)
                if st.session_state.master_suppliers:
                    df_lines = normalize_supplier_names(df_lines, st.session_state.master_suppliers)

                df_lines['Shopify_Status'] = "Pending"
                
                # Initialize Flags
                df_lines['Use_Split'] = False 
                df_lines['Strict_Search'] = False 
                
                cols = ["Use_Split", "Strict_Search", "Supplier_Name", "Collaborator", "Product_Name", "ABV", "Format", "Pack_Size", "Volume", "Item_Price", "Quantity"]
                existing = [c for c in cols if c in df_lines.columns]
                st.session_state.line_items = df_lines[existing]
                
                st.session_state.shopify_logs = []
                st.session_state.untappd_logs = []
                st.session_state.matrix_data = None
                st.session_state.upload_data = None
                st.session_state.upload_generated = False # RESET
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
    if 'Shopify_Status' in df.columns:
        unmatched_count = len(df[df['Shopify_Status'] != "✅ Match"])
    else: unmatched_count = len(df) 
    all_matched = (unmatched_count == 0) and ('Shopify_Status' in df.columns)

    # --- UPDATED TAB TITLES ---
    tabs = ["📝 1. Line Items", "🔍 2. Prepare Search", "🍺 3. Prepare Upload", "☁️ 4. Product Upload", "🚀 5. Finalize PO"]
    current_tabs = st.tabs(tabs)
    
    # --- TAB 1: LINE ITEMS ---
    # --- TAB 1: LINE ITEMS ---
    with current_tabs[0]:
        st.subheader("1. Review & Edit Lines")
        
        # 1. Prepare Display Data (No Renaming Columns!)
        display_df = st.session_state.line_items.copy()
        
        # 2. Define Order (Use ACTUAL column names)
        ideal_order = [
            'Use_Split', 
            'Strict_Search', 
            'Shopify_Status', # <--- Keep original name
            'Matched_Product', 
            'Matched_Variant', 
            'Image', 
            'Supplier_Name', 
            'Product_Name', 
            'ABV', 
            'Format', 
            'Pack_Size', 
            'Volume', 
            'Quantity', 
            'Item_Price', 
            'Collaborator', 
            'Shopify_Variant_ID', 
            'London_SKU', 
            'Gloucester_SKU'
        ]
        
        # Filter and Reorder columns safely
        final_cols = [c for c in ideal_order if c in display_df.columns]
        rem = [c for c in display_df.columns if c not in final_cols]
        display_df = display_df[final_cols + rem]
        
        # 3. Configure Columns (Renaming happens VISUALLY here)
        column_config = {
            "Image": st.column_config.ImageColumn("Img"),
            # Rename header visually, but keep data ID same to prevent bugs
            "Shopify_Status": st.column_config.TextColumn("Status", disabled=True), 
            "Matched_Product": st.column_config.TextColumn("Shopify Match", disabled=True),
            "Matched_Variant": st.column_config.TextColumn("Variant Match", disabled=True),
            "Use_Split": st.column_config.CheckboxColumn("Order Split?", width="small", help="Tick to order half-case (e.g. 12x instead of 24x)"),
            "Strict_Search": st.column_config.CheckboxColumn("Strict?", width="small", help="Tick to force exact name matching (prevents Vol 1 matching Vol 2)")
        }

        # 4. Render Editor
        edited_lines = st.data_editor(
            display_df, 
            num_rows="dynamic", 
            width='stretch',
            key=f"line_editor_{st.session_state.line_items_key}",
            column_config=column_config
        )
        
        # 5. Save Data (Directly, no conversion needed)
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

    # --- TAB 2: PREPARE MISSING ITEMS ---
    with current_tabs[1]:
        st.subheader("2. Prepare Missing Items for Search")
        
        if all_matched:
            st.success("🎉 All products matched to Shopify! No action needed here.")
        elif st.session_state.matrix_data is not None and not st.session_state.matrix_data.empty:
            
            # --- 1. CHECK IF SEARCH HAS BEEN RUN ---
            # We look for any non-empty value in the Untappd_Status column
            search_has_run = False
            if 'Untappd_Status' in st.session_state.matrix_data.columns:
                # distinct values that are not empty string
                status_vals = st.session_state.matrix_data['Untappd_Status'].astype(str).unique()
                if any(v.strip() for v in status_vals):
                    search_has_run = True

            if search_has_run:
                st.info("👇 Review matches below. If a match is incorrect, edit the Name/Supplier, tick 'Retry', and click Search again.")
            else:
                st.info("👇 Select the **Product Type** for each item below, then click Search.")
            
            type_options = ["Beer", "Cider", "Spirits", "Softs", "Wine", "Merch", "Dispense", "Snacks", "PoS", "Other", "Free Of Charge PoS"]
            
            # --- 2. CONFIG ---
            prep_config = {
                "Type": st.column_config.SelectboxColumn("Product Type", options=type_options, required=True, width="medium"),
                "Untappd_Status": st.column_config.TextColumn("UT Status", disabled=True, width="small"),
                "Match_Check": st.column_config.TextColumn("Match Details (Verify Here)", disabled=True, width="large"),
                "Retry": st.column_config.CheckboxColumn("Retry?", width="small", help="Tick this and click Search to re-run lookup for this line.")
            }
            
            # Add Dynamic Config for Repeating Columns
            for i in range(1, 4):
                prep_config[f"Format{i}"] = st.column_config.TextColumn(f"Format {i}", width="small")
                prep_config[f"Pack_Size{i}"] = st.column_config.TextColumn(f"Pack {i}", width="small")
                prep_config[f"Volume{i}"] = st.column_config.TextColumn(f"Vol {i}", width="small")
                prep_config[f"Item_Price{i}"] = st.column_config.NumberColumn(f"Cost {i}", format="£%.2f", width="small")
                prep_config[f"Split_Case{i}"] = st.column_config.CheckboxColumn(f"Split {i}?", width="small")

            # --- 3. DYNAMIC COLUMN ORDER ---
            if search_has_run:
                # Post-Search: Retry First, then Status/Check, then Data
                base_cols = ['Retry', 'Untappd_Status', 'Match_Check', 'Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']
            else:
                # Pre-Search: Just Data (Cleaner UI)
                base_cols = ['Supplier_Name', 'Type', 'Collaborator', 'Product_Name', 'ABV']
            
            ordered_cols = base_cols.copy()
            for i in range(1, 4):
                if f"Format{i}" in st.session_state.matrix_data.columns:
                    ordered_cols.extend([f"Format{i}", f"Pack_Size{i}", f"Volume{i}", f"Item_Price{i}", f"Split_Case{i}"])
            
            display_cols = [c for c in ordered_cols if c in st.session_state.matrix_data.columns]

            # --- SAFETY FIX (Types) ---
            for col in display_cols:
                if "Pack_Size" in col or "Volume" in col or "Format" in col:
                    st.session_state.matrix_data[col] = (
                        st.session_state.matrix_data[col]
                        .fillna("")
                        .astype(str)
                        .str.replace(r'\.0$', '', regex=True)
                        .replace("nan", "")
                    )

            edited_prep = st.data_editor(
                st.session_state.matrix_data[display_cols],
                num_rows="fixed",
                width='stretch',
                column_config=prep_config,
                key=f"prep_editor_{st.session_state.matrix_key}"
            )
            
            if edited_prep is not None:
                st.session_state.matrix_data.update(edited_prep)

            st.divider()
            col_search, col_help = st.columns([1, 2])
            with col_search:
                missing_types = st.session_state.matrix_data['Type'].replace('', pd.NA).isna().sum()
                
                # Button Text Changes based on state
                btn_label = "🔎 Search Untappd Details" if not search_has_run else "🔎 Search Again / Retry"
                
                if st.button(btn_label, type="primary"):
                    if missing_types > 0:
                        st.error(f"⚠️ Please select a Product Type for all {missing_types} rows above before searching.")
                    elif "untappd" in st.secrets:
                        with st.spinner("Searching Untappd API..."):
                             updated_matrix, u_logs = batch_untappd_lookup(st.session_state.matrix_data)
                             st.session_state.matrix_data = updated_matrix
                             st.session_state.untappd_logs = u_logs
                             st.session_state.matrix_key += 1 
                             st.success("Search Complete!") 
                             st.rerun()
                    else: st.error("Untappd Secrets Missing")
            with col_help:
                if st.session_state.untappd_logs:
                    with st.expander("View Search Logs"): st.write(st.session_state.untappd_logs)
        else: st.info("Run 'Check Inventory' in Tab 1 first.")

    # --- TAB 3: PREPARE UPLOAD ---
    with current_tabs[2]:
        # --- UPDATED SUBHEADER ---
        st.subheader("3. Review matches and add missing product information")
        
        has_untappd_cols = 'Untappd_Status' in st.session_state.matrix_data.columns if st.session_state.matrix_data is not None else False
        
        if not has_untappd_cols:
             st.warning("⚠️ Please run the search in 'Tab 2. Prepare Search' first.")
        
        elif st.session_state.matrix_data is not None and not st.session_state.matrix_data.empty:
            st.info("👇 These details will be used to create products in Cin7. Edit manually if the match is wrong or missing.")
            
            # ... (Rest of Tab 3 code remains the same)
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

            edited_matches = st.data_editor(
                st.session_state.matrix_data,
                column_order=full_view,
                num_rows="fixed",
                width='stretch',
                key=f"match_editor_{st.session_state.matrix_key}",
                column_config=column_config
            )
            
            if edited_matches is not None: st.session_state.matrix_data = edited_matches

            st.divider()
            if st.button("✨ Validate & Stage for Upload", type="primary"):
                staged_df, errors = stage_products_for_upload(st.session_state.matrix_data)
                if errors:
                    for e in errors: st.error(e)
                else:
                    st.session_state.upload_data = staged_df
                    st.session_state.upload_generated = False # RESET FLAG
                    st.success("Products staged successfully! Go to Tab 4.")

    # --- TAB 4: PRODUCT UPLOAD ---
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
                    
                    # Ensure Type carries over but stays empty if it was empty
                    prod_type = row.get('Type', '')

                    # Clean ABV
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
                            new_row['Type'] = prod_type # Keep passed value (empty or set)
                            
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
                
                # --- REORDER COLUMNS HERE ---
                all_cols = final_df.columns.tolist()
                # Type moved to 2nd position
                desired_order = ['Attribute_5', 'Type', 'Sales_Price', 'item_price', 'Variant_Name', 'Variant_SKU', 'Family_Name']
                final_order = []
                for c in desired_order:
                    if c in all_cols: final_order.append(c)
                for c in all_cols:
                    if c not in final_order: final_order.append(c)
                
                st.session_state.upload_data = final_df[final_order]
                st.session_state.upload_generated = True 
                
                # --- CHECK FOR MISSING TYPES IMMEDIATELY ---
                missing_types = st.session_state.upload_data['Type'].replace('', pd.NA).isna().sum()
                if missing_types > 0:
                    st.warning(f"⚠️ You have {missing_types} rows with missing Product Types. Please select a Type in the table below before uploading.")
                else:
                    st.success("Upload Data Generated!")
                
                st.rerun()

            # --- VALIDATION LOGIC ---
            # Check if any Types are missing
            missing_types = 0
            if 'Type' in st.session_state.upload_data.columns:
                missing_types = st.session_state.upload_data['Type'].replace('', pd.NA).isna().sum()

            if missing_types > 0:
                st.error(f"🛑 STOP: {missing_types} rows are missing a Product Type. Please select a Type in the table below.")

            # --- DATA EDITOR ---
            upload_col_config = {
                "Attribute_5": st.column_config.SelectboxColumn("Core/Rotation", options=["Rotational Product", "Core Product"], required=True, width="medium"),
                "Type": st.column_config.SelectboxColumn("Product Type", options=["Beer", "Cider", "Spirits", "Softs", "Wine", "Merch", "Dispense", "Snacks", "PoS", "Other"], required=True, width="medium"),
                "Sales_Price": st.column_config.NumberColumn("Sales Price", format="£%.2f", disabled=True)
            }
            
            current_cols = st.session_state.upload_data.columns.tolist()
            # Ensure Type is 2nd in display
            disp_order = ['Attribute_5', 'Type', 'Sales_Price', 'item_price', 'Variant_Name', 'Variant_SKU', 'Family_Name']
            final_disp = []
            for c in disp_order:
                if c in current_cols: final_disp.append(c)
            for c in current_cols:
                if c not in final_disp: final_disp.append(c)

            edited_upload = st.data_editor(
                st.session_state.upload_data,
                width=2000,
                column_config=upload_col_config,
                column_order=final_disp, 
                key="upload_editor_final"
            )
            if edited_upload is not None: st.session_state.upload_data = edited_upload

            # --- SHOPIFY SECTION ---
            st.divider()
            st.markdown("### 🛒 Shopify Integration")

            col_shop_1, col_shop_2 = st.columns([1, 1])
            
            with col_shop_1:
                # Disabled if missing types or not generated
                btn_disabled = not st.session_state.upload_generated or missing_types > 0
                
                if st.button("🕵️ Check Shopify Existence (L- & G-)", disabled=btn_disabled):
                    if "shopify" in st.secrets:
                        unique_families = st.session_state.upload_data['Family_Name'].unique()
                        st.write(f"Scanning Shopify for {len(unique_families)} Families (x2 locations)...")
                        results = []
                        prog_bar = st.progress(0)
                        for i, base_name in enumerate(unique_families):
                            prog_bar.progress((i + 1) / len(unique_families))
                            l_title = f"L-{base_name}"
                            g_title = f"G-{base_name}"
                            l_pid, _ = check_shopify_title(l_title)
                            g_pid, _ = check_shopify_title(g_title)
                            l_status = f"✅ ({l_pid})" if l_pid else "🆕 Create"
                            g_status = f"✅ ({g_pid})" if g_pid else "🆕 Create"
                            results.append({
                                "Family Base Name": base_name,
                                "L- Prefix": l_status, "G- Prefix": g_status,
                                "L_ID": l_pid, "G_ID": g_pid  
                            })
                            time.sleep(0.2) 

                        st.success("Scan Complete")
                        results_df = pd.DataFrame(results)
                        st.dataframe(results_df[["Family Base Name", "L- Prefix", "G- Prefix"]], use_container_width=True)
                        st.session_state.shopify_check_results = results_df
                    else: st.error("Shopify secrets missing.")

            with col_shop_2:
                if st.button("📍 Check Location IDs (Debug)"):
                    if "shopify" in st.secrets:
                        creds = st.secrets["shopify"]
                        shop_url = creds.get("shop_url")
                        token = creds.get("access_token")
                        version = creds.get("api_version", "2024-04")
                        url = f"https://{shop_url}/admin/api/{version}/locations.json"
                        headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
                        
                        try:
                            r = requests.get(url, headers=headers)
                            if r.status_code == 200:
                                locs = r.json().get('locations', [])
                                st.write("### 🏢 Available Locations")
                                for l in locs:
                                    st.code(f"Name: {l['name']}\nID:   {l['id']}")
                                st.info("If auto-detection fails, add these IDs to secrets.toml:\n\n[shopify]\nlocation_id_london = 123...\nlocation_id_gloucester = 456...")
                            else:
                                st.error(f"API Error: {r.status_code} - {r.text}")
                        except Exception as e:
                            st.error(f"Connection Error: {e}")

            st.divider()

            # Upload Button - Disabled if missing types
            if st.button("🚀 Upload to Shopify (L & G)", disabled=btn_disabled):
                if missing_types > 0:
                    st.error("Please fill in all Product Types above first.")
                    st.stop()
                    
                if "shopify" not in st.secrets:
                    st.error("Shopify secrets missing.")
                    st.stop()
                
                # ... (Rest of Shopify Upload Logic) ...
                # 1. SETUP & FETCH IDs
                creds = st.secrets["shopify"]
                shop_url = creds.get("shop_url")
                token = creds.get("access_token")
                version = creds.get("api_version", "2023-04") 
                base_url = f"https://{shop_url}/admin/api/{version}"
                headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
                
                st.write("Fetching Configuration (Locations & Catalogs)...")
                
                # A. Locations
                loc_data = fetch_shopify_location_ids()
                if not loc_data or not loc_data['london'] or not loc_data['gloucester']:
                    st.error("❌ Location Error: Could not find 'London' or 'Gloucester' Location IDs.")
                    st.stop()

                # B. Publications/Catalogs
                pub_data = fetch_publication_ids()
                if not pub_data or not pub_data['london'] or not pub_data['gloucester']:
                    st.warning("⚠️ Catalog Warning: Could not find 'London' or 'Gloucester' Catalogs/Publications. Products will be created but NOT added to B2B catalogs.")
                else:
                    st.success("✅ Found Catalogs: London & Gloucester")

                log_box = st.expander("Shopify Upload Logs", expanded=True)
                grouped = st.session_state.upload_data.groupby('Family_Name')
                prog_bar = st.progress(0)
                total_groups = len(grouped)
                
                for i, (fam_name, group) in enumerate(grouped):
                    prog_bar.progress((i)/total_groups)
                    log_box.write(f"**Processing: {fam_name}**")
                    
                    for loc_prefix in ["L", "G"]:
                        full_title = f"{loc_prefix}-{fam_name}"
                        pid, existing_vid = check_shopify_title(full_title)
                        
                        target_loc_id = loc_data['london'] if loc_prefix == "L" else loc_data['gloucester']
                        target_pub_id = pub_data['london'] if loc_prefix == "L" else pub_data['gloucester']
                        
                        if pid:
                            # --- EXISTING PRODUCT ---
                            log_box.write(f"   🔹 {loc_prefix}: Found ID {pid}. Checking variants...")
                            
                            # Ensure existing product is published to the catalog
                            if target_pub_id:
                                published = publish_product_to_app(pid, target_pub_id)
                                if published: log_box.write(f"      📖 Verified in Catalog")

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
                                            log_box.write(f"      ✅ Added Variant & Set Loc: {var_title}")
                                        else:
                                            log_box.write(f"      ⚠️ Added {var_title} but missed inventory ID.")
                                            
                                    elif r.status_code == 422 and "already exists" in r.text:
                                         log_box.write(f"      ⚠️ Variant SKU Exists: {row['Variant_Name']}")
                                    else:
                                        log_box.write(f"      ❌ Variant Error: {r.text}")
                                        log_box.code(json.dumps(var_payload, indent=2))
                                except Exception as e:
                                    log_box.write(f"      💥 Variant Exception: {e}")
                                    
                        else:
                            # --- NEW PRODUCT ---
                            log_box.write(f"   🆕 {loc_prefix}: Creating New Product...")
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
                                    log_box.write(f"      ✅ Created Product! ID: {new_id}")
                                    
                                    # 1. Update Inventory Locations
                                    created_variants = p_resp.get('variants', [])
                                    for cv in created_variants:
                                        inv_id = cv.get('inventory_item_id')
                                        if inv_id:
                                            set_variant_location(inv_id, target_loc_id, loc_data['all_ids'])
                                    log_box.write(f"      📍 Location set to {'London' if loc_prefix=='L' else 'Gloucester'}")
                                    
                                    # 2. Publish to Catalog
                                    if target_pub_id:
                                        published = publish_product_to_app(new_id, target_pub_id)
                                        if published: log_box.write(f"      📖 Published to Catalog")
                                        else: log_box.write(f"      ⚠️ Catalog Publish Failed")

                                else:
                                    log_box.write(f"      ❌ Create Error: {r.text}")
                                    log_box.code(json.dumps(prod_payload, indent=2))
                            except Exception as e:
                                log_box.write(f"      💥 Create Exception: {e}")
                        time.sleep(0.5)
                prog_bar.progress(1.0)
                st.success("Shopify Process Complete!")

            # --- CIN7 SECTION ---
            st.divider()
            st.markdown("### 📦 Cin7 Integration")
            
            # Cin7 Upload - Disabled if missing types
            if st.button("🚀 Upload To Cin7 (Families & Variants)", disabled=btn_disabled):
                if missing_types > 0:
                    st.error("Please fill in all Product Types above first.")
                    st.stop()

                if "cin7" in st.secrets:
                    unique_rows = st.session_state.upload_data.copy()
                    log_box = st.expander("Sync Log", expanded=True)
                    full_log = sync_product_to_cin7(unique_rows)
                    for line in full_log: log_box.write(line)
                    st.success("Sync Process Complete!")
                else: st.error("Cin7 Secrets Missing")

    # --- TAB 5: HEADER / EXPORT ---
    # --- TAB 5: HEADER / EXPORT ---
    with current_tabs[4]:
        st.subheader("5. Finalize & Export")
        
        # --- LOCK LOGIC ---
        if not all_matched:
            st.error("🔒 **Tab Locked**")
            st.warning(f"You have **{unmatched_count} unmatched items**. Please resolve them in **Tab 1** (Check Inventory) or **Tab 2** (Search Untappd) before finalizing the Purchase Order.")
        else:
            # --- HEADER SECTION ---
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

            # --- NEW PREVIEW SECTION ---
            st.markdown("#### B. PO Line Preview (Calculated)")
            st.caption("Review the final quantities and costs below. Split cases have been calculated.")

            # Generate the preview dataframe
            po_preview_df = prepare_final_po_lines(st.session_state.line_items)

            if not po_preview_df.empty:
                po_col_config = {
                    "PO_Qty": st.column_config.NumberColumn("Final Qty", format="%.2f"),
                    "PO_Cost": st.column_config.NumberColumn("Final Cost", format="£%.2f"),
                    "Total": st.column_config.NumberColumn("Line Total", format="£%.2f", disabled=True),
                    "Notes": st.column_config.TextColumn("Notes", disabled=True)
                }
                
                # Show the editor (readonly except for maybe Qty/Cost if you really wanted, but best keep readonly for integrity)
                st.dataframe(
                    po_preview_df, 
                    use_container_width=True, 
                    column_config=po_col_config,
                    hide_index=True
                )
            else:
                st.warning("No matched lines to display.")

            st.divider()
            
            # --- EXPORT SECTION ---
            st.markdown("#### C. Export")
            po_location = st.selectbox("Select Delivery Location:", ["London", "Gloucester"], key="final_po_loc")
            
            if st.button(f"📤 Export PO to Cin7 ({po_location})", type="primary", disabled=po_preview_df.empty):
                if "cin7" in st.secrets:
                    with st.spinner("Creating Purchase Order..."):
                        # Pass the PREVIEW DF (po_preview_df) not the raw items
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












































