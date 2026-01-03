import streamlit as st
import requests
import json

st.set_page_config(page_title="Cin7 Connector Test")
st.title("🧪 Cin7 Supplier Dropdown Test")

# 1. Check Secrets
if "cin7" not in st.secrets:
    st.error("❌ Cin7 secrets not found in .streamlit/secrets.toml")
    st.stop()

# 2. Define the Fetch Function
@st.cache_data(ttl=600) # Cache for 10 mins so it's fast after first load
def fetch_all_cin7_suppliers():
    """
    Fetches ALL suppliers from Cin7 (Paginated).
    Returns a list of dictionaries: [{'Name': '...', 'ID': '...'}]
    """
    creds = st.secrets["cin7"]
    base_url = creds.get("base_url", "https://inventory.dearsystems.com/ExternalApi/v2")
    
    # Headers - Cin7 is strict about case sensitivity sometimes
    headers = {
        "api-auth-accountid": creds.get("account_id"),
        "api-auth-applicationkey": creds.get("api_key"),
        "Content-Type": "application/json"
    }
    
    all_suppliers = []
    page = 1
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    try:
        while True:
            status_text.text(f"Fetching Page {page}...")
            
            # Manual URL construction to ensure parameters are clean
            url = f"{base_url}/supplier?Page={page}&Limit=100"
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                st.error(f"API Error on Page {page}: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            
            if "Suppliers" in data and data["Suppliers"]:
                batch = data["Suppliers"]
                # Extract just what we need
                for s in batch:
                    all_suppliers.append({
                        "Name": s.get("Name"),
                        "ID": s.get("ID"),
                        "Currency": s.get("Currency")
                    })
                
                # Update UI
                count = len(all_suppliers)
                status_text.text(f"Fetched {count} suppliers so far...")
                
                # Pagination Logic: If we got less than 100, we are at the end
                if len(batch) < 100:
                    break
                
                page += 1
            else:
                break
                
    except Exception as e:
        st.error(f"Connection Exception: {e}")
        return []
        
    progress_bar.empty()
    status_text.success(f"✅ Successfully loaded {len(all_suppliers)} suppliers.")
    
    # Sort alphabetically
    return sorted(all_suppliers, key=lambda x: x['Name'].lower())

# 3. The UI Interaction
if st.button("🔄 Connect & Fetch Suppliers"):
    suppliers = fetch_all_cin7_suppliers()
    
    if suppliers:
        st.divider()
        st.subheader("Simulated Invoice Header")
        
        # Create a list of names for the dropdown
        supplier_names = [s['Name'] for s in suppliers]
        
        # Simulate an AI guess (e.g. AI extracted "Anspach")
        ai_guess = "Anspach & Hobday"
        
        # Try to find the index of the guess
        default_index = 0
        if ai_guess in supplier_names:
            default_index = supplier_names.index(ai_guess)
        
        selected_name = st.selectbox(
            "Select Payable To:", 
            options=supplier_names,
            index=default_index
        )
        
        # Find the ID for the selection
        selected_data = next((s for s in suppliers if s['Name'] == selected_name), None)
        
        if selected_data:
            st.info(f"**Selected ID:** `{selected_data['ID']}`")
            st.info(f"**Currency:** `{selected_data['Currency']}`")
            
            st.write("---")
            st.json(selected_data) # Show full data object
