# ==========================================
# 1. MASTER DATA (The Valid List)
# ==========================================
VALID_FORMATS = """
Cask | 9 Gallon
Cask | 4.5 Gallon
Cask | 5 Litre
KeyKeg | 10 Litre
KeyKeg | 20 Litre
KeyKeg | 30 Litre
KeyKeg | 12 Litre
KeyKeg | 50 Litre
Steel Keg | 20 Litre
Steel Keg | 30 Litre
Steel Keg | 50 Litre
Steel Keg | 12 Litre
Bag in Box | 10 Litre
Bag in Box | 20 Litre
Bag in Box | 5 Litre
Bottles | 33cl
Bottles | 50cl
Bottles | 75cl
Bottles | 66cl
Bottles | 35cl
Bottles | 56.8cl
Bottles | 70cl
Bottles | 20cl
Bottles | 25cl
Bottles | 24cl
Bottles | 27.5cl
Bottles | 35.5cl
Bottles | 37.5cl
Bottles | 10cl
Bottles | 150cl
Bottles | 34cl
Bottles | 30cl
Cans | 33cl
Cans | 44cl
Cans | 25cl
Cans | 56.8cl
Cans | 50cl
Cans | 35cl
Cans | 47.3cl
Cans | 18.7cl
Cans | 10cl
Cans | 40.3cl
Cans | 35.5cl
Cans | 12.5cl
Cans | 47cl
Cans | 14cl
PolyKeg | 10 Litre
PolyKeg | 20 Litre
PolyKeg | 30 Litre
PolyKeg | 12 Litre
PolyKeg | 50 Litre
UniKeg | 10 Litre
UniKeg | 20 Litre
UniKeg | 30 Litre
UniKeg | 12 Litre
UniKeg | 50 Litre
Dolium Keg | 10 Litre
Dolium Keg | 20 Litre
Dolium Keg | 30 Litre
Dolium Keg | 12 Litre
Dolium Keg | 50 Litre
EcoKeg | 10 Litre
EcoKeg | 20 Litre
EcoKeg | 30 Litre
EcoKeg | 12 Litre
EcoKeg | 50 Litre
US Dolium Keg | 20 Litre
Cellar Equipment | 250 Pack
"""

# ==========================================
# 2. GLOBAL RULES (Applies to everyone)
# ==========================================
# ==========================================
# 2. GLOBAL RULES (Applies to everyone)
# ==========================================
GLOBAL_RULES_TEXT = f"""
1. **PRODUCT NAMES (SMART CLEANING)**:
   - **Hyphen Handling**: 
     - **DO NOT** blindly split at every hyphen (-).
     - **CHECK**: Does the text AFTER the hyphen describe the Beer (e.g. "Nelson Sauvin", "Restoration Series")? -> **KEEP IT**.
     - **CHECK**: Does the text AFTER the hyphen describe Format/Size (e.g. "Firkin", "9G", "Keg")? -> **REMOVE IT**.
     - Example: "Ruby Mild - Restoration Series - Firkin" -> Product Name: "Ruby Mild - Restoration Series".
   - **Remove Prefixes**: Strip codes like "SRM-", "NRB", "30EK", "9G".
   - **Collaborator**: Extract partner names (e.g. "STF/Croft" -> Collab: Croft).
   - **Title Case**: Convert Product Name to Title Case.

2. **FORMAT MAPPING (The Dictionary)**:
   - "LSS" -> Steel Keg.
   - "Kegstar" (41L) -> Cask 9 Gallon.
   - "Kegstar" (Other) -> Steel Keg.
   - "E-Keg" / "eKeg" / "Keg" -> Steel Keg.
   - "Firkin" -> Cask 9 Gallon.
   - "Pin" -> Cask 4.5 Gallon.
   - "Poly" -> PolyKeg.
   - **Conversions**: ml->cl, L->Litre.

3. **VALID LIST HANDLING**:
   - The "VALID FORMATS LIST" uses `Format | Volume`. SPLIT this into two columns.

4. **PACK SIZE vs QUANTITY**:
   - **Pack_Size**: Bottles/Cans = count. Kegs = NULL.
   - **Quantity**: Units ordered.

5. **FINANCIALS**: 
   - **Item_Price**: Price per PURCHASE UNIT.
   - **Landed Cost**: (Total Delivery / Total Units) + Item Price.
   - **Discount**: Apply line item discounts. In case of line discount, divide line total by number of units to find cost price per unit.

6. **FILTERING**:
   - Exclude "pump clip", "badge" ONLY IF price is 0.00.

7. **HEADER EXTRACTION**:
   - **Payable_To**: The Supplier Name (Not Pig's Ears).

VALID FORMATS LIST:
{VALID_FORMATS}
"""

# ==========================================
# 3. SUPPLIER SPECIFIC RULEBOOK
# ==========================================
SUPPLIER_RULEBOOK = {

   "Thornbridge Brewery": """
   when the product name includes ekeg it is a steel keg, when it includes keg it is a Keykeg

   The product format is the last part of each product name, this needs removing e.g. Green Mountain AF Ekeg should be Green Mountain AF and Wild Swan Ecask should be Wild Swan

   when the product name includes ECask the format is cask - be careful to check all the lines on the invoice, sometimes there are products with the same name but different formats
   e.g Jaipur keg and jaipur Ecask 
   sometimes the qty, ord (quantity) is more than 1 digit e.g 18
   for example
   18.0000 0.0000 18.0000B/JAIP-ECA09-059 Jaipur Ecask - 5.9% ABV 9 Gallon 97.00 ECask 1,746.00

   The products Pale Rider and Easy Rider should have the Supplier_Name Kelham Island Brewery
   """,
   
   "UnBarred Brewery": """
   For Unbarred Brewery when the product name has Cask - at the beginning remove this from the product name
   """,
   
   "Pig & Porter": """
   For Pig And Porter Limited the Supplier_Name is always Pig & Porter

   Product Skylarking Session Ipa is just called Skylarking

   Product Got The Face On Tropical Pale is just called Got The Face On
   """,
   
   "Crafty AF Ltd": """
   For Craft AF Ltd the supplier_name for the products is always Below Brew Co
   """,
      
   "Lakedown Brewing Company LLP": """
   for Lakedown Brewing Company LLP the supplier_name for the products is Lakedown Brewing Co.

   The product name is within apostrophes - this the rest needs disregarding - unless no apostrophes exist then use the full string

   If the product name contains Marquee - that is the product name - disregard the information within the apostrophes!
   """,

   
   "Anspach & Hobday": """
   check the product name carefully
   the pack size is in the product name for cans
   """,
   
   "The Beak Brewery Limited": """
   the abv and style is at the end of the product name - this needs removing
   the data is set out in the following order
   Quantity, Product_Name, Unit Price, Line Discount (Amount) / Line Discount (Percentage), VAT (Tax), Line total price

   for example
   QTY ITEM UNIT PRICE DISCOUNT VAT LINE PRICE is as follows
   3 Aloft 5% DDH pale - 30L SS £135.00 £16.875 / 12.5% 20% £354.38
   
   IMPORTANT The item price is is the line price divided by the quantity - calculate the item price this way
   firkin is always 9 Gallon for volume
   """,
   
   "Track Brewing Company Limited": """
   FOR TRACK the style is listed in the product name after the last -
   Don't include this in the product name
   If the product name includes Dreaming Of... the next part is the hop variety and needs to be included in the product name
   """,
   
   "Little Mercies Limited": """
   For Little Mercies all bottles apart from Gift Boxes need to be adjusted to be pack size 1. The cost price needs to be recalculated to account for this 
   """,
   
   "Trenchmore LLP": """
   - Supplier Name: "Silly Moo Cider".
   - Product Name: Remove "Silly Moo" from description.
   """,
   
   "Pilton Cider Ltd": """
   - BOTTLES: If size is 33cl -> Pack_Size: 12.
   - BOTTLES: If size is 75cl -> Pack_Size: 1.
   """,
   
    "DEYA Brewing Company": """
    - FORMAT: "LSS" -> Steel Keg.
    - CANS: 500mL -> Volume: 50cl.
    """,

    "Simple Things Fermentations": """
    - COLLAB: "STF/Croft 3" -> Collaborator is "Croft 3".
    - PREFIX: Remove "30EK", "9G".
    - DISCOUNT: Apply 15% discount.
    """,
    
    "James Clay and Sons": """
    - STRATEGY: Split Description into Supplier/Product.
    - PATTERN: "NxVol" (e.g. 20x50cl) indicates Pack/Volume.
    """,
    
    "Polly's Brew Co.": "PRODUCT NAME: Stop at first hyphen. Watch for 18-packs.",
    
    "North Riding Brewery": "DISCOUNT: Handle '(discount)' line item (negative total). Divide by units.",
    
    "Neon Raptor": "Handle 'Discount' column. Merge multi-line descriptions.",

   "German Drinks Company Limited": """
   - Supplier: Extract from Product Name (e.g. "Rothaus").
   - Product: The remainder (e.g. "Pils").
   - Format: Default to "Bottles" unless "Keg" is specified.
   - Payable To: "German Drinks Company Limited".
   """
}
