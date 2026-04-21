"""
generate_lookup_tables.py

Stage‑2 pipeline for normalizing make/model data and classifying EV types.

Input:
    all_make_models.csv
        - CD_MAKE_VEH
        - CD_MODEL_VEH

Output (in seeds_stage2/):
    - lk_make_map.csv
    - lk_model_map.csv
    - lk_ev_model_map.csv
"""

import pandas as pd
import re
import os

# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = "all_make_models.csv"
OUTPUT_DIR = "seeds_stage2"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# 1. MAKE NORMALIZATION
# ============================================================

MAKE_CORRECTIONS = {
    "B Y D": "BYD",
    "B M W": "BMW",

    "HYNDAI": "HYUNDAI",
    "HYUNDAI ": "HYUNDAI",
    "HYUNDAI AUST": "HYUNDAI",
    "HYUNDAI MOTORS": "HYUNDAI",
    "HYU NDAI": "HYUNDAI",
    "HYUND": "HYUNDAI",

    "MAHIND": "MAHINDRA",
    "MITSUB": "MITSUBISHI",

    "MERC BENZ": "MERCEDES-BENZ",
    "MERC B": "MERCEDES-BENZ",
    "MERCEDES BENZ": "MERCEDES-BENZ",
    "MERCEDESBENZ": "MERCEDES-BENZ",

    "VW": "VOLKSWAGEN",
    "VOLKS": "VOLKSWAGEN",
    "VOLKSW": "VOLKSWAGEN",

    "PORSCH": "PORSCHE",
    "POLEST": "POLESTAR",
    "PEUG": "PEUGEOT",
    "M G": "MG",
    "CHEV": "CHEVROLET",
    "CITRN": "CITROEN",
    "DAIHAT": "DAIHATSU",
    "FERRAR": "FERRARI",
    "G M C": "GMC",
    "G WALL": "GREAT WALL",
    "GENSIS": "GENESIS",
    "HAVAL": "HAVAL"
}

PASSENGER_MAKES = {
    "ABARTH","ALFA ROMEO","ASTON MARTIN","AUDI","BENTLEY","BMW","BYD","CADILLAC",
    "CHERY","CHEVROLET","CITROEN","CUPRA","DODGE","FERRARI","FIAT","FORD","GENESIS",
    "GAC","GEELY","GWM","HOLDEN","HONDA","HYUNDAI","INFINITI","ISUZU UTE","JAGUAR",
    "JEEP","KIA","LAND ROVER","LEXUS","LOTUS","MASERATI","MAHINDRA","MAZDA","MCLAREN",
    "MERCEDES-BENZ","MG","MINI","MITSUBISHI","NISSAN","PEUGEOT","POLESTAR","PORSCHE",
    "RENAULT","ROLLS-ROYCE","SKODA","SUBARU","SUZUKI","TESLA","TOYOTA","VOLKSWAGEN",
    "VOLVO","CHEVROLET","CITROEN","DAIHATSU","FERRARI","GMC","GREAT WALL","GENESIS","HAVAL",

    # EV-only / NEV brands
    "XPENG","ZEEKR","NIO","LUCID","RIVIAN","LEAPMOTOR","SKYWORTH","DENZA","DEEPAL",
    "FARIZON","JAECOO","KGM","OMODA"
}

def normalize_make(m):
    if not isinstance(m, str):
        return ""
    m = m.strip().upper()
    return MAKE_CORRECTIONS.get(m, m)


# ============================================================
# 2. MODEL NORMALIZATION
# ============================================================

def normalize_spaces(text):
    return re.sub(r"\s+", " ", text).strip()

def remove_hyphen_spaces(text):
    return re.sub(r"\s*-\s*", "-", text)

def fix_ocr(text):
    # Replace zero with O only when surrounded by letters
    text = re.sub(r"(?<=[A-Z])0(?=[A-Z])", "O", text)
    # Replace O with 0 when surrounded by digits
    text = re.sub(r"(?<=\d)O(?=\d)", "0", text)
    return text

def collapse_variants(model):
    m = model.upper()

    # FORD EVEREST family (avoid EV false positives)
    if re.fullmatch(r"(EVER(AS|ER|ES|RE|RS|SE|ST|TE)?|EVRRES|EVREST)", m):
        return "EVEREST"


    # OUTLANDER family
    if re.fullmatch(r"(OUTL(A|E)N(D|R)?|OUTLA|OUTLAD|OUTLAN|OUTLAND)", m):
        return "OUTLANDER"

    # TIGGO family
    if re.fullmatch(r"(TIGGO|TIGG0|TIGO|TIGOG|TIGOO)", m):
        return "TIGGO"

    # KAROQ family
    if re.fullmatch(r"(KAROQ|KAROA|KARQ|KAROK|KARO)", m):
        return "KAROQ"

    # HILUX family
    if re.fullmatch(r"(HILUX|HI LUX|HILUS|HILX|HILIUX|HILUC|HILUXT|HILYX)", m):
        return "HILUX"

    # RAV4 family
    if re.fullmatch(r"(RAV4|RAV 4|RAVE 4|RAV4C|RAV4CR)", m):
        return "RAV4"

    # CX-5 family
    if re.fullmatch(r"(CX5|CX 5|CX-5)", m):
        return "CX-5"

    # E-TRON family
    if re.fullmatch(r"(E TRON|ETRON|E-TRON|E TRN|ETRONG)", m):
        return "E-TRON"

    return model


EV_REPLACEMENTS = {
    "Q4ETRO": "Q4 E-TRON",
    "Q4 E T": "Q4 E-TRON",
    "Q4 E-T": "Q4 E-TRON",
    "Q4 ETR": "Q4 E-TRON",
    "RS ETR": "RS E-TRON",
    "RSETRO": "RS E-TRON",
    "500E T": "500E",
    "EV 500": "500E",
    "IONIC": "IONIQ",
    "IONOQ": "IONIQ",
    "IONIQ5": "IONIQ 5",
    "IONIQ6": "IONIQ 6",
    "EX30 E": "EX30",
    "EX30 T": "EX30",
    "EX30CC": "EX30",
}

def normalize_model(model):
    if not isinstance(model, str):
        return ""

    model = model.upper().strip()
    model = normalize_spaces(model)
    model = remove_hyphen_spaces(model)
    model = fix_ocr(model)

    if model in EV_REPLACEMENTS:
        model = EV_REPLACEMENTS[model]

    model = collapse_variants(model)
    return model


# ============================================================
# 3. EV CLASSIFICATION
# ============================================================

BEV_BRANDS = {
    "TESLA", "POLESTAR", "DEEPAL", "DENZA", "XPENG", "CHERY", "CUPRA",
    "NIO", "ZEEKR", "LUCID", "RIVIAN", "BYD", "LEAPMOTOR", "SKYWORTH",
    "FARIZON", "JAECOO", "KGM", "OMODA"
}

BEV_MARKERS = {
    "EV", "BEV", "E-TRON", "ETRON", "IONIQ",
    "LIGHTNING", "MACH-E", "MACH E",
    "ID.", "ID ",   # VW ID.3 / ID.4
    "BORN", "EX30", "EX40", "C40",
    "MG4", "ZS EV",
    "LEAF", "ARIYA",
    "BZ4X",
    "EV6", "EV9", "EV5",
    "I4", "I5", "I7", "IX", "IX1", "IX3", "IX5",
    "SEAL", "DOLPHIN", "ATTO 3",
    "ORA",
    "RZ", "UX300E",
    "EQE", "EQA", "EQB", "EQC", "EQV"
}

PHEV_MARKERS = {"PHEV", "PLUG"}
PHEV_PATTERNS = [
    r"PHEV",
    r"PLUG[\s-]?IN",
    r"TFSI\s*E",
    r"e-?HYBRID",
    r"XDRIVE\s*\d+E",
    r"\d+E\b",
    r"RECHARGE",
    r"TSI\s*E",
]
HEV_MARKERS = {"HYBRID", "HEV", "E-POWER", "EPOWER", "MILD HYBRID", "SHVS", "E-TECH"}

def classify_ev(make, model):
    make = make.upper()
    model = model.upper()

        # Ford Everest is ICE (avoid EV false positives)
    if make == "FORD" and model.startswith("EVER"):
        return "ICE"


    # PHEV first (to avoid misclassifying as BEV)
    for pattern in PHEV_PATTERNS:
        if re.search(pattern, model):
            return "PHEV"

    if make in BEV_BRANDS:
        return "BEV"

    if any(marker in model for marker in BEV_MARKERS):
        return "BEV"

    if any(marker in model for marker in PHEV_MARKERS):
        return "PHEV"

    if any(marker in model for marker in HEV_MARKERS):
        return "HEV"

    return "ICE"


# ============================================================
# 4. MAIN PIPELINE
# ============================================================

df = pd.read_csv(INPUT_FILE, dtype=str)

# Validate required columns
required_cols = {"CD_MAKE_VEH", "CD_MODEL_VEH"}
if not required_cols.issubset(df.columns):
    missing = required_cols - set(df.columns)
    raise ValueError(f"Missing required columns in {INPUT_FILE}: {missing}")

# Create raw fields from input columns
df["make_raw"] = df["CD_MAKE_VEH"].astype(str).str.upper().str.strip()
df["model_raw"] = df["CD_MODEL_VEH"].astype(str).str.upper().str.strip()

# Apply normalization (with fillna safety)
df["make_standardized"] = df["make_raw"].fillna("").apply(normalize_make)
df["model_standardized"] = df["model_raw"].fillna("").apply(normalize_model)

# Filter to passenger cars
df = df[df["make_standardized"].isin(PASSENGER_MAKES)].copy()

# EV classification
df["ev_category"] = df.apply(
    lambda r: classify_ev(r["make_standardized"], r["model_standardized"]),
    axis=1
)

# Optional: flag ICE rows for later audit (if you want to inspect them)
# df["ev_flagged"] = df["ev_category"].apply(lambda x: "check" if x == "ICE" else "")


# ============================================================
# 5. OUTPUT LOOKUP TABLES
# ============================================================

# 1. Make map
make_map = (
    df[["make_raw", "make_standardized"]]
    .drop_duplicates()
    .sort_values("make_standardized")
)
make_map.to_csv(f"{OUTPUT_DIR}/lk_make_map.csv", index=False)

# 2. Model map
model_map = (
    df[["make_standardized", "model_raw", "model_standardized"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)
model_map.to_csv(f"{OUTPUT_DIR}/lk_model_map.csv", index=False)

# 3. EV model map
ev_map = (
    df[["make_standardized", "model_standardized", "ev_category"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)
ev_map.to_csv(f"{OUTPUT_DIR}/lk_ev_model_map.csv", index=False)

# Basic EV summary
print("\nEV category distribution (after passenger-car filter):")
print(df["ev_category"].value_counts(dropna=False))

print("\n✔ Lookup tables generated in seeds_stage2/")
print("   - lk_make_map.csv")
print("   - lk_model_map.csv")
print("   - lk_ev_model_map.csv")
