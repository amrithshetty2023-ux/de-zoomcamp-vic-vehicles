import pandas as pd
import re

# ============================================================
# 1. Load clean make/model extract
# ============================================================
df = pd.read_csv("all_make_models.csv")

df.columns = ["make_raw", "model_raw"]
OUTPUT_DIR = "seeds_stage2"


# ============================================================
# 2. Passenger‑car makes (your curated list)
# ============================================================
PASSENGER_CAR_MAKES = {
    # Mainstream
    "TOYOTA", "MAZDA", "HYUNDAI", "KIA", "FORD", "HONDA", "NISSAN",
    "VOLKSWAGEN", "SUBARU", "SUZUKI", "MITSUBISHI",

    # Luxury / Performance
    "BMW", "MERCEDES-BENZ", "AUDI", "LEXUS", "PORSCHE", "VOLVO",
    "JAGUAR", "LAND ROVER", "TESLA",

    # New EV brands
    "BYD", "MG", "POLESTAR", "CUPRA",

    # SUV / UTE passenger brands
    "ISUZU", "JEEP", "RAM", "LDV", "GWM", "HAVAL",

    # Small performance brands
    "ABARTH", "ALFA ROMEO", "MINI"
}


# ============================================================
# 3. Make normalisation rules
# ============================================================
MAKE_CORRECTIONS = {
    r"^B M W$": "BMW",
    r"^B Y D$": "BYD",
    r"^MERC.*": "MERCEDES-BENZ",
    r"^VW$": "VOLKSWAGEN",
    r"^GREAT WALL$": "GWM",
    r"^HAVAL.*": "HAVAL",
}


def normalize_make(make):
    if pd.isna(make):
        return None
    m = make.strip().upper()
    for pattern, replacement in MAKE_CORRECTIONS.items():
        if re.match(pattern, m):
            return replacement
    return m


df["make_standardized"] = df["make_raw"].apply(normalize_make)


# ============================================================
# 4. Filter to passenger‑car makes only
# ============================================================
df = df[df["make_standardized"].isin(PASSENGER_CAR_MAKES)].copy()


# ============================================================
# 5. Model normalisation rules
# ============================================================
def normalize_model(model):
    if pd.isna(model):
        return None
    m = model.upper().strip()

    # collapse multiple spaces
    m = re.sub(r"\s+", " ", m)

    # remove trailing punctuation
    m = m.replace(".", "").replace(",", "")

    # fix common OCR issues
    m = m.replace("0", "O") if re.match(r".*[A-Z]+0[A-Z]+.*", m) else m

    return m


df["model_standardized"] = df["model_raw"].apply(normalize_model)


# ============================================================
# 6. EV classification (Zecar‑aligned)
# ============================================================
BEV_MODELS = {
    "MODEL 3", "MODEL Y", "ATTO 3", "SEAL", "SEALION 6", "MG4",
    "POLESTAR 2", "EQA", "EQB", "EQE", "EQC", "500E"
}

PHEV_MODELS = {
    "OUTLANDER PHEV", "XC60 RECHARGE", "XC90 RECHARGE"
}

HEV_MODELS = {
    "RAV4 HYBRID", "COROLLA HYBRID", "CAMRY HYBRID",
    "C-HR HYBRID", "YARIS HYBRID"
}


def classify_ev(model):
    if model in BEV_MODELS:
        return "BEV"
    if model in PHEV_MODELS:
        return "PHEV"
    if model in HEV_MODELS:
        return "HEV"
    return "ICE"


df["ev_category"] = df["model_standardized"].apply(classify_ev)


# ============================================================
# 7. Export lookup tables
# ============================================================

# 1. Make map
df_make = (
    df[["make_raw", "make_standardized"]]
    .drop_duplicates()
    .sort_values("make_standardized")
)
df_make.to_csv("lk_make_map.csv", index=False)

# 2. Model map
df_model = (
    df[["make_standardized", "model_raw", "model_standardized"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)
df_model.to_csv("lk_model_map.csv", index=False)

# 3. EV model map
df_ev = (
    df[["make_standardized", "model_standardized", "ev_category"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)
df_ev.to_csv("lk_ev_model_map.csv", index=False)

print("Lookup tables generated:")
print(" - lk_make_map.csv")
print(" - lk_model_map.csv")
print(" - lk_ev_model_map.csv")
