import pandas as pd
import re

# ---------------------------------------------
# NORMALIZATION HELPERS
# ---------------------------------------------

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


def normalize_model(model):
    if pd.isna(model) or model.strip() == "":
        return ""

    model = model.upper()
    model = normalize_spaces(model)
    model = remove_hyphen_spaces(model)
    model = fix_ocr(model)
    model = collapse_variants(model)
    return model


# ---------------------------------------------
# EV CLASSIFICATION
# ---------------------------------------------

BEV_LIST = {
    "MODEL 3", "MODEL Y", "E-TRON", "IONIQ5", "IONIQ6", "EV6", "EV9",
    "BORN", "ATTO 3", "ZS EV", "MX-30", "LEAF", "POLESTAR 2", "EX30",
    "TESLA 3", "TESLA Y"
}

PHEV_LIST = {
    "OUTLANDER PHEV", "RAV4 PHEV", "TIGGO 8 PHEV", "XC60 PHEV",
    "XC90 PHEV", "E-POWER", "Q5 TFSI E"
}

HEV_LIST = {
    "RAV4 HYBRID", "COROLLA HYBRID", "CAMRY HYBRID", "CR-V HYBRID",
    "HR-V HYBRID", "YARIS HYBRID", "C-HR HYBRID"
}

def classify_ev(model):
    m = model.upper()

    # Exact match
    if m in BEV_LIST:
        return "BEV"
    if m in PHEV_LIST:
        return "PHEV"
    if m in HEV_LIST:
        return "HEV"

    # Pattern-based inference
    if "EV" in m and not any(x in m for x in ["PHEV", "HEV"]):
        return "BEV"
    if "PHEV" in m:
        return "PHEV"
    if "HYBRID" in m or "HEV" in m:
        return "HEV"

    return "ICE"


# ---------------------------------------------
# MAIN PROCESS
# ---------------------------------------------

def process_lk_model_map(input_csv):
    df = pd.read_csv(input_csv)

    # Normalize
    df["make_standardized"] = df["make_standardized"].astype(str).str.upper().apply(normalize_spaces)
    df["model_raw"] = df["model_raw"].astype(str).apply(normalize_model)
    df["model_standardized"] = df["model_standardized"].astype(str).apply(normalize_model)

    # If model_standardized empty → use normalized model_raw
    df["model_standardized"] = df.apply(
        lambda r: r["model_standardized"] if r["model_standardized"] else r["model_raw"],
        axis=1
    )

    # Deduplicate
    df = df.drop_duplicates(subset=["make_standardized", "model_standardized"])

    # Save normalized file
    df.to_csv("lk_model_map_clean.csv", index=False)

    # Build EV classification file
    ev_df = df.copy()
    ev_df["ev_category"] = ev_df["model_standardized"].apply(classify_ev)
    ev_df.to_csv("lk_ev_classification.csv", index=False)

    print("✔ Normalized file saved: lk_model_map_clean.csv")
    print("✔ EV classification file saved: lk_ev_classification.csv")


# ---------------------------------------------
# RUN
# ---------------------------------------------
if __name__ == "__main__":
    process_lk_model_map("./seeds_stage1/lk_model_map.csv")
