import pandas as pd
import re

# ============================================================
# 1. Intelligent Normalization
# ============================================================

def normalize_model(s: str) -> str:
    if not isinstance(s, str):
        return ""

    s = s.strip().upper()
    s = re.sub(r"\s+", " ", s)              # collapse spaces
    s = re.sub(r"\s*-\s*", "-", s)          # normalize hyphens

    # OCR fixes
    s = s.replace("0", "O") if re.search(r"[A-Z]0[A-Z]", s) else s
    s = s.replace("1", "I") if re.search(r"[A-Z]1[A-Z]", s) else s

    # Collapse known EV spelling variants
    replacements = {
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

    if s in replacements:
        s = replacements[s]

    return s


# ============================================================
# 2. EV Classification Logic (Zecar‑aligned)
# ============================================================

BEV_BRANDS = {
    "TESLA", "POLESTAR", "DEEPAL", "DENZA", "XPENG",
    "NIO", "ZEEKR", "LUCID", "RIVIAN"
}

def classify_ev(make, model):
    make = make.upper()
    model = model.upper()

    # --- Rule 1: Brand is always BEV ---
    if make in BEV_BRANDS:
        return "BEV"

    # --- Rule 2: Model contains BEV markers ---
    bev_markers = [
        "EV", "BEV", "E-TRON", "ETRON", "IONIQ", "LIGHTN", "MACH E",
        "500E", "BORN", "IX", "I4", "I5", "I7",
        "EX30", "EX40", "C40", "XC40 R",
        "ZS EV", "MG4", "LEAF", "ARIYA",
        "BZ4X", "MIRAI",
        "NIRO E", "EV6", "EV9", "EV5", "EV3",
        "RZ", "UX300E"
    ]

    if any(marker in model for marker in bev_markers):
        return "BEV"

    # --- Rule 3: PHEV markers ---
    if "PHEV" in model or "PLUG" in model:
        return "PHEV"

    # --- Rule 4: HEV markers ---
    hev_markers = ["HYB", "HEV", "HYBRID", "E-POWER"]
    if any(marker in model for marker in hev_markers):
        return "HEV"

    # --- Default ---
    return "ICE"


# ============================================================
# 3. Main Processing Pipeline
# ============================================================

def process_ev_lookup(input_csv, output_csv):
    df = pd.read_csv(input_csv, dtype=str)

    # Normalize make + model
    df["make_standardized"] = df["make_standardized"].str.upper().str.strip()
    df["model_standardized"] = df["model_standardized"].fillna("").apply(normalize_model)

    # Classify EV category
    df["ev_category"] = df.apply(
        lambda r: classify_ev(r["make_standardized"], r["model_standardized"]),
        axis=1
    )

    # Deduplicate
    df = df.drop_duplicates(
        subset=["make_standardized", "model_standardized"]
    ).sort_values(["make_standardized", "model_standardized"])

    df.to_csv(output_csv, index=False)
    print(f"✔ Clean EV lookup written to {output_csv}")


# ============================================================
# 4. Run
# ============================================================

if __name__ == "__main__":
    process_ev_lookup(
        input_csv="lk_ev_classification.csv",
        output_csv="lk_ev_classification_clean.csv"
    )
