import pandas as pd
import os

# -----------------------------
# 1. CONFIG
# -----------------------------

INPUT_FILE = "all.csv"
OUTPUT_DIR = "seeds_stage1"

# Passenger + SUV + luxury makes
PASSENGER_MAKES = [
    "ABARTH","ALFA ROMEO","ASTON MARTIN","AUDI","BENTLEY","BMW","BYD","CADILLAC",
    "CHEVROLET","CHERY","CITROEN","CUPRA","DODGE","FERRARI","FIAT","FORD","GENESIS",
    "GWM","HOLDEN","HONDA","HYUNDAI","INFINITI","ISUZU UTE","JAGUAR","JEEP","KIA",
    "LAND ROVER","LEXUS","LOTUS","MASERATI","MAZDA","MCLAREN","MERCEDES-BENZ",
    "MG","MINI","MITSUBISHI","NISSAN","PEUGEOT","PORSCHE","POLESTAR","RENAULT",
    "ROLLS-ROYCE","SKODA","SUBARU","SUZUKI","TESLA","TOYOTA","VOLKSWAGEN","VOLVO"
]

# -----------------------------
# 2. LOAD RAW DATA
# -----------------------------

df = pd.read_csv(INPUT_FILE)

# -----------------------------
# 3. NORMALIZATION HELPERS
# -----------------------------

def clean(text):
    return (
        str(text)
        .strip()
        .upper()
        .replace("\t", " ")
        .replace("\n", " ")
    )

df["make_raw"] = df["CD_MAKE_VEH"].apply(clean)
df["model_raw"] = df["CD_MODEL_VEH"].apply(clean)

# -----------------------------
# 4. MAKE STANDARDIZATION
# -----------------------------

# Basic corrections for common abbreviations
MAKE_CORRECTIONS = {
    "ALFA R": "ALFA ROMEO",
    "ASTON": "ASTON MARTIN",
    "MERC BENZ": "MERCEDES-BENZ",
    "VW": "VOLKSWAGEN",
    "AUDI ": "AUDI",
}

def standardize_make(m):
    if m in MAKE_CORRECTIONS:
        return MAKE_CORRECTIONS[m]
    return m

df["make_standardized"] = df["make_raw"].apply(standardize_make)

# -----------------------------
# 5. FILTER TO PASSENGER MAKES
# -----------------------------

df_pass = df[df["make_standardized"].isin(PASSENGER_MAKES)].copy()

# -----------------------------
# 6. GENERATE lk_make_map.csv
# -----------------------------

make_map = (
    df_pass[["make_raw", "make_standardized"]]
    .drop_duplicates()
    .sort_values("make_standardized")
)

os.makedirs(OUTPUT_DIR, exist_ok=True)
make_map.to_csv(f"{OUTPUT_DIR}/lk_make_map.csv", index=False)

print("Generated:", f"{OUTPUT_DIR}/lk_make_map.csv")

# -----------------------------
# 7. GENERATE lk_model_map.csv
# -----------------------------

# Model standardization (light touch for Stage 1)
def clean_model(m):
    return (
        m.replace("  ", " ")
         .replace("-", " ")
         .strip()
    )

df_pass["model_standardized"] = df_pass["model_raw"].apply(clean_model)

model_map = (
    df_pass[["make_standardized", "model_raw", "model_standardized"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)

model_map.to_csv(f"{OUTPUT_DIR}/lk_model_map.csv", index=False)

print("Generated:", f"{OUTPUT_DIR}/lk_model_map.csv")

print("\nStage 1 complete — inspect the seeds and confirm they look correct.")
