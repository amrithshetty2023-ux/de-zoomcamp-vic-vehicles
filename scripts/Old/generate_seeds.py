import pandas as pd
import os

# -----------------------------
# 1. CONFIG
# -----------------------------

INPUT_FILE = "all.csv"          # Your VIC dataset
OUTPUT_DIR = "seeds"               # Where CSVs will be saved

# Ensure output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Updated EV-capable + passenger-car makes (your combined list)
PASSENGER_MAKES = [
    "ALFA ROMEO","AUDI","BMW","BYD","CADILLAC","CHERY","CUPRA","DEEPAL","DENZA",
    "FARIZON","FIAT","FORD","GAC","GEELY","GENESIS","GWM","HYUNDAI","JAECOO",
    "JEEP","KGM","KIA","LDV","LEAPMOTOR","LEXUS","LOTUS","MAZDA","MERCEDES-BENZ",
    "MG","MINI","NISSAN","PEUGEOT","POLESTAR","PORSCHE","RENAULT","ROLLS-ROYCE",
    "SKODA","SKYWORTH","SMART","SUBARU","TESLA","TOYOTA","VOLKSWAGEN","VOLVO",
    "XPENG","ZEEKR"
]

# -----------------------------
# 2. LOAD RAW DATA
# -----------------------------

df = pd.read_csv(INPUT_FILE)

# -----------------------------
# 3. NORMALIZE MAKE + MODEL
# -----------------------------

def normalize(text):
    return (
        str(text)
        .strip()
        .upper()
        .replace("\t", " ")
        .replace("\n", " ")
    )

df["CD_MAKE_VEH"] = df["CD_MAKE_VEH"].apply(normalize)
df["CD_MODEL_VEH"] = df["CD_MODEL_VEH"].apply(normalize)

# -----------------------------
# 4. FILTER TO PASSENGER-CAR MAKES ONLY
# -----------------------------

df_pass = df[df["CD_MAKE_VEH"].isin(PASSENGER_MAKES)].copy()

# -----------------------------
# 5. GENERATE lk_make_map.csv
# -----------------------------

make_map = (
    df_pass[["CD_MAKE_VEH"]]
    .drop_duplicates()
    .rename(columns={"CD_MAKE_VEH": "make_standardized"})
)

make_map["make_raw"] = make_map["make_standardized"]
make_map = make_map[["make_raw", "make_standardized"]]

make_map.to_csv(f"{OUTPUT_DIR}/lk_make_map.csv", index=False)

print(f"Generated: {OUTPUT_DIR}/lk_make_map.csv")

# -----------------------------
# 6. GENERATE lk_model_map.csv
# -----------------------------

model_map = (
    df_pass[["CD_MAKE_VEH", "CD_MODEL_VEH"]]
    .drop_duplicates()
    .rename(columns={
        "CD_MAKE_VEH": "make_standardized",
        "CD_MODEL_VEH": "model_raw"
    })
)

model_map["model_standardized"] = model_map["model_raw"]

model_map = model_map[[
    "make_standardized",
    "model_raw",
    "model_standardized"
]]

model_map.to_csv(f"{OUTPUT_DIR}/lk_model_map.csv", index=False)

print(f"Generated: {OUTPUT_DIR}/lk_model_map.csv")

# -----------------------------
# 7. GENERATE lk_ev_model_map.csv
# -----------------------------

# All passenger makes are EV-capable (your rule)
model_map["ev_category"] = model_map["make_standardized"].apply(
    lambda m: "EV" if m in PASSENGER_MAKES else "UNKNOWN"
)

ev_map = model_map[[
    "make_standardized",
    "model_standardized",
    "ev_category"
]].drop_duplicates()

ev_map.to_csv(f"{OUTPUT_DIR}/lk_ev_model_map.csv", index=False)

print(f"Generated: {OUTPUT_DIR}/lk_ev_model_map.csv")

# -----------------------------
# DONE
# -----------------------------

print("\nAll seed files generated successfully!")
