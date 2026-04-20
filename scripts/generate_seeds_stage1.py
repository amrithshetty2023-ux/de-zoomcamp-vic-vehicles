import pandas as pd
import os

INPUT_FILE = "all.csv"
OUTPUT_DIR = "seeds_stage1"

df = pd.read_csv(INPUT_FILE)

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
    "PORSCH": "PORSCHE",
    "POLEST": "POLESTAR",
    "VOLKSW": "VOLKSWAGEN",
    "PEUG": "PEUGEOT",
    "M G": "MG"
}

def standardize_make(m):
    return MAKE_CORRECTIONS.get(m, m)

df["make_standardized"] = df["make_raw"].apply(standardize_make)

PASSENGER_MAKES = {
    "ABARTH","ALFA ROMEO","ASTON MARTIN","AUDI","BENTLEY","BMW","BYD","CADILLAC",
    "CHERY","CHEVROLET","CITROEN","CUPRA","DODGE","FERRARI","FIAT","FORD","GENESIS",
    "GAC","GEELY","GWM","HOLDEN","HONDA","HYUNDAI","INFINITI","ISUZU UTE","JAGUAR",
    "JEEP","KIA","LAND ROVER","LEXUS","LOTUS","MASERATI","MAHINDRA","MAZDA","MCLAREN",
    "MERCEDES-BENZ","MG","MINI","MITSUBISHI","NISSAN","PEUGEOT","POLESTAR","PORSCHE",
    "RENAULT","ROLLS-ROYCE","SKODA","SUBARU","SUZUKI","TESLA","TOYOTA","VOLKSWAGEN",
    "VOLVO",

    # EV-only and new-energy brands
    "XPENG","ZEEKR","NIO","LUCID","RIVIAN","LEAPMOTOR","SKYWORTH","DENZA","DEEPAL",
    "FARIZON","JAECOO","KGM", "OMODA"
}

df_pass = df[df["make_standardized"].isin(PASSENGER_MAKES)].copy()

def clean_model(m):
    return (
        m.replace("  ", " ")
         .strip()
    )

df_pass["model_standardized"] = df_pass["model_raw"].apply(clean_model)

os.makedirs(OUTPUT_DIR, exist_ok=True)

make_map = (
    df_pass[["make_raw", "make_standardized"]]
    .drop_duplicates()
    .sort_values("make_standardized")
)
make_map.to_csv(f"{OUTPUT_DIR}/lk_make_map.csv", index=False)

model_map = (
    df_pass[["make_standardized", "model_raw", "model_standardized"]]
    .drop_duplicates()
    .sort_values(["make_standardized", "model_standardized"])
)
model_map.to_csv(f"{OUTPUT_DIR}/lk_model_map.csv", index=False)

print("Generated:", f"{OUTPUT_DIR}/lk_make_map.csv")
print("Generated:", f"{OUTPUT_DIR}/lk_model_map.csv")
