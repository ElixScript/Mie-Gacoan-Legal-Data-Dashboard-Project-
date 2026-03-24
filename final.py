import pandas as pd
import re
from pathlib import Path

# =========================
# PATH
# =========================
INPUT_PATH = Path("data/processed/cleaned_gacoan_legal_analytics_data.csv")
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "data_cleaned_akhir.csv"

# =========================
# LOAD
# =========================
df = pd.read_csv(INPUT_PATH)

# =========================
# LIST PROVINSI
# =========================
PROVINSI_LIST = [
    "aceh","sumatera utara","sumatera barat","riau","kepulauan riau","jambi",
    "sumatera selatan","bengkulu","lampung","bangka belitung","banten",
    "dki jakarta","jakarta","jawa barat","jawa tengah","jawa timur",
    "di yogyakarta","yogyakarta","bali","nusa tenggara barat","nusa tenggara timur",
    "kalimantan barat","kalimantan tengah","kalimantan selatan","kalimantan timur","kalimantan utara",
    "sulawesi utara","sulawesi tengah","sulawesi selatan","sulawesi tenggara","gorontalo","sulawesi barat",
    "maluku","maluku utara","papua","papua barat","papua barat daya","papua selatan","papua tengah","papua pegunungan"
]

# =========================
# CLEAN TEXT
# =========================
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r'[^a-z\s]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

# =========================
# EXTRACT
# =========================
def extract_provinsi(alamat):
    if pd.isna(alamat):
        return None

    text = clean_text(alamat)

    found = []
    for prov in PROVINSI_LIST:
        if prov in text:
            found.append(prov)

    if not found:
        return None

    # ambil yang paling belakang
    found_sorted = sorted(found, key=lambda x: text.rfind(x))
    return found_sorted[-1].title()

# =========================
# APPLY DENGAN FALLBACK
# =========================
df["Provinsi_Extracted"] = df["Alamat"].apply(extract_provinsi)

# 👉 fallback: kalau hasil None → pakai provinsi lama
df["Provinsi"] = df["Provinsi_Extracted"].combine_first(df["Provinsi"])

# =========================
# SAVE
# =========================
df.to_csv(OUTPUT_PATH, index=False)

print("Done. File saved to:", OUTPUT_PATH)