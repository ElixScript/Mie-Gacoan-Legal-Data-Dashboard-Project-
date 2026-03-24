import pandas as pd
import re
from pathlib import Path

# =========================
# 1. Path Setup
# =========================
INPUT_PATH = Path("data/processed/cleaned_gacoan_legal_analytics_data.csv")  # ganti sesuai file kamu
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "data_cleaned_akhir.csv"

# =========================
# 2. Load Data
# =========================
df = pd.read_csv(INPUT_PATH)

# =========================
# 3. Regex Provinsi
# =========================
pattern = re.compile(r'''(?i)\b(
aceh|
sumatera utara|sumut|
sumatera barat|sumbar|
riau|
kepulauan riau|kepri|
jambi|
sumatera selatan|sumsel|
bengkulu|
lampung|
bangka belitung|
banten|
dki jakarta|jakarta|
jawa barat|jabar|
jawa tengah|jateng|
di yogyakarta|yogyakarta|diy|
jawa timur|jatim|
bali|
nusa tenggara barat|ntb|
nusa tenggara timur|ntt|
kalimantan barat|kalbar|
kalimantan tengah|kalteng|
kalimantan selatan|kalsel|
kalimantan timur|kaltim|
kalimantan utara|kaltara|
sulawesi utara|sulut|
sulawesi tengah|sulteng|
sulawesi selatan|sulsel|
sulawesi tenggara|sultra|
gorontalo|
sulawesi barat|sulbar|
maluku|
maluku utara|
papua|
papua barat|
papua barat daya|
papua selatan|
papua tengah|
papua pegunungan
)\b''', re.VERBOSE)

# =========================
# 4. Extract Function
# =========================
def extract_provinsi(alamat):
    if pd.isna(alamat):
        return None
    
    matches = pattern.findall(alamat)
    return matches[-1].title() if matches else None  # ambil paling belakang

# =========================
# 5. Apply
# =========================
df["Provinsi_Extracted"] = df["Alamat"].apply(extract_provinsi)

# Replace jika hasil ekstraksi ada
df["Provinsi_Final"] = df["Provinsi_Extracted"].combine_first(df["Provinsi"])

# =========================
# 6. Debug (optional tapi penting)
# =========================
changed = df[df["Provinsi"] != df["Provinsi_Final"]]
print(f"Jumlah data diperbaiki: {len(changed)}")

# =========================
# 7. Save Output
# =========================
df.to_csv(OUTPUT_PATH, index=False)

print(f"File berhasil disimpan di: {OUTPUT_PATH}")