"""
FASE 2 — Data Engineering
Tujuan : Baca detail cabang dari Fase 1, lalu tambahkan kolom-kolom legal
         compliance dengan distribusi probabilitas yang realistis.
         Juga extract Kota/Kabupaten dari kolom alamat via regex.
         Terakhir, inject "dirty data" untuk praktik cleaning di Fase 3.
Input  : data/raw/mie_gacoan_all_details.json
Output :
         data/processed/gacoan_legal_analytics_data.csv       ← versi bersih (referensi)
         data/processed/dirty_gacoan_legal_analytics_data.csv ← versi kotor (bahan cleaning)
"""

import json
import re
import random
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────
RAW_FILE      = Path("data/raw/mie_gacoan_all_details.json")
OUTPUT_DIR    = Path("data/processed")
CLEAN_FILE    = OUTPUT_DIR / "gacoan_legal_analytics_data.csv"
DIRTY_FILE    = OUTPUT_DIR / "dirty_gacoan_legal_analytics_data.csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)   # reproducible


# ─────────────────────────────────────────────
#  Step 1: Ekstraksi Kota / Kabupaten dari alamat
# ─────────────────────────────────────────────

DKI_PATTERNS: dict[str, str] = {
    r"Jkt\s*Utara|Jakarta\s*Utara|North\s*Jakarta":   "Kota Jakarta Utara",
    r"Jkt\s*Selatan|Jakarta\s*Selatan|South\s*Jakarta": "Kota Jakarta Selatan",
    r"Jkt\s*Barat|Jakarta\s*Barat|West\s*Jakarta":     "Kota Jakarta Barat",
    r"Jkt\s*Timur|Jakarta\s*Timur|East\s*Jakarta":     "Kota Jakarta Timur",
    r"Jkt\s*Pusat|Jakarta\s*Pusat|Central\s*Jakarta":  "Kota Jakarta Pusat",
}


def extract_kota_kabupaten(alamat: str | None, provinsi: str | None) -> str | None:
    """Ekstrak nama kota/kabupaten dari string alamat Google Maps."""
    if not alamat or not isinstance(alamat, str):
        return None

    # 1. "Kota X" atau "Kabupaten X" (penulisan lengkap)
    m = re.search(r'\b(Kota|Kabupaten)\s+([A-Za-z\s]+?)(?=,|$|\d)', alamat)
    if m:
        return f"{m.group(1)} {m.group(2).strip().title()}"

    # 2. "Kab. X" (singkatan)
    m = re.search(r'\bKab\.\s+([A-Za-z\s]+?)(?=,|$|\d)', alamat)
    if m:
        return f"Kabupaten {m.group(1).strip().title()}"

    # 3. "X City" (format bahasa Inggris dari Google Maps)
    m = re.search(r'([A-Za-z\s]+?)\s+City\b', alamat)
    if m:
        return f"Kota {m.group(1).strip().title()}"

    # 4. Pola khusus DKI Jakarta
    if provinsi and "Jakarta" in provinsi:
        for pattern, result in DKI_PATTERNS.items():
            if re.search(pattern, alamat, re.IGNORECASE):
                return result

    return None


# ─────────────────────────────────────────────
#  Step 2: Generate kolom-kolom legal
# ─────────────────────────────────────────────

HALAL_STATUS_CHOICES   = ["Tersertifikasi", "Dalam Proses", "Belum Tersertifikasi"]
HALAL_STATUS_WEIGHTS   = [70, 20, 10]

IMB_STATUS_CHOICES     = ["Approved", "In Progress", "Bermasalah"]
IMB_STATUS_WEIGHTS     = [80, 15, 5]

DISPUTE_LEVEL_CHOICES  = ["Low", "Medium", "High (Eskalasi)"]
DISPUTE_LEVEL_WEIGHTS  = [70, 20, 10]

SENGKETA_DESKRIPSI_MAP = {
    "Low":            ["Tidak Ada Sengketa", "Sengketa Kecil Sudah Selesai"],
    "Medium":         ["Negosiasi Berjalan", "Mediasi Aktif"],
    "High (Eskalasi)": ["Menunggu Audit", "Gugatan Aktif", "Perlu Keputusan Direksi"],
}


def generate_legal_row(idx: int, detail: dict) -> dict:
    """Buat satu baris data legal untuk sebuah cabang."""
    alamat  = detail.get("alamat") or ""
    provinsi = detail.get("provinsi") or detail.get("provinsi_query") or ""

    # ── Tanggal sewa ──────────────────────────────────
    days_from_now = random.randint(-15, 1000)   # -15 berarti sudah kadaluarsa
    sewa_berakhir = datetime.now() + timedelta(days=days_from_now)
    sisa_hari     = days_from_now

    # ── Status ───────────────────────────────────────
    imb_status    = random.choices(IMB_STATUS_CHOICES,    weights=IMB_STATUS_WEIGHTS)[0]
    halal_status  = random.choices(HALAL_STATUS_CHOICES,  weights=HALAL_STATUS_WEIGHTS)[0]
    dispute_level = random.choices(DISPUTE_LEVEL_CHOICES, weights=DISPUTE_LEVEL_WEIGHTS)[0]

    deskripsi_sengketa = random.choice(SENGKETA_DESKRIPSI_MAP[dispute_level])

    # ── Tanggal halal (masa berlaku 2 tahun dari acak) ────────────
    halal_start = datetime.now() - timedelta(days=random.randint(0, 700))
    halal_exp   = halal_start + timedelta(days=730)   # 2 tahun

    return {
        "ID_Cabang":           f"GCN-{idx:04d}",
        "Nama_Cabang":         detail.get("nama", f"Mie Gacoan Cabang {idx}"),
        "Alamat":              alamat,
        "Kota_Kabupaten":      extract_kota_kabupaten(alamat, provinsi),
        "Provinsi":            provinsi,
        "Rating":              detail.get("rating"),
        "Jumlah_Review":       detail.get("jumlah_review"),
        "Telepon":             detail.get("telepon"),
        "Status_IMB":          imb_status,
        "Sewa_Mulai":          (sewa_berakhir - timedelta(days=random.randint(365, 1825))).strftime("%Y-%m-%d"),
        "Sewa_Berakhir":       sewa_berakhir.strftime("%Y-%m-%d"),
        "Sisa_Hari_Sewa":      sisa_hari,
        "Status_Sertifikat_Halal": halal_status,
        "Halal_Exp":           halal_exp.strftime("%Y-%m-%d"),
        "Tingkat_Sengketa":    dispute_level,
        "Deskripsi_Sengketa":  deskripsi_sengketa,
        "Tanggal_Update":      datetime.now().strftime("%Y-%m-%d"),
        "URL_Maps":            detail.get("url", ""),
    }


# ─────────────────────────────────────────────
#  Step 3: Injeksi Dirty Data
# ─────────────────────────────────────────────

def inject_dirty_data(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Tambahkan berbagai jenis 'kotor' ke salinan dataframe."""
    df = df_clean.copy()

    # 1. Missing values (NaN) di Alamat & Provinsi untuk ~5% baris
    missing_idx = df.sample(frac=0.05, random_state=1).index
    df.loc[missing_idx, ["Alamat", "Provinsi"]] = None

    # 2. Typo di Status_IMB untuk ~10% baris
    typo_imb_idx = df.sample(frac=0.10, random_state=2).index
    df.loc[typo_imb_idx, "Status_IMB"] = (
        df.loc[typo_imb_idx, "Status_IMB"]
        .replace({"Approved": "aprovd", "In Progress": "progres", "Bermasalah": "masalah"})
    )

    # 3. Duplikasi 5 baris acak
    duplicates = df.sample(n=5, random_state=3)
    df = pd.concat([df, duplicates], ignore_index=True)

    # 4. Missing Kota_Kabupaten untuk ~5% baris
    missing_kota_idx = df.sample(frac=0.05, random_state=4).index
    df.loc[missing_kota_idx, "Kota_Kabupaten"] = None

    # 5. Lowercase typo di Kota_Kabupaten untuk ~5% baris
    typo_kota_idx = df.sample(frac=0.05, random_state=5).index
    df.loc[typo_kota_idx, "Kota_Kabupaten"] = (
        df.loc[typo_kota_idx, "Kota_Kabupaten"]
        .str.lower()
        .where(df.loc[typo_kota_idx, "Kota_Kabupaten"].notna(), other=None)
    )

    # 6. Sisa_Hari_Sewa basi (angka lama, bukan dihitung ulang dari hari ini)
    stale_idx = df.sample(frac=0.30, random_state=6).index
    df.loc[stale_idx, "Sisa_Hari_Sewa"] = (
        df.loc[stale_idx, "Sisa_Hari_Sewa"] + random.randint(30, 120)
    )

    return df


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {RAW_FILE}\n"
            "Jalankan dulu: python 01_scrape_raw_locations.py & 02_scrape_all_details.py"
        )

    print(f"📂 Membaca data dari {RAW_FILE} ...")
    with open(RAW_FILE, encoding="utf-8") as f:
        details: list[dict] = json.load(f)

    print(f"   {len(details)} cabang ditemukan.\n")

    # ── Generate data legal ────────────────────────────
    print("⚙️  Generate kolom legal compliance ...")
    rows = [generate_legal_row(i + 1, d) for i, d in enumerate(details)]
    df_clean = pd.DataFrame(rows)

    total_kota = df_clean["Kota_Kabupaten"].notna().sum()
    print(f"   ✓ {total_kota}/{len(df_clean)} cabang berhasil diekstrak Kota/Kabupaten-nya.")

    # Simpan versi bersih
    df_clean.to_csv(CLEAN_FILE, index=False, encoding="utf-8-sig")
    print(f"   ✓ Disimpan: {CLEAN_FILE}  [{df_clean.shape[0]} baris × {df_clean.shape[1]} kolom]")

    # ── Inject dirty data ──────────────────────────────
    print("\n💉 Menyuntikkan dirty data ...")
    df_dirty = inject_dirty_data(df_clean)
    df_dirty.to_csv(DIRTY_FILE, index=False, encoding="utf-8-sig")
    print(f"   ✓ Disimpan: {DIRTY_FILE}  [{df_dirty.shape[0]} baris × {df_dirty.shape[1]} kolom]")

    # ── Ringkasan ─────────────────────────────────────
    print("\n📊 Ringkasan distribusi data bersih:")
    print("   Status IMB:")
    for v, c in df_clean["Status_IMB"].value_counts().items():
        print(f"     {v:<30} {c:>4} cabang")
    print("   Tingkat Sengketa:")
    for v, c in df_clean["Tingkat_Sengketa"].value_counts().items():
        print(f"     {v:<30} {c:>4} cabang")
    critical = (df_clean["Sisa_Hari_Sewa"] < 90).sum()
    print(f"   Sewa kritis (< 90 hari): {critical} cabang")

    print("\n✅ Fase 2 selesai!")


if __name__ == "__main__":
    main()
