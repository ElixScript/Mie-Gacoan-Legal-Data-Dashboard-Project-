"""
FASE 3 — Data Cleaning
Tujuan : Bersihkan dirty_gacoan_legal_analytics_data.csv menjadi dataset
         yang siap dipakai di Google Looker Studio.

Langkah cleaning:
  1. Impor dan inspeksi awal
  2. Hapus baris duplikat
  3. Perbaiki typo di Status_IMB
  4. Tangani nilai kosong (Alamat, Provinsi)
  5. Standardisasi casing Kota_Kabupaten
  6. Hitung ulang Sisa_Hari_Sewa berdasarkan tanggal hari ini
  7. Simpan hasil

Input  : data/processed/dirty_gacoan_legal_analytics_data.csv
Output : data/processed/cleaned_gacoan_legal_analytics_data.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────
INPUT_FILE  = Path("data/processed/dirty_gacoan_legal_analytics_data.csv")
OUTPUT_FILE = Path("data/processed/cleaned_gacoan_legal_analytics_data.csv")

# Mapping typo → nilai yang benar
TYPO_CORRECTION_MAP: dict[str, str] = {
    "aprovd":  "Approved",
    "progres": "In Progress",
    "masalah": "Bermasalah",
    # Tambahkan pola lain yang ditemukan di lapangan di sini
}


# ─────────────────────────────────────────────
#  Fungsi-fungsi cleaning
# ─────────────────────────────────────────────

def log(step: int, message: str, before: int | None = None, after: int | None = None) -> None:
    """Cetak log pembersihan dengan perubahan jumlah baris."""
    prefix = f"  [Step {step}]"
    if before is not None and after is not None:
        delta = before - after
        status = f"✓ Dihapus {delta} baris ({before} → {after})" if delta > 0 \
                 else "✓ Tidak ada perubahan jumlah baris"
        print(f"{prefix} {message}: {status}")
    else:
        print(f"{prefix} {message}")


def step1_load_and_inspect(path: Path) -> pd.DataFrame:
    """Impor CSV dan tampilkan ringkasan awal."""
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"  [Step 1] Data dimuat: {df.shape[0]} baris × {df.shape[1]} kolom")
    print(f"           Kolom: {list(df.columns)}\n")
    print("           Nilai kosong per kolom:")
    null_counts = df.isnull().sum()
    for col, cnt in null_counts[null_counts > 0].items():
        print(f"             {col:<30} {cnt:>4}")
    print()
    return df


def step2_remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Hapus baris yang identik persis — simpan entri pertama."""
    before = len(df)
    df = df.drop_duplicates(keep="first").reset_index(drop=True)
    log(2, "Penghapusan duplikat", before, len(df))
    return df


def step3_fix_typos(df: pd.DataFrame) -> pd.DataFrame:
    """Perbaiki typo pada kolom Status_IMB."""
    # Hitung dulu berapa yang terkena typo
    typo_count = df["Status_IMB"].isin(TYPO_CORRECTION_MAP.keys()).sum()
    df["Status_IMB"] = df["Status_IMB"].replace(TYPO_CORRECTION_MAP)
    log(3, f"Koreksi typo Status_IMB ({typo_count} nilai diperbaiki)")

    # Pastikan tidak ada nilai di luar kategori yang dikenal
    valid_imb = {"Approved", "In Progress", "Bermasalah"}
    invalid_mask = ~df["Status_IMB"].isin(valid_imb) & df["Status_IMB"].notna()
    if invalid_mask.any():
        print(f"           ⚠ Nilai tak dikenal: {df.loc[invalid_mask, 'Status_IMB'].unique()}")
    return df


def step4_handle_missing_locations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hapus baris yang tidak punya Alamat ATAU Provinsi.
    Alasan: Looker Studio memerlukan info lokasi untuk plotting peta.
    """
    before = len(df)
    df = df.dropna(subset=["Alamat", "Provinsi"]).reset_index(drop=True)
    log(4, "Penghapusan baris tanpa data lokasi (Alamat/Provinsi)", before, len(df))
    return df


def step5_standardize_kota_kabupaten(df: pd.DataFrame) -> pd.DataFrame:
    """Title Case semua nilai Kota_Kabupaten agar tidak terpecah di filter dashboard."""
    def standardize(value):
        if pd.isna(value) or not str(value).strip():
            return None
        return str(value).strip().title()

    before_nulls = df["Kota_Kabupaten"].isna().sum()
    df["Kota_Kabupaten"] = df["Kota_Kabupaten"].apply(standardize)
    after_nulls  = df["Kota_Kabupaten"].isna().sum()
    fixed_casing = len(df) - after_nulls   # baris yang punya nilai
    log(5, f"Standardisasi Kota_Kabupaten (title case) — {fixed_casing} baris dinormalkan, "
           f"{after_nulls} masih kosong")
    return df


def step6_recalculate_remaining_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hitung ulang Sisa_Hari_Sewa dari tanggal hari ini.
    Kolom asli mungkin sudah 'basi' karena dihitung waktu data di-generate.
    """
    today = pd.Timestamp("today").normalize()

    df["Sewa_Berakhir"] = pd.to_datetime(df["Sewa_Berakhir"], errors="coerce")
    df["Sisa_Hari_Sewa"] = (df["Sewa_Berakhir"] - today).dt.days

    # Baris dengan tanggal tidak valid → isi 0 dan beri warning
    invalid_date_count = df["Sewa_Berakhir"].isna().sum()
    df["Sisa_Hari_Sewa"] = df["Sisa_Hari_Sewa"].fillna(0).astype(int)

    if invalid_date_count:
        print(f"  [Step 6] ⚠ {invalid_date_count} baris dengan tanggal Sewa_Berakhir tidak valid → Sisa_Hari_Sewa diisi 0")

    log(6, "Rekalkulasi Sisa_Hari_Sewa berdasarkan tanggal hari ini")
    return df


def step7_final_checks_and_save(df: pd.DataFrame) -> None:
    """Validasi akhir dan simpan file output."""
    # Pastikan tipe data kolom kunci sudah benar
    df["Sisa_Hari_Sewa"] = df["Sisa_Hari_Sewa"].astype(int)
    df["Tanggal_Update"]  = datetime.now().strftime("%Y-%m-%d")

    # Urutkan: cabang paling kritis (sisa hari terkecil) di atas
    df = df.sort_values("Sisa_Hari_Sewa", ascending=True).reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    log(7, f"Data bersih disimpan → {OUTPUT_FILE}  "
           f"[{df.shape[0]} baris × {df.shape[1]} kolom]")


# ─────────────────────────────────────────────
#  Laporan ringkasan pasca-cleaning
# ─────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "═" * 55)
    print("  📊 RINGKASAN DATA BERSIH")
    print("═" * 55)
    print(f"  Total cabang    : {len(df)}")
    print(f"  Total provinsi  : {df['Provinsi'].nunique()}")
    print(f"  Total kota/kab  : {df['Kota_Kabupaten'].nunique()}")

    crit = (df["Sisa_Hari_Sewa"] < 90).sum()
    exp  = (df["Sisa_Hari_Sewa"] < 0).sum()
    print(f"\n  Sewa kritis (< 90 hari)  : {crit} cabang")
    print(f"  Sewa sudah kadaluarsa    : {exp} cabang")

    print("\n  Status IMB:")
    for v, c in df["Status_IMB"].value_counts().items():
        print(f"    {v:<20} {c:>4} cabang")

    print("\n  Tingkat Sengketa:")
    for v, c in df["Tingkat_Sengketa"].value_counts().items():
        print(f"    {v:<30} {c:>4} cabang")

    print("\n  Nilai kosong di kolom utama:")
    key_cols = ["Alamat", "Provinsi", "Kota_Kabupaten", "Status_IMB",
                "Sewa_Berakhir", "Tingkat_Sengketa"]
    for col in key_cols:
        n = df[col].isna().sum()
        status = "✓ OK" if n == 0 else f"⚠ {n} kosong"
        print(f"    {col:<25} {status}")
    print("═" * 55)


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {INPUT_FILE}\n"
            "Jalankan dulu: python 03_generate_legal_data.py"
        )

    print("🧹 FASE 3 — DATA CLEANING")
    print("=" * 55)

    df = step1_load_and_inspect(INPUT_FILE)
    df = step2_remove_duplicates(df)
    df = step3_fix_typos(df)
    df = step4_handle_missing_locations(df)
    df = step5_standardize_kota_kabupaten(df)
    df = step6_recalculate_remaining_days(df)
    step7_final_checks_and_save(df)

    print_summary(df)
    print("\n✅ Fase 3 selesai! Data siap diimpor ke Google Looker Studio.")


if __name__ == "__main__":
    main()
