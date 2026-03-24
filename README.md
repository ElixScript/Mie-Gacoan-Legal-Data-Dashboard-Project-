# Pipeline Legal Data Analytics — Mie Gacoan Mock Project

## Struktur File

```
scripts/
├── 01_scrape_raw_locations.py   # Fase 1a: Scrape URL cabang dari Google Maps
├── 02_scrape_all_details.py     # Fase 1b: Scrape detail setiap cabang
├── 03_generate_legal_data.py    # Fase 2 : Generate & inject dirty data legal
├── 04_clean_legal_data.py       # Fase 3 : Cleaning data untuk Looker Studio
└── requirements.txt

data/
├── raw/
│   ├── mie_gacoan_locations.json        # Output Fase 1a
│   └── mie_gacoan_all_details.json      # Output Fase 1b
└── processed/
    ├── gacoan_legal_analytics_data.csv           # Output Fase 2 (bersih, referensi)
    ├── dirty_gacoan_legal_analytics_data.csv     # Output Fase 2 (kotor, bahan cleaning)
    └── cleaned_gacoan_legal_analytics_data.csv   # Output Fase 3 (siap Looker Studio)
```

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Cara Menjalankan

Jalankan secara berurutan:

```bash
# Fase 1: Scraping
python scripts/01_scrape_raw_locations.py
python scripts/02_scrape_all_details.py

# Fase 2: Data Engineering
python scripts/03_generate_legal_data.py

# Fase 3: Data Cleaning
python scripts/04_clean_legal_data.py
```

## Catatan

- Semua script mendukung **resume otomatis** — aman dijalankan ulang kalau koneksi terputus.
- Script scraping menggunakan Playwright (Chromium headless), bukan requests biasa,
  karena Google Maps memerlukan JavaScript rendering dan infinite scroll.
- File output Fase 3 (`cleaned_gacoan_legal_analytics_data.csv`) siap langsung
  diupload ke Google Looker Studio sebagai data source.
