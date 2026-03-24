"""
FASE 1 — Script 1/2 (IMPROVED VERSION)
Tujuan : Scrape daftar URL seluruh cabang Mie Gacoan dari Google Maps
Output : data/raw/mie_gacoan_locations.json
"""

import json
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────
OUTPUT_DIR  = Path("data/raw")
OUTPUT_FILE = OUTPUT_DIR / "mie_gacoan_locations.json"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PROVINCES = [
    "Aceh", "Sumatra Utara", "Sumatra Barat", "Riau", "Kepulauan Riau",
    "Jambi", "Bengkulu", "Sumatra Selatan", "Bangka Belitung", "Lampung",
    "DKI Jakarta", "Jawa Barat", "Banten", "Jawa Tengah",
    "DI Yogyakarta", "Jawa Timur",
    "Bali", "Nusa Tenggara Barat", "Nusa Tenggara Timur",
    "Kalimantan Barat", "Kalimantan Tengah", "Kalimantan Selatan",
    "Kalimantan Timur", "Kalimantan Utara",
    "Sulawesi Utara", "Gorontalo", "Sulawesi Tengah",
    "Sulawesi Selatan", "Sulawesi Barat", "Sulawesi Tenggara",
    "Maluku", "Maluku Utara",
    "Papua", "Papua Barat",
]

SCROLL_PAUSE_SEC = 2
MAX_SCROLL_ITER  = 60


# ─────────────────────────────────────────────
#  Helper Functions
# ─────────────────────────────────────────────

def safe_goto(page, url, retries=3):
    """Buka halaman dengan retry untuk menghindari timeout."""
    for attempt in range(retries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            return True
        except PlaywrightTimeout:
            print(f"\n   ⚠ Retry {attempt+1}/{retries}...")
            time.sleep(3)
    return False


def scroll_until_end(page):
    """Scroll sampai tidak ada tambahan data lagi."""
    prev_height = 0

    for _ in range(MAX_SCROLL_ITER):
        try:
            current_height = page.evaluate(
                "document.querySelector('div[role=\"feed\"]').scrollHeight"
            )

            if current_height == prev_height:
                print("      ✓ Tidak ada tambahan data, stop scroll.")
                break

            prev_height = current_height

            page.evaluate(
                """
                const feed = document.querySelector('div[role="feed"]');
                if (feed) feed.scrollTo(0, feed.scrollHeight);
                """
            )

            time.sleep(SCROLL_PAUSE_SEC)

        except Exception:
            print("      ⚠ Error saat scroll, lanjut...")
            break


def scrape_province(page, province: str) -> list[dict]:
    """Scrape satu provinsi."""
    query = f"Mie Gacoan {province}"
    url   = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    results = []

    if not safe_goto(page, url):
        print(f"   ❌ Gagal load {province}, skip.")
        return results

    try:
        page.wait_for_selector('div[role="feed"]', timeout=30_000)
    except PlaywrightTimeout:
        print(f"   ⚠ Feed tidak muncul di {province}, skip.")
        return results

    scroll_until_end(page)

    try:
        links = page.locator('a[href*="maps/place"]').all()
    except Exception:
        print(f"   ⚠ Gagal ambil links di {province}")
        return results

    for link in links:
        try:
            name = link.get_attribute("aria-label")
            href = link.get_attribute("href")

            if name and href and "Mie Gacoan" in name:
                results.append({
                    "name": name.strip(),
                    "url": href,
                    "province": province,
                })

        except Exception:
            continue

    return results


# ─────────────────────────────────────────────
#  Main Pipeline
# ─────────────────────────────────────────────

def main():
    all_locations = []
    done_provinces = set()

    # Resume kalau file sudah ada
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            all_locations = json.load(f)

        done_provinces = {loc["province"] for loc in all_locations}

        print(f"ℹ Resume: {len(done_provinces)} provinsi sudah diproses")
        print(f"   Total sementara: {len(all_locations)} lokasi\n")

    remaining = [p for p in PROVINCES if p not in done_provinces]
    print(f"▶ Akan scrape {len(remaining)} provinsi.\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)

        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800}
        )

        page = ctx.new_page()

        for i, province in enumerate(remaining, 1):
            print(f"[{i:02d}/{len(remaining)}] Scraping: {province} ...", end=" ")

            locs = scrape_province(page, province)
            print(f"{len(locs)} lokasi ditemukan.")

            all_locations.extend(locs)

            # Save progress
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(all_locations, f, ensure_ascii=False, indent=2)

            time.sleep(random.uniform(1, 3))

        browser.close()

    # ─────────────────────────────────────────
    # Deduplication
    # ─────────────────────────────────────────
    seen_urls = set()
    unique_locations = []

    for loc in all_locations:
        if loc["url"] not in seen_urls:
            seen_urls.add(loc["url"])
            unique_locations.append(loc)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(unique_locations, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Selesai!")
    print(f"Total unik: {len(unique_locations)} lokasi")
    print(f"Disimpan di: {OUTPUT_FILE}")


# ─────────────────────────────────────────────

if __name__ == "__main__":
    main()