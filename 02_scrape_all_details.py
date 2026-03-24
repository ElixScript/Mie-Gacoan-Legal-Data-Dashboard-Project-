"""
FASE 1 — Script 2/2
Tujuan : Kunjungi halaman detail setiap cabang Mie Gacoan di Google Maps
         lalu ekstrak: alamat lengkap, provinsi, rating, jam operasional,
         nomor telepon, opsi layanan, aksesibilitas.
Input  : data/raw/mie_gacoan_locations.json
Output : data/raw/mie_gacoan_all_details.json
"""

import json
import time
import re
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────
RAW_DIR      = Path("data/raw")
INPUT_FILE   = RAW_DIR / "mie_gacoan_locations.json"
OUTPUT_FILE  = RAW_DIR / "mie_gacoan_all_details.json"
WAIT_BETWEEN = 1.5   # detik antar request


# ─────────────────────────────────────────────
#  Helper: teks aman dari selector
# ─────────────────────────────────────────────

def safe_text(page, selector: str) -> str | None:
    try:
        el = page.query_selector(selector)
        return el.inner_text().strip() if el else None
    except Exception:
        return None


def safe_attr(page, selector: str, attr: str) -> str | None:
    try:
        el = page.query_selector(selector)
        return el.get_attribute(attr).strip() if el else None
    except Exception:
        return None


# ─────────────────────────────────────────────
#  Ekstrak detail dari halaman place
# ─────────────────────────────────────────────

def extract_details(page, location: dict) -> dict:
    """Kunjungi URL dan ekstrak semua info penting."""
    detail = {
        "nama":           location["name"],
        "provinsi_query": location["province"],
        "url":            location["url"],
        "alamat":         None,
        "provinsi":       None,
        "rating":         None,
        "jumlah_review":  None,
        "telepon":        None,
        "jam_buka":       None,
        "layanan":        [],
    }

    try:
        page.goto(location["url"], wait_until="domcontentloaded", timeout=25_000)
        page.wait_for_selector('h1', timeout=10_000)
        time.sleep(1.5)
    except PlaywrightTimeout:
        return detail

    # ── Alamat ──────────────────────────────────
    addr_el = page.query_selector('button[data-item-id="address"]')
    if addr_el:
        detail["alamat"] = addr_el.get_attribute("aria-label") or addr_el.inner_text()
        detail["alamat"] = detail["alamat"].replace("Address: ", "").strip()

    # ── Provinsi dari kolom province query atau parsing alamat ──
    detail["provinsi"] = location["province"]

    # ── Rating ──────────────────────────────────
    rating_text = safe_text(page, 'div.F7nice span[aria-hidden="true"]')
    if rating_text:
        try:
            detail["rating"] = float(rating_text.replace(",", "."))
        except ValueError:
            pass

    reviews_text = safe_text(page, 'div.F7nice span[aria-label*="review"]')
    if reviews_text:
        nums = re.findall(r'\d+', reviews_text.replace(".", "").replace(",", ""))
        if nums:
            detail["jumlah_review"] = int(nums[0])

    # ── Telepon ──────────────────────────────────
    phone_el = page.query_selector('button[data-item-id^="phone"]')
    if phone_el:
        detail["telepon"] = phone_el.get_attribute("aria-label") or phone_el.inner_text()
        detail["telepon"] = detail["telepon"].replace("Phone: ", "").strip()

    # ── Jam buka ──────────────────────────────────
    hours_el = page.query_selector('div[aria-label*="hours"] table')
    if hours_el:
        detail["jam_buka"] = hours_el.inner_text().strip()

    # ── Layanan (Dine-in, Takeaway, dll) ──────────
    service_els = page.query_selector_all('div[jsaction*="pane.rating"] li')
    detail["layanan"] = [el.inner_text().strip() for el in service_els if el.inner_text().strip()]

    return detail


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"File input tidak ditemukan: {INPUT_FILE}\n"
            "Jalankan dulu: python 01_scrape_raw_locations.py"
        )

    with open(INPUT_FILE, encoding="utf-8") as f:
        locations: list[dict] = json.load(f)

    # Resume: muat data yang sudah ada
    done_details: list[dict] = []
    done_urls:    set[str]   = set()

    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            done_details = json.load(f)
        done_urls = {d["url"] for d in done_details}
        print(f"ℹ Resume: {len(done_urls)} cabang sudah diproses.")

    remaining = [loc for loc in locations if loc["url"] not in done_urls]
    print(f"▶ Akan scrape detail {len(remaining)} cabang.\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx     = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()

        for i, loc in enumerate(remaining, 1):
            print(f"[{i:04d}/{len(remaining)}] {loc['name']} ({loc['province']}) ...",
                  end=" ", flush=True)
            detail = extract_details(page, loc)
            done_details.append(detail)
            print("✓" if detail["alamat"] else "⚠ alamat kosong")

            # Simpan progres setiap 10 cabang
            if i % 10 == 0:
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(done_details, f, ensure_ascii=False, indent=2)

            time.sleep(WAIT_BETWEEN)

        browser.close()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(done_details, f, ensure_ascii=False, indent=2)

    total_with_addr = sum(1 for d in done_details if d["alamat"])
    print(f"\n✅ Selesai! Total: {len(done_details)} cabang "
          f"({total_with_addr} dengan alamat lengkap).")
    print(f"   Disimpan di: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
