"""
Kotak Neo API v2 - Download All Master Scrip CSV Files

This script:
1. Authenticates with Kotak Neo API using 2FA login (via Login.py).
2. Queries the Kotak Neo API for all master scrip CSV files across all exchange segments:
   - bse_cm.csv  (BSE Cash / Equity)
   - bse_fo.csv  (BSE Futures & Options)
   - cde_fo.csv  (Currency Derivatives)
   - mcx_fo.csv  (MCX Commodity F&O)
   - nse_cm.csv  (NSE Cash / Equity)
   - nse_com.csv (NSE Commodities)
   - nse_fo.csv  (NSE Futures & Options)
3. Downloads each file and saves it into the 'master_scrips/' directory:
   - {segment}.csv (e.g. bse_cm.csv)
"""

import os
import sys

# Ensure local SDK 'Kotak-neo-api-v2' is in sys.path
sdk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Kotak-neo-api-v2")
if os.path.exists(sdk_path) and sdk_path not in sys.path:
    sys.path.insert(0, sdk_path)

from Login import login_kotak_neo
from neo_websocket import download_all_master_scrips, MASTER_SCRIP_DIR, ALL_EXCHANGE_SEGMENTS


def main():
    print("==================================================")
    print("   KOTAK NEO API - DOWNLOAD ALL MASTER SCRIP FILES ")
    print("==================================================")
    
    client = None
    try:
        client = login_kotak_neo()
    except Exception as e:
        print(f"[!] Login exception: {e}")

    if not client:
        print("[!] Broker API Login skipped or unconfigured. Proceeding with direct Kotak CDN master scrip download...")

    print("\n[*] Downloading all complete Kotak Neo Master Scrip CSV files...")
    results = download_all_master_scrips(client, force_download=True)

    print("\n==================================================")
    print("        MASTER SCRIP DOWNLOAD SUMMARY             ")
    print("==================================================")
    print(f" Target Directory: {MASTER_SCRIP_DIR}\n")

    success_count = 0
    for seg in ALL_EXCHANGE_SEGMENTS:
        info = results.get(seg, {})
        if info.get("success"):
            success_count += 1
            size = info.get("size_mb", 0)
            rows = info.get("rows")
            row_str = f"{rows:,} lines" if rows else "cached"
            print(f"  [✓] {seg:<8} -> master_scrips/{seg}.csv ({size:.2f} MB | {row_str})")
        else:
            err = info.get("error", "Unknown error")
            print(f"  [✗] {seg:<8} -> FAILED: {err}")

    print("==================================================")
    print(f" Total Downloaded: {success_count}/{len(ALL_EXCHANGE_SEGMENTS)} files successfully saved.")
    print("==================================================\n")


if __name__ == "__main__":
    main()
