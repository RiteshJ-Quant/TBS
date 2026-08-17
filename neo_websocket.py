"""
Kotak Neo API v2 - Real-Time Feed & LTP for Nifty Current Month Future

This script:
1. Authenticates using Kotak Neo API 2FA (via Login.py).
2. Downloads and caches Kotak Master Scrip file for NSE F&O segment (nse_fo).
3. Parses the Master Scrip CSV to find NIFTY Futures contracts.
4. Automatically identifies the Current Month Nifty Future contract (next upcoming expiry date).
5. Fetches immediate Snapshot Quote (LTP, OHLC) via REST API (works off-market / closed market hours).
6. Connects to Kotak Neo WebSocket to stream real-time ticks during live market hours.
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime

# Ensure local SDK 'Kotak-neo-api-v2' is in sys.path
sdk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Kotak-neo-api-v2")
if os.path.exists(sdk_path) and sdk_path not in sys.path:
    sys.path.insert(0, sdk_path)

from Login import login_kotak_neo

MASTER_SCRIP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_scrips")


def download_and_load_master_scrip(client, exchange_segment="nse_fo", force_download=False):
    """
    Downloads and caches the Master Scrip CSV file provided by Kotak Neo API.
    """
    os.makedirs(MASTER_SCRIP_DIR, exist_ok=True)
    csv_file_path = os.path.join(MASTER_SCRIP_DIR, f"masterscrip_{exchange_segment}.csv")

    # Use cached CSV if available and not forced
    if os.path.exists(csv_file_path) and not force_download:
        file_age_hours = (time.time() - os.path.getmtime(csv_file_path)) / 3600
        if file_age_hours < 24:  # Less than 24 hours old
            print(f"[+] Loading cached Master Scrip file: {csv_file_path} ({file_age_hours:.1f} hrs old)")
            df = pd.read_csv(csv_file_path)
            df = df.rename(columns=lambda x: x.strip())
            return df

    print(f"[*] Downloading Master Scrip file for segment '{exchange_segment}' from Kotak Neo API...")
    res = client.scrip_master(exchange_segment=exchange_segment)

    csv_url = None
    if isinstance(res, str) and res.startswith("http"):
        csv_url = res
    elif isinstance(res, dict) and "filesPaths" in res:
        for path in res.get("filesPaths", []):
            if exchange_segment.lower() in path.lower():
                csv_url = path
                break

    if not csv_url:
        print(f"[!] Could not extract direct URL from scrip_master response: {res}")
        if os.path.exists(csv_file_path):
            print("[+] Falling back to existing cached master file...")
            df = pd.read_csv(csv_file_path)
            df = df.rename(columns=lambda x: x.strip())
            return df
        raise ValueError(f"Failed to fetch Master Scrip URL for {exchange_segment}")

    print(f"[+] Downloading master CSV from: {csv_url}")
    response = requests.get(csv_url)
    response.raise_for_status()

    # Save to local cache
    with open(csv_file_path, "wb") as f:
        f.write(response.content)
    print(f"[+] Saved master scrip CSV to: {csv_file_path}")

    df = pd.read_csv(csv_file_path)
    df = df.rename(columns=lambda x: x.strip())
    return df


def get_current_month_nifty_future(df):
    """
    Parses the Master Scrip DataFrame to locate the NIFTY Current Month Future contract.
    """
    print("[*] Searching Master Scrip for NIFTY Current Month Future...")

    # Identify symbol column name in Kotak's master CSV
    symbol_col = None
    for col in ["pSymbolName", "pSymbol", "pTrdSymbol", "symbol"]:
        if col in df.columns:
            symbol_col = col
            break

    if not symbol_col:
        raise KeyError(f"Could not find symbol column in CSV. Columns found: {df.columns.tolist()}")

    # 1. Filter for NIFTY
    nifty_df = df[df[symbol_col].astype(str).str.upper().str.strip() == "NIFTY"].copy()
    if nifty_df.empty:
        nifty_df = df[df[symbol_col].astype(str).str.upper().str.strip().str.contains("NIFTY")].copy()

    # 2. Filter for Futures contracts (pOptionType == "XX" or "" or strike <= 0)
    if "pOptionType" in nifty_df.columns:
        nifty_fut = nifty_df[nifty_df["pOptionType"].astype(str).str.upper().isin(["XX", "", "NAN", "FUT"])].copy()
        if nifty_fut.empty:
            nifty_fut = nifty_df.copy()
    else:
        nifty_fut = nifty_df.copy()

    if "dStrikePrice;" in nifty_fut.columns:
        nifty_fut["dStrikePrice;"] = pd.to_numeric(nifty_fut["dStrikePrice;"], errors="coerce").fillna(0)
        nifty_fut = nifty_fut[nifty_fut["dStrikePrice;"] <= 0]
    elif "dStrikePrice" in nifty_fut.columns:
        nifty_fut["dStrikePrice"] = pd.to_numeric(nifty_fut["dStrikePrice"], errors="coerce").fillna(0)
        nifty_fut = nifty_fut[nifty_fut["dStrikePrice"] <= 0]

    if nifty_fut.empty:
        raise ValueError("No NIFTY Futures contracts found in master scrip file.")

    # 3. Parse Expiry Dates
    # Kotak epoch timestamps in master files have an offset of +315511200 seconds (~10 years)
    if "pExpiryDate" in nifty_fut.columns:
        if pd.api.types.is_numeric_dtype(nifty_fut["pExpiryDate"]):
            nifty_fut["expiry_dt"] = pd.to_datetime(nifty_fut["pExpiryDate"], unit="s") + pd.to_timedelta(315511200, unit="s")
        else:
            nifty_fut["expiry_dt"] = pd.to_datetime(nifty_fut["pExpiryDate"], errors="coerce")
    else:
        raise KeyError("pExpiryDate column missing in master scrip file.")

    nifty_fut = nifty_fut.dropna(subset=["expiry_dt"])

    # 4. Filter for upcoming expiries (today onwards)
    today = pd.Timestamp.now().normalize()
    upcoming_fut = nifty_fut[nifty_fut["expiry_dt"] >= today].sort_values("expiry_dt", ascending=True)

    if upcoming_fut.empty:
        upcoming_fut = nifty_fut.sort_values("expiry_dt", ascending=False)

    # Current Month Future is the first upcoming contract
    cur_fut = upcoming_fut.iloc[0]

    # Extract Instrument Token & Trading Symbol
    token = None
    for t_col in ["pSymbolToken", "pSymbol", "lTok", "pToken", "instrument_token", "token"]:
        if t_col in cur_fut and pd.notna(cur_fut[t_col]):
            try:
                token = str(int(float(cur_fut[t_col])))
            except Exception:
                token = str(cur_fut[t_col])
            break

    trd_symbol = cur_fut.get("pTrdSymbol") or cur_fut.get("pSymbol") or cur_fut.get("pSymbolName") or "NIFTY FUT"
    expiry_str = cur_fut["expiry_dt"].strftime("%d-%b-%Y")

    print("\n==================================================")
    print("      IDENTIFIED NIFTY CURRENT MONTH FUTURE      ")
    print("==================================================")
    print(f"  Trading Symbol   : {trd_symbol}")
    print(f"  Instrument Token : {token}")
    print(f"  Expiry Date      : {expiry_str}")
    print(f"  Exchange Segment : nse_fo")
    print("==================================================\n")

    return {
        "instrument_token": token,
        "trading_symbol": trd_symbol,
        "expiry": expiry_str,
        "exchange_segment": "nse_fo"
    }


def fetch_snapshot_quote(client, token, symbol, segment="nse_fo"):
    """
    Fetches snapshot LTP & quote details via Kotak REST API.
    Works even when the market is closed / off-hours.
    """
    print(f"[*] Fetching Snapshot Quote for {symbol} (Token: {token})...")
    try:
        quote_res = client.quotes(
            instrument_tokens=[{
                "instrument_token": str(token),
                "exchange_segment": segment
            }],
            quote_type="ltp"
        )
        print("\n--------------------------------------------------")
        print(f"  SNAPSHOT QUOTE / LAST TRADED PRICE (LTP)")
        print("--------------------------------------------------")
        if isinstance(quote_res, list) and len(quote_res) > 0:
            q_data = quote_res[0]
            ltp = q_data.get("ltp") or q_data.get("last_traded_price") or q_data.get("v", {}).get("ltp") or q_data
            print(f"  Symbol           : {symbol}")
            print(f"  Token            : {token}")
            print(f"  Last Price (LTP) : ₹{ltp}")
            if "close" in q_data:
                print(f"  Close Price      : ₹{q_data['close']}")
            if "change" in q_data:
                print(f"  Price Change     : {q_data['change']}")
        elif isinstance(quote_res, dict):
            print(f"  Quote Response   : {quote_res}")
        else:
            print(f"  Quote Output     : {quote_res}")
        print("--------------------------------------------------\n")
        return quote_res
    except Exception as e:
        print(f"[!] Could not fetch snapshot quote: {e}\n")
        return None


def main():
    # Step 1: Login to Kotak Neo API
    print("[*] Authenticating with Kotak Neo API...")
    client = login_kotak_neo()
    if not client:
        print("[!] Authentication failed. Cannot proceed.")
        return

    # Step 2: Download Master Scrip & Find Nifty Current Month Future
    try:
        df_master = download_and_load_master_scrip(client, exchange_segment="nse_fo")
        nifty_info = get_current_month_nifty_future(df_master)
    except Exception as e:
        print(f"[!] Error fetching/parsing master scrip: {e}")
        return

    token = nifty_info["instrument_token"]
    symbol = nifty_info["trading_symbol"]

    if not token:
        print("[!] Could not determine instrument token for Nifty Future.")
        return

    # Step 3: Fetch Snapshot Quote (LTP) via REST API (Guaranteed to return LTP off-market)
    fetch_snapshot_quote(client, token=token, symbol=symbol, segment="nse_fo")

    # Step 4: Define WebSocket Callbacks for Live Market Feed
    ws_is_closed = False

    def on_message(message):
        """Callback for incoming WebSocket tick data."""
        if isinstance(message, dict):
            feed_type = message.get("type", "tick")
            data = message.get("data", message)

            if isinstance(data, dict):
                ltp = data.get("ltp") or data.get("last_traded_price") or data.get("v", {}).get("ltp")
                change = data.get("change") or data.get("nc")
                volume = data.get("volume") or data.get("v")
                print(f"[LTP TICK] Symbol: {symbol} | Token: {token} | LTP: ₹{ltp} | Change: {change} | Vol: {volume}")
            else:
                print(f"[WS MESSAGE] ({feed_type}): {data}")
        elif isinstance(message, list):
            for item in message:
                if isinstance(item, dict):
                    ltp = item.get("ltp") or item.get("last_traded_price")
                    print(f"[LTP TICK] Symbol: {symbol} | Token: {token} | LTP: ₹{ltp}")
                else:
                    print(f"[WS TICK]: {item}")
        else:
            print(f"[WS TICK]: {message}")

    def on_error(error):
        nonlocal ws_is_closed
        if not ws_is_closed:
            err_msg = str(error)
            if "already closed" in err_msg.lower() or "none_type" in err_msg.lower():
                ws_is_closed = True
                print("[!] Market is currently closed or WebSocket feed disconnected by broker server.")
            else:
                print(f"[WS ERROR]: {error}")

    def on_open(message):
        print(f"[WS OPEN]: Connected to Kotak Neo WebSocket feed for {symbol} (Token: {token})")

    def on_close(message):
        nonlocal ws_is_closed
        if not ws_is_closed:
            ws_is_closed = True
            print(f"[WS CLOSE]: WebSocket session closed.")

    # Set callbacks on client
    client.on_message = on_message
    client.on_error = on_error
    client.on_open = on_open
    client.on_close = on_close

    # Step 5: Initiate WebSocket Live Feed
    print(f"[*] Subscribing to live WebSocket stream for {symbol} (Token: {token})...\n")
    try:
        client.subscribe(
            instrument_tokens=[{
                "instrument_token": str(token),
                "exchange_segment": "nse_fo"
            }]
        )
    except Exception as e:
        print(f"[!] WebSocket subscribe notice: {e}")

    # Keep main thread alive
    try:
        for _ in range(10):
            if ws_is_closed:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Exiting script...")

    if ws_is_closed:
        print("\n[NOTE] Off-Market Hours Info:")
        print("       - Live WebSocket streaming is active during Indian Market Hours (9:15 AM - 3:30 PM IST, Mon-Fri).")
        print("       - During off-market hours or weekends, Kotak's WebSocket server disconnects active streaming sessions.")
        print("       - Your last available LTP quote was retrieved above via Snapshot Quotes API.")


if __name__ == "__main__":
    main()
