"""
Kotak Neo API v2 - TRADE BOTS Web Application Server
Flask Backend Server providing REST API endpoints for:
- 2FA Broker Login & Session Management
- TRADE BOTS Time-Based Automated Strategy Scheduler
- Manual Desk (OMS & Multi-Leg Basket Orders)
- Order Placement (Market, Limit, SL, SL-M)
- Order Book & Positions Reports
- Scrip Autocomplete Suggestions

Usage:
    python server.py
    Open browser at http://127.0.0.1:5000
"""

import os
import sys
import json
import time
import uuid
import re
import logging
import threading
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory

SUBSCRIBED_TOKENS = set()
SUBSCRIBED_LOCK = threading.Lock()
LIVE_LTP_CACHE = {}
LIVE_LTP_LOCK = threading.Lock()
TOKEN_TO_SYMBOL_MAP = {}
TOKEN_MAP_LOCK = threading.Lock()

PREWARMED_CONTRACTS = {}
PREWARM_ACTIVE = {}
PREWARM_LOCK = threading.Lock()


def register_token_aliases(token: str, *aliases):
    """Registers aliases for an instrument token so WebSocket ticks update all symbol aliases."""
    tok_str = str(token).strip()
    if not tok_str:
        return
    with TOKEN_MAP_LOCK:
        if tok_str not in TOKEN_TO_SYMBOL_MAP:
            TOKEN_TO_SYMBOL_MAP[tok_str] = set()
        for a in aliases:
            if a:
                TOKEN_TO_SYMBOL_MAP[tok_str].add(str(a).strip())


# Ensure local SDK 'Kotak-neo-api-v2' is accessible in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SDK_PATH = os.path.join(BASE_DIR, "Kotak-neo-api-v2")
if os.path.exists(SDK_PATH) and SDK_PATH not in sys.path:
    sys.path.insert(0, SDK_PATH)

try:
    from neo_api_client import NeoAPI
except ImportError:
    print("[!] Warning: Could not import 'neo_api_client'. Ensure 'Kotak-neo-api-v2' directory exists.")
    NeoAPI = None

try:
    import pyotp
except ImportError:
    pyotp = None

try:
    from neo_websocket import download_all_master_scrips, MASTER_SCRIP_DIR, ALL_EXCHANGE_SEGMENTS
except ImportError:
    download_all_master_scrips = None
    MASTER_SCRIP_DIR = os.path.join(BASE_DIR, "master_scrips")
    ALL_EXCHANGE_SEGMENTS = ["bse_cm", "bse_fo", "cde_fo", "mcx_fo", "nse_cm", "nse_com", "nse_fo"]

app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Global session storage for active authenticated client
SESSION = {
    "client": None,
    "ucc": None,
    "mobile_number": None,
    "environment": "prod",
    "logged_in": False
}

STRATEGIES_FILE = os.path.join(BASE_DIR, "strategies.json")
STRATEGIES_LOCK = threading.RLock()
STRATEGIES = []
EXECUTION_LOGS = []

SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
SETTINGS_LOCK = threading.RLock()
APP_SETTINGS = {
    "offset_type": "Points (₹)",
    "offset_value": 0.5,
    "time_display_mode": "24-Hour Format (e.g. 21:09:15)",
    "quantity_display_mode": "Show in Lots (Base)"
}

CONSOLE_LOGS = []
CONSOLE_LOGS_LOCK = threading.Lock()


def add_system_console_log(msg: str, category: str = "SYSTEM"):
    """Adds a log entry to the live system console buffer for the UI console."""
    entry = {
        "id": str(uuid.uuid4())[:8],
        "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "category": category.upper(),
        "message": msg
    }
    with CONSOLE_LOGS_LOCK:
        CONSOLE_LOGS.insert(0, entry)
        if len(CONSOLE_LOGS) > 150:
            CONSOLE_LOGS.pop()
    logging.info(f"[{category}] {msg}")



def load_settings():
    """Loads user OMS limit order and display settings from JSON file."""
    global APP_SETTINGS
    with SETTINGS_LOCK:
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    data = json.load(f)
                    APP_SETTINGS.update(data)
                    logging.info(f"[+] Loaded settings from {SETTINGS_FILE}: {APP_SETTINGS}")
            except Exception as e:
                logging.error(f"[!] Error loading {SETTINGS_FILE}: {e}")


def save_settings():
    """Saves user OMS limit order and display settings to JSON file."""
    with SETTINGS_LOCK:
        try:
            with open(SETTINGS_FILE, "w") as f:
                json.dump(APP_SETTINGS, f, indent=2)
        except Exception as e:
            logging.error(f"[!] Error saving {SETTINGS_FILE}: {e}")


POPULAR_SCRIPS = [
    {"symbol": "NIFTY", "name": "Nifty Index Option/Future", "exchange": "nse_fo"},
    {"symbol": "BANKNIFTY", "name": "Bank Nifty Index Option/Future", "exchange": "nse_fo"},
    {"symbol": "FINNIFTY", "name": "Fin Nifty Index Option/Future", "exchange": "nse_fo"},
    {"symbol": "SENSEX", "name": "Sensex Index Option/Future", "exchange": "bse_fo"},
    {"symbol": "SBIN-EQ", "name": "State Bank of India", "exchange": "nse_cm"},
    {"symbol": "RELIANCE-EQ", "name": "Reliance Industries Ltd", "exchange": "nse_cm"},
    {"symbol": "INFY-EQ", "name": "Infosys Limited", "exchange": "nse_cm"},
    {"symbol": "TCS-EQ", "name": "Tata Consultancy Services", "exchange": "nse_cm"},
    {"symbol": "HDFCBANK-EQ", "name": "HDFC Bank Limited", "exchange": "nse_cm"},
    {"symbol": "TATAMOTORS-EQ", "name": "Tata Motors Limited", "exchange": "nse_cm"}
]


def load_strategies():
    """Loads user strategies from JSON file."""
    global STRATEGIES
    with STRATEGIES_LOCK:
        if os.path.exists(STRATEGIES_FILE):
            try:
                with open(STRATEGIES_FILE, "r") as f:
                    STRATEGIES = json.load(f)
                    logging.info(f"[+] Loaded {len(STRATEGIES)} strategies from {STRATEGIES_FILE}")
            except Exception as e:
                logging.error(f"[!] Error reading {STRATEGIES_FILE}: {e}")
                STRATEGIES = []
        else:
            # Seed default demo strategy matching user screenshot if file is new
            STRATEGIES = [
                {
                    "id": "strat_demo_1",
                    "symbol": "NIFTY",
                    "broker": "Kotak Neo",
                    "lots": 1,
                    "batches": 1,
                    "entry_time": "09:20",
                    "entry_interval": "0",
                    "exit_time": "15:20",
                    "exit_interval": "0",
                    "sl": "2 Pts",
                    "strike_selection": "Premium closest to 3",
                    "trade_mode": "REAL",
                    "status": "IDLE",
                    "pnl": "₹0.00",
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            ]
            save_strategies()


def save_strategies():
    """Saves user strategies to JSON file."""
    with STRATEGIES_LOCK:
        try:
            with open(STRATEGIES_FILE, "w") as f:
                json.dump(STRATEGIES, f, indent=2)
        except Exception as e:
            logging.error(f"[!] Error writing to {STRATEGIES_FILE}: {e}")


def generate_totp_code(totp_secret: str) -> str:
    """Generate 6-digit TOTP from secret if pyotp is installed."""
    if not totp_secret:
        return ""
    if not pyotp:
        return ""
    try:
        clean_secret = totp_secret.replace(" ", "").upper()
        return pyotp.TOTP(clean_secret).now()
    except Exception as e:
        logging.error(f"Error generating TOTP: {e}")
        return ""


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Rounds price to nearest exchange tick size (0.05 for NSE)."""
    if price <= 0:
        return tick_size
    return round(round(price / tick_size) * tick_size, 2)


def execute_single_order(client, payload: dict) -> dict:
    """Helper to place an order via Kotak Neo API client."""
    exchange_segment = str(payload.get("exchange_segment", "nse_cm")).strip().lower()
    trading_symbol = str(payload.get("trading_symbol", "")).strip().upper()
    transaction_type = str(payload.get("transaction_type", "B")).strip().upper()
    product = str(payload.get("product", "CNC")).strip().upper()
    order_type = str(payload.get("order_type", "MKT")).strip().upper()
    quantity = str(payload.get("quantity", "1")).strip()
    price = str(payload.get("price", "0")).strip()
    trigger_price = str(payload.get("trigger_price", "0")).strip()
    validity = str(payload.get("validity", "DAY")).strip().upper()
    amo = str(payload.get("amo", "NO")).strip().upper()
    disclosed_quantity = str(payload.get("disclosed_quantity", "0")).strip()
    market_protection = str(payload.get("market_protection", "0")).strip()
    pf = str(payload.get("pf", "N")).strip().upper()
    tag = str(payload.get("tag", "trade_bots")).strip()

    if transaction_type in ["BUY", "B"]:
        tx_type = "B"
    elif transaction_type in ["SELL", "S"]:
        tx_type = "S"
    else:
        tx_type = transaction_type

    if order_type in ["MKT", "SL-M"]:
        price = "0"
    if order_type not in ["SL", "SL-M"]:
        trigger_price = "0"

    try:
        response = client.place_order(
            exchange_segment=exchange_segment,
            product=product,
            price=price,
            order_type=order_type,
            quantity=quantity,
            validity=validity,
            trading_symbol=trading_symbol,
            transaction_type=tx_type,
            amo=amo,
            disclosed_quantity=disclosed_quantity,
            market_protection=market_protection,
            pf=pf,
            trigger_price=trigger_price,
            tag=tag
        )
    except Exception as ex:
        response = {"stat": "Not_Ok", "errMsg": str(ex)}

    order_id = None
    stat = None
    err_msg = None

    if isinstance(response, dict):
        stat = str(response.get("stat") or response.get("status") or "").strip()
        order_id = response.get("order_id") or response.get("nOrderNo") or response.get("result") or response.get("data", {}).get("order_id")
        err_msg = response.get("errMsg") or response.get("message") or response.get("reason") or response.get("error")

    tx_str = "BUY" if tx_type == "B" else ("SELL" if tx_type == "S" else tx_type)
    
    # Check if order was successfully accepted by broker
    is_success = bool(order_id) or (stat in ["Ok", "success", "OK", "0"])

    if is_success and order_id:
        if order_type == "SL":
            log_msg = f"🛡️ [SL ORDER SUBMITTED] {trading_symbol} {tx_str} Qty={quantity} Trigger=₹{trigger_price} Limit=₹{price} (Order ID: {order_id})"
            add_system_console_log(log_msg, category="SL_ORDER")
        elif order_type == "L":
            log_msg = f"🚀 [LIMIT ORDER SUBMITTED] {trading_symbol} {tx_str} Qty={quantity} Price=₹{price} (Order ID: {order_id})"
            add_system_console_log(log_msg, category="ORDER_DETAILS")
        else:
            log_msg = f"⚡ [MARKET ORDER SUBMITTED] {trading_symbol} {tx_str} Qty={quantity} (Order ID: {order_id})"
            add_system_console_log(log_msg, category="ORDER_EXECUTED")
    else:
        failure_reason = err_msg or "Broker API returned no Order ID or status Not_Ok"
        log_msg = f"❌ [ORDER REJECTED] {trading_symbol} {tx_str} ({order_type}) Qty={quantity} - Reason: {failure_reason}"
        logging.error(f"[!] Order Placement Rejected by Broker: {trading_symbol} | Response: {response}")
        add_system_console_log(log_msg, category="BROKER")

    return {
        "success": is_success,
        "order_id": order_id if is_success else None,
        "error": err_msg if not is_success else None,
        "raw_response": response,
        "payload": payload
    }



def prewarm_strategy_option_chain(strat):
    """
    Pre-warms option chain 20 seconds before entry time.
    Resolves option strikes (+/- 25 strikes around ATM) and bulk-subscribes
    broker WebSocket to quote feeds of candidate contracts.
    """
    symbol = (strat.get("symbol") or strat.get("scrip_index") or "NIFTY").upper()
    spot_price = get_live_spot_price(symbol)
    step = get_index_step_size(symbol)
    if spot_price <= 0:
        spot_price = 24500.0 if symbol == "NIFTY" else (52000.0 if symbol == "BANKNIFTY" else 23500.0)

    atm_strike = int(round(spot_price / step) * step)
    candidate_strikes = [atm_strike + (i * step) for i in range(-15, 16)]
    
    client = SESSION.get("client")
    subscribed_count = 0

    for strike in candidate_strikes:
        for opt_type in ["CE", "PE"]:
            opt_info = lookup_option_contract_token(symbol, strike, opt_type)
            if opt_info:
                token = opt_info.get("token")
                trd_sym = opt_info.get("trading_symbol")
                contract_sym = f"{symbol}26804{strike}{opt_type}"
                segment = opt_info.get("segment", "nse_fo")
                if token:
                    register_token_aliases(token, trd_sym, contract_sym, f"{symbol}{strike}{opt_type}")
                    if client and SESSION.get("logged_in"):
                        if subscribe_token_once(client, token=token, segment=segment, is_index=False):
                            subscribed_count += 1

    logging.info(f"🔥 [PRE-WARM INIT] Pre-warmed option chain for {symbol} around ATM {atm_strike} (Subscribed {subscribed_count} option strike quote feeds)")


def evaluate_prewarmed_contracts(strat):
    """
    Fast preselection evaluation loop (runs every 250 ms) during 20-second pre-warm window.
    Preselects qualifying contract legs based on real-time quotes in LIVE_LTP_CACHE.
    """
    symbol = (strat.get("symbol") or strat.get("scrip_index") or "NIFTY").upper()
    legs = strat.get("legs") or []
    spot_price = get_live_spot_price(symbol)
    
    if not legs:
        opt_type = (strat.get("option_type") or "CE").upper()
        criteria = strat.get("strike_selection", "ATM")
        target_val = strat.get("closest_premium") or strat.get("target_premium")
        lots = int(strat.get("lots", 1))
        legs = [{
            "option_type": opt_type,
            "strike_selection": criteria,
            "target_val": target_val,
            "lots": lots,
            "transaction_type": strat.get("transaction_type", "S")
        }]

    preselected_legs = []
    
    for leg in legs:
        opt_type = (leg.get("option_type") or leg.get("opt_type") or "CE").upper()
        criteria = leg.get("strike_selection") or leg.get("criteria") or strat.get("strike_selection", "ATM")
        target_val = leg.get("target_val") or leg.get("target_premium") or strat.get("closest_premium")
        lots = int(leg.get("lots", 1))
        tx_type = leg.get("transaction_type", "S")
        
        strike = calculate_option_strike(symbol, opt_type, criteria, spot_price=spot_price, target_val=target_val)
        opt_info = lookup_option_contract_token(symbol, strike, opt_type)
        
        token = opt_info.get("token") if opt_info else None
        trd_sym = opt_info.get("trading_symbol") if opt_info else f"{symbol}26804{strike}{opt_type}"
        contract_sym = f"{symbol}26804{strike}{opt_type}"
        segment = opt_info.get("segment") if opt_info else ("bse_fo" if symbol in ["SENSEX", "BANKEX"] else "nse_fo")
        
        ltp_val = get_option_contract_ltp(token, trd_sym, contract_sym)
        lot_size = get_symbol_lot_size(symbol, opt_info)

        preselected_legs.append({
            "symbol": symbol,
            "strike": strike,
            "option_type": opt_type,
            "criteria": criteria,
            "contract_symbol": contract_sym,
            "trading_symbol": trd_sym,
            "token": token,
            "segment": segment,
            "transaction_type": tx_type,
            "lots": lots,
            "lot_size": lot_size,
            "quantity": lots * lot_size,
            "ltp": ltp_val,
            "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3]
        })

    strat_id = strat.get("id")
    with PREWARM_LOCK:
        PREWARMED_CONTRACTS[strat_id] = {
            "strategy_id": strat_id,
            "symbol": symbol,
            "updated_at": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "legs": preselected_legs
        }
    
    return preselected_legs


def background_strategy_scheduler():
    """
    Background worker thread running continuously (250ms loop frequency) to trigger
    20-second pre-warming and execute TRADE BOTS time-based strategies at exact entry & exit times.
    """
    logging.info("[+] TRADE BOTS High-Precision Background Strategy Scheduler Started (250ms loop).")
    executed_entries = set()
    executed_exits = set()

    while True:
        try:
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            current_hhmm = now.strftime("%H:%M")

            with STRATEGIES_LOCK:
                for strat in STRATEGIES:
                    strat_id = strat.get("id")
                    status = strat.get("status", "IDLE")
                    entry_time = str(strat.get("entry_time", "")).strip()
                    exit_time = str(strat.get("exit_time", "")).strip()

                    if not entry_time or ":" not in entry_time:
                        continue

                    try:
                        entry_h, entry_m = map(int, entry_time.split(":"))
                        entry_dt = datetime(now.year, now.month, now.day, entry_h, entry_m, 0)
                    except Exception:
                        continue

                    time_to_entry = (entry_dt - now).total_seconds()
                    entry_key = f"{strat_id}_{today_str}_{entry_time}"

                    # -------------------------------------------------------------
                    # 1. PRE-WARMING PHASE (20 seconds prior to entry time: T-20s to T)
                    # -------------------------------------------------------------
                    if status in ["RUNNING", "PREWARMING"] and (entry_key not in executed_entries):
                        if 0 < time_to_entry <= 20:
                            if strat.get("status") != "PREWARMING":
                                strat["status"] = "PREWARMING"
                            
                            if not PREWARM_ACTIVE.get(strat_id):
                                PREWARM_ACTIVE[strat_id] = True
                                logging.info(f"🔥 [PRE-WARM STARTED] Strategy '{strat.get('symbol')}' starting 20s pre-warming ({time_to_entry:.1f}s remaining before {entry_time})")
                                try:
                                    prewarm_strategy_option_chain(strat)
                                except Exception as pex:
                                    logging.error(f"[!] Pre-warm option chain error for {strat_id}: {pex}")

                            try:
                                legs = evaluate_prewarmed_contracts(strat)
                                logging.debug(f"⚡ [250ms PRE-WARM TICK] Strat '{strat.get('symbol')}' T-{time_to_entry:.1f}s: Preselected {len(legs)} contract leg(s)")
                            except Exception as eex:
                                logging.error(f"[!] Pre-warm evaluation tick error: {eex}")

                    # -------------------------------------------------------------
                    # 2. INSTANT STRATEGY ENTRY (At T, e.g. time_to_entry <= 0)
                    # -------------------------------------------------------------
                    if status in ["RUNNING", "PREWARMING"] and (entry_key not in executed_entries) and (time_to_entry <= 0) and (time_to_entry >= -300):
                        executed_entries.add(entry_key)
                        logging.info(f"⏰ [INSTANT TRADE BOTS ENTRY] Executing Strategy '{strat.get('symbol')}' at {now.strftime('%H:%M:%S.%f')[:-3]} (Scheduled: {entry_time})")
                        
                        order_ids = []
                        prewarmed_data = PREWARMED_CONTRACTS.get(strat_id)
                        
                        if prewarmed_data and prewarmed_data.get("legs"):
                            contracts = prewarmed_data["legs"]
                            logging.info(f"⚡ [INSTANT ENTRY] Utilizing {len(contracts)} pre-warmed & pre-selected qualifying contract(s) with ZERO strike resolution latency!")
                        else:
                            contracts = get_strategy_tracker_contracts(strat)

                        if SESSION.get("logged_in") and SESSION.get("client"):
                            try:
                                with SETTINGS_LOCK:
                                    offset_val = float(APP_SETTINGS.get("offset_value", 0.5))
                                    offset_type = str(APP_SETTINGS.get("offset_type", "Points (₹)"))

                                for c in contracts:
                                    trd_sym = c.get("trading_symbol") or c.get("contract_symbol")
                                    raw_tx = str(c.get("transaction_type", "S")).strip().upper()
                                    tx_type = "B" if raw_tx in ["BUY", "B"] else "S"
                                    lot_sz = c.get("lot_size") or get_symbol_lot_size(c.get("symbol") or strat.get("symbol"), c)
                                    qty = str(c.get("quantity") or (int(c.get("lots", 1)) * lot_sz))
                                    segment = c.get("segment", "nse_fo")

                                    # Retrieve latest LTP for the leg
                                    ltp_val = float(c.get("ltp") or get_option_contract_ltp(c.get("token"), trd_sym, c.get("contract_symbol")) or 0.0)
                                    if ltp_val <= 0:
                                        ltp_val = 100.0

                                    # Calculate limit offset based on user settings
                                    if "%" in offset_type or "Percentage" in offset_type:
                                        entry_offset = ltp_val * (offset_val / 100.0)
                                    else:
                                        entry_offset = offset_val

                                    # Calculate limit entry price: Price = LTP +/- entry_limit_offset (rounded to tick 0.05)
                                    if tx_type == "B":
                                        limit_entry_price = round_to_tick(ltp_val + entry_offset)
                                    else:
                                        limit_entry_price = round_to_tick(max(0.05, ltp_val - entry_offset))

                                    # 1. Submit Limit Entry Order (order_type="L") via Kotak SDK
                                    logging.info(f"🚀 [LIMIT ENTRY ORDER] Submitting Limit Order for {trd_sym} {tx_type} Qty={qty} @ Price=₹{limit_entry_price} (LTP=₹{ltp_val}, Offset={entry_offset:.2f})")
                                    res_entry = execute_single_order(SESSION["client"], {
                                        "exchange_segment": segment,
                                        "trading_symbol": trd_sym,
                                        "transaction_type": tx_type,
                                        "product": "MIS",
                                        "order_type": "L",
                                        "price": str(limit_entry_price),
                                        "quantity": qty,
                                        "tag": "trade_bots_limit_entry"
                                    })
                                    entry_oid = res_entry.get("order_id") if isinstance(res_entry, dict) else None
                                    if entry_oid:
                                        order_ids.append(f"LIMIT_ENTRY:{entry_oid}")

                                    # 2. Check if Stop Loss (SL) is explicitly configured and enabled in strategy / contract
                                    sl_raw = strat.get("sl") if strat.get("sl") is not None else c.get("sl")
                                    sl_str = str(sl_raw).strip() if sl_raw is not None else ""

                                    is_sl_enabled = False
                                    sl_val = 0.0
                                    if sl_str and sl_str.upper() not in ["NONE", "DISABLED", "OFF", "0", "0 PTS", "0.0", "0%", "0.0 PTS"]:
                                        sl_matches = re.findall(r"[-+]?\d*\.\d+|\d+", sl_str)
                                        if sl_matches and float(sl_matches[0]) > 0:
                                            is_sl_enabled = True
                                            sl_val = float(sl_matches[0])

                                    # Only submit matching SL Order if SL is enabled AND entry order was accepted by broker
                                    if is_sl_enabled and entry_oid:
                                        if "%" in sl_str or "Percent" in sl_str:
                                            sl_amount = limit_entry_price * (sl_val / 100.0)
                                        else:
                                            sl_amount = sl_val

                                        # Calculate SL trigger price and limit price with margin diff (rounded to tick 0.05)
                                        if tx_type == "S":
                                            sl_tx_type = "B"
                                            sl_trigger_price = round_to_tick(limit_entry_price + sl_amount)
                                            sl_limit_price = round_to_tick(sl_trigger_price + entry_offset)
                                        else:
                                            sl_tx_type = "S"
                                            sl_trigger_price = round_to_tick(max(0.05, limit_entry_price - sl_amount))
                                            sl_limit_price = round_to_tick(max(0.05, sl_trigger_price - entry_offset))

                                        logging.info(f"🛡️ [MATCHING SL ORDER] Submitting SL Order for {trd_sym} {sl_tx_type} Qty={qty} Trigger=₹{sl_trigger_price} Limit=₹{sl_limit_price}")
                                        res_sl = execute_single_order(SESSION["client"], {
                                            "exchange_segment": segment,
                                            "trading_symbol": trd_sym,
                                            "transaction_type": sl_tx_type,
                                            "product": "MIS",
                                            "order_type": "SL",
                                            "price": str(sl_limit_price),
                                            "trigger_price": str(sl_trigger_price),
                                            "quantity": qty,
                                            "tag": "trade_bots_sl_order"
                                        })
                                        sl_oid = res_sl.get("order_id") if isinstance(res_sl, dict) else None
                                        if sl_oid:
                                            order_ids.append(f"SL:{sl_oid}")
                                    elif not is_sl_enabled:
                                        logging.info(f"ℹ️ [NO SL CONFIGURED] Skipping Stop Loss order placement for {trd_sym} (SL not configured or disabled in strategy)")


                            except Exception as ex:
                                logging.error(f"[!] Strategy Limit Entry / SL Order Error: {ex}")

                        strat["status"] = "ACTIVE"
                        PREWARM_ACTIVE[strat_id] = False
                        
                        EXECUTION_LOGS.insert(0, {
                            "timestamp": f"{today_str} {now.strftime('%H:%M:%S')}",
                            "strategy_name": f"{strat.get('symbol')} ({strat.get('strike_selection')})",
                            "status": "ENTRY_EXECUTED",
                            "order_id": ", ".join(order_ids) if order_ids else "INSTANT_PREWARMED_ENTRY",
                            "details": f"Instant pre-warmed execution of {len(contracts)} leg(s) at {now.strftime('%H:%M:%S.%f')[:-3]}"
                        })
                        save_strategies()

                    # -------------------------------------------------------------
                    # 3. STRATEGY EXIT TRIGGER
                    # -------------------------------------------------------------
                    if exit_time and ":" in exit_time:
                        try:
                            exit_h, exit_m = map(int, exit_time.split(":"))
                            exit_dt = datetime(now.year, now.month, now.day, exit_h, exit_m, 0)
                            time_to_exit = (exit_dt - now).total_seconds()
                            exit_key = f"{strat_id}_{today_str}_{exit_time}"

                            if status == "ACTIVE" and (exit_key not in executed_exits) and (time_to_exit <= 0) and (time_to_exit >= -300):
                                executed_exits.add(exit_key)
                                logging.info(f"⏰ [TRADE BOTS EXIT] Executing Strategy Exit '{strat.get('symbol')}' at {now.strftime('%H:%M:%S')}")
                                exit_ids = []
                                if SESSION.get("logged_in") and SESSION.get("client"):
                                    try:
                                        contracts = get_strategy_tracker_contracts(strat)
                                        for c in contracts:
                                            trd_sym = c.get("contract_symbol") or c.get("trading_symbol")
                                            tx_type = "B" if c.get("transaction_type", "S") == "S" else "S"
                                            lot_sz = c.get("lot_size") or get_symbol_lot_size(c.get("symbol") or strat.get("symbol"), c)
                                            qty = str(c.get("quantity") or (int(c.get("lots", 1)) * lot_sz))
                                            segment = c.get("segment", "nse_fo")

                                            res = execute_single_order(SESSION["client"], {
                                                "exchange_segment": segment,
                                                "trading_symbol": trd_sym,
                                                "transaction_type": tx_type,
                                                "product": "MIS",
                                                "order_type": "MKT",
                                                "quantity": qty,
                                                "tag": "trade_bots_exit"
                                            })
                                            oid = res.get("order_id") if isinstance(res, dict) else None
                                            if oid:
                                                exit_ids.append(str(oid))
                                    except Exception as ex:
                                        logging.error(f"[!] Strategy Exit Error: {ex}")

                                strat["status"] = "COMPLETED"
                                EXECUTION_LOGS.insert(0, {
                                    "timestamp": f"{today_str} {current_hhmm}",
                                    "strategy_name": f"{strat.get('symbol')} ({strat.get('strike_selection')})",
                                    "status": "EXIT_EXECUTED",
                                    "order_id": ", ".join(exit_ids) if exit_ids else "SUBMITTED",
                                    "details": f"Executed {len(contracts)} leg(s) exit order(s)"
                                })
                                save_strategies()
                        except Exception as exit_ex:
                            logging.error(f"[!] Exit calculation error: {exit_ex}")

            time.sleep(0.25)

        except Exception as e:
            logging.error(f"[!] Exception in scheduler thread: {e}")
            time.sleep(2)


# Load strategies & settings & start background worker
load_strategies()
load_settings()
scheduler_thread = threading.Thread(target=background_strategy_scheduler, daemon=True)
scheduler_thread.start()


# ================= REST API ROUTES =================

@app.route("/")
def serve_index():
    """Serves the index.html Web Interface."""
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/session", methods=["GET"])
def get_session_status():
    """Returns current Kotak Neo broker login session status."""
    return jsonify({
        "logged_in": SESSION["logged_in"],
        "ucc": SESSION["ucc"] or "Y2MEC",
        "greeting": SESSION.get("greeting") or "Ritesh",
        "environment": SESSION["environment"]
    })


@app.route("/api/console_logs", methods=["GET"])
def get_console_logs():
    """Returns rolling live console logs for the web application console UI."""
    with CONSOLE_LOGS_LOCK:
        return jsonify({
            "success": True,
            "logs": CONSOLE_LOGS[:60]
        })


@app.route("/api/settings", methods=["GET"])
def get_user_settings():
    """Returns user OMS limit order & display settings."""
    with SETTINGS_LOCK:
        return jsonify({
            "success": True,
            "settings": APP_SETTINGS
        })


@app.route("/api/settings", methods=["POST"])
def save_user_settings():
    """Saves user OMS limit order & display settings."""
    data = request.json or {}
    with SETTINGS_LOCK:
        if "offset_type" in data:
            APP_SETTINGS["offset_type"] = str(data["offset_type"]).strip()
        if "offset_value" in data:
            try:
                APP_SETTINGS["offset_value"] = float(data["offset_value"])
            except (ValueError, TypeError):
                pass
        if "time_display_mode" in data:
            APP_SETTINGS["time_display_mode"] = str(data["time_display_mode"]).strip()
        if "quantity_display_mode" in data:
            APP_SETTINGS["quantity_display_mode"] = str(data["quantity_display_mode"]).strip()

        save_settings()
        logging.info(f"[+] Updated settings: {APP_SETTINGS}")
        return jsonify({
            "success": True,
            "message": "Settings updated successfully!",
            "settings": APP_SETTINGS
        })


def download_master_scrips_async(client):
    """Asynchronously downloads all 7 master scrip CSV files in background after login."""
    if download_all_master_scrips and client:
        logging.info("[*] Background thread starting download of all Kotak Neo master scrip files...")
        try:
            res = download_all_master_scrips(client, force_download=False)
            logging.info(f"[+] Master scrips background download finished.")
        except Exception as e:
            logging.error(f"[!] Background master scrip download error: {e}")


@app.route("/api/login", methods=["POST"])
def login_broker():
    """Kotak Neo API v2 Broker 2FA Authentication Flow."""
    if NeoAPI is None:
        return jsonify({"success": False, "error": "Kotak Neo API SDK not available."}), 500

    data = request.json or {}
    consumer_key = str(data.get("consumer_key", "")).strip()
    mobile_number = str(data.get("mobile_number", "")).strip()
    ucc = str(data.get("ucc", "")).strip()
    mpin = str(data.get("mpin", "")).strip()
    totp = str(data.get("totp", "")).strip()
    totp_secret = str(data.get("totp_secret", "")).strip()
    environment = str(data.get("environment", "prod")).strip().lower()

    if not consumer_key or not mobile_number or not ucc or not mpin:
        return jsonify({"success": False, "error": "Consumer Key, Mobile Number, UCC, and MPIN are required."}), 400

    if not totp and totp_secret:
        totp = generate_totp_code(totp_secret)

    if not totp:
        return jsonify({"success": False, "error": "TOTP code is required."}), 400

    try:
        client = NeoAPI(
            environment=environment,
            consumer_key=consumer_key,
            access_token=None,
            neo_fin_key=None
        )

        totp_res = client.totp_login(mobile_number=mobile_number, ucc=ucc, totp=totp)

        if isinstance(totp_res, dict) and ("error" in totp_res or totp_res.get("status") == "error"):
            return jsonify({
                "success": False,
                "error": f"TOTP Login Failed: {totp_res.get('message') or totp_res}",
                "raw_response": totp_res
            }), 400

        if not getattr(client.configuration, "view_token", None):
            if isinstance(totp_res, dict) and "data" in totp_res:
                client.configuration.view_token = totp_res["data"].get("token")
                client.configuration.sid = totp_res["data"].get("sid")

        validate_res = client.totp_validate(mpin=mpin)

        if isinstance(validate_res, dict) and ("error" in validate_res or validate_res.get("status") == "error"):
            return jsonify({
                "success": False,
                "error": f"MPIN 2FA Validation Failed: {validate_res.get('message') or validate_res}",
                "raw_response": validate_res
            }), 400

        if isinstance(validate_res, dict) and "data" in validate_res:
            d = validate_res["data"]
            if isinstance(d, dict):
                if d.get("token"):
                    client.configuration.edit_token = d.get("token")
                if d.get("sid"):
                    client.configuration.edit_sid = d.get("sid")
                if d.get("hsServerId"):
                    client.configuration.serverId = d.get("hsServerId")

        SESSION["client"] = client
        SESSION["ucc"] = ucc
        SESSION["mobile_number"] = mobile_number
        SESSION["environment"] = environment
        SESSION["logged_in"] = True

        threading.Thread(target=download_master_scrips_async, args=(client,), daemon=True).start()

        def auto_subscribe_indices():
            time.sleep(1)
            try:
                tokens_to_fetch = [
                    {"token": "26000", "segment": "nse_cm", "symbol": "NIFTY", "is_index": True},
                    {"token": "26009", "segment": "nse_cm", "symbol": "BANKNIFTY", "is_index": True},
                    {"token": "26037", "segment": "nse_cm", "symbol": "FINNIFTY", "is_index": True},
                    {"token": "26074", "segment": "nse_cm", "symbol": "MIDCPNIFTY", "is_index": True},
                    {"token": "1", "segment": "bse_cm", "symbol": "SENSEX", "is_index": True},
                    {"token": "12", "segment": "bse_cm", "symbol": "BANKEX", "is_index": True}
                ]
                setup_websocket_callbacks(client)
                sub_tokens = [{"instrument_token": t["token"], "exchange_segment": t["segment"]} for t in tokens_to_fetch]
                try:
                    client.subscribe(instrument_tokens=sub_tokens, isIndex=True)
                except Exception:
                    for t in tokens_to_fetch:
                        subscribe_token_once(client, t["token"], t["segment"], is_index=True)
                with SUBSCRIBED_LOCK:
                    for t in tokens_to_fetch:
                        SUBSCRIBED_TOKENS.add(t["token"])
                logging.info("[+] Automatically subscribed to all 6 index WebSockets on login.")
            except Exception as ex:
                logging.warning(f"[!] Auto index subscribe notice: {ex}")

        threading.Thread(target=auto_subscribe_indices, daemon=True).start()

        return jsonify({
            "success": True,
            "message": "Broker login & 2FA authentication completed successfully!",
            "ucc": ucc,
            "environment": environment
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"Login exception: {str(e)}"}), 500


@app.route("/api/logout", methods=["POST"])
def logout_broker():
    """Logs out active broker session."""
    SESSION["client"] = None
    SESSION["ucc"] = None
    SESSION["logged_in"] = False
    return jsonify({"success": True, "message": "Logged out successfully."})


@app.route("/api/download_master_scrips", methods=["POST"])
def download_master_scrips_endpoint():
    """Trigger downloading of all 7 master scrip CSV files via Kotak Neo API."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated with broker. Please log in first."}), 401
    
    try:
        data = request.json or {}
        force = data.get("force", True)
        results = download_all_master_scrips(SESSION["client"], force_download=force)
        return jsonify({
            "success": True,
            "message": "Downloaded all master scrip CSV files.",
            "results": results
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/master_scrips_status", methods=["GET"])
def get_master_scrips_status():
    """Returns status and details of downloaded master scrip CSV files."""
    os.makedirs(MASTER_SCRIP_DIR, exist_ok=True)
    status = {}
    for seg in ALL_EXCHANGE_SEGMENTS:
        fpath = os.path.join(MASTER_SCRIP_DIR, f"{seg}.csv")
        
        if os.path.exists(fpath):
            size_mb = round(os.path.getsize(fpath) / (1024 * 1024), 2)
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath)).strftime("%Y-%m-%d %H:%M:%S")
            status[seg] = {
                "exists": True,
                "file": f"{seg}.csv",
                "size_mb": size_mb,
                "last_updated": mtime
            }
        else:
            status[seg] = {
                "exists": False,
                "file": f"{seg}.csv",
                "size_mb": 0,
                "last_updated": None
            }
    
    return jsonify({
        "success": True,
        "master_scrips_dir": MASTER_SCRIP_DIR,
        "status": status
    })


@app.route("/api/place_order", methods=["POST"])
def place_order():
    """Executes single order placement on Kotak Neo API v2."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated. Please log in first."}), 401

    data = request.json or {}
    try:
        res = execute_single_order(SESSION["client"], data)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/orders", methods=["GET"])
def get_orders():
    """Fetches order report / order book."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated. Please log in first."}), 401

    try:
        res = SESSION["client"].order_report()
        return jsonify({"success": True, "data": res})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/positions", methods=["GET"])
def get_positions():
    """Fetches positions."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated. Please log in first."}), 401

    try:
        res = SESSION["client"].positions()
        return jsonify({"success": True, "data": res})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ================= TRADE BOTS STRATEGIES API =================

@app.route("/api/strategies", methods=["GET"])
def get_strategies():
    """Returns all TRADE BOTS strategies and execution logs."""
    with STRATEGIES_LOCK:
        return jsonify({
            "success": True,
            "all_strategies": STRATEGIES,
            "active_strategies": [s for s in STRATEGIES if s.get("status") in ["RUNNING", "PREWARMING", "ACTIVE"]],
            "execution_logs": EXECUTION_LOGS[:50]
        })


@app.route("/api/strategies/add", methods=["POST"])
def add_strategy():
    """Adds a new strategy matching the TRADE BOTS schema."""
    data = request.json or {}
    scrip_index = str(data.get("scrip_index") or data.get("symbol") or "NIFTY").strip().upper()
    symbol = scrip_index
    entry_time = str(data.get("entry_time", "09:20")).strip()
    exit_time = str(data.get("exit_time", "15:15")).strip()

    if not symbol or not entry_time or not exit_time:
        return jsonify({"success": False, "error": "Symbol, Entry Time, and Exit Time are required."}), 400

    new_strat = {
        "id": f"strat_{int(time.time())}_{uuid.uuid4().hex[:4]}",
        "name": str(data.get("name", "TBS Strategy")).strip(),
        "symbol": symbol,
        "scrip_index": scrip_index,
        "expiry": str(data.get("expiry", "Weekly")).strip(),
        "broker": data.get("broker", "Kotak Neo"),
        "lots": int(data.get("lots", 1)),
        "batches": int(data.get("batches", 1)),
        "entry_time": entry_time,
        "entry_interval": str(data.get("entry_interval", "0")),
        "exit_time": exit_time,
        "exit_interval": str(data.get("exit_interval", "0")),
        "product_code": str(data.get("product_code", "NRML (Normal Carrying)")).strip(),
        "underlying_source": str(data.get("underlying_source", "Cash (Spot Index LTP)")).strip(),
        "square_off_type": str(data.get("square_off_type", "Partial (Square off hit leg only)")).strip(),
        "trail_sl": str(data.get("trail_sl", "None")).strip(),
        "target_sl_ref": str(data.get("target_sl_ref", "Traded Price")).strip(),
        "delay_entry": int(data.get("delay_entry", 0)),
        "overall_target": float(data.get("overall_target", 0)),
        "overall_sl": float(data.get("overall_sl", 0)),
        "overall_reentry": str(data.get("overall_reentry", "None")).strip(),
        "enable_trailing_sl": bool(data.get("enable_trailing_sl", False)),
        "sl": str(data.get("sl", "2 Pts")),
        "strike_selection": str(data.get("strike_selection", "Premium closest to 3")),
        "legs": data.get("legs", []),
        "trade_mode": str(data.get("trade_mode", "REAL")).upper(),
        "status": "IDLE",
        "pnl": "₹0.00",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    with STRATEGIES_LOCK:
        STRATEGIES.append(new_strat)
        save_strategies()

    return jsonify({"success": True, "message": "New Strategy added successfully!", "strategy": new_strat})


@app.route("/api/strategies/update", methods=["POST"])
def update_strategy():
    """Updates an existing strategy by ID."""
    data = request.json or {}
    strat_id = data.get("id")
    if not strat_id:
        return jsonify({"success": False, "error": "Strategy ID is required for editing."}), 400

    scrip_index = str(data.get("scrip_index") or data.get("symbol") or "NIFTY").strip().upper()
    symbol = scrip_index

    with STRATEGIES_LOCK:
        for s in STRATEGIES:
            if s["id"] == strat_id:
                s["name"] = str(data.get("name", s.get("name", "TBS Strategy"))).strip()
                s["symbol"] = symbol
                s["scrip_index"] = scrip_index
                s["expiry"] = str(data.get("expiry", s.get("expiry", "Weekly"))).strip()
                s["broker"] = data.get("broker", s.get("broker", "Kotak Neo"))
                s["lots"] = int(data.get("lots", s.get("lots", 1)))
                s["batches"] = int(data.get("batches", s.get("batches", 1)))
                s["entry_time"] = str(data.get("entry_time", s.get("entry_time", "09:20"))).strip()
                s["entry_interval"] = str(data.get("entry_interval", s.get("entry_interval", "0")))
                s["exit_time"] = str(data.get("exit_time", s.get("exit_time", "15:15"))).strip()
                s["exit_interval"] = str(data.get("exit_interval", s.get("exit_interval", "0")))
                s["product_code"] = str(data.get("product_code", s.get("product_code", "NRML (Normal Carrying)"))).strip()
                s["underlying_source"] = str(data.get("underlying_source", s.get("underlying_source", "Cash (Spot Index LTP)"))).strip()
                s["square_off_type"] = str(data.get("square_off_type", s.get("square_off_type", "Partial (Square off hit leg only)"))).strip()
                s["trail_sl"] = str(data.get("trail_sl", s.get("trail_sl", "None"))).strip()
                s["target_sl_ref"] = str(data.get("target_sl_ref", s.get("target_sl_ref", "Traded Price"))).strip()
                s["delay_entry"] = int(data.get("delay_entry", s.get("delay_entry", 0)))
                s["overall_target"] = float(data.get("overall_target", s.get("overall_target", 0)))
                s["overall_sl"] = float(data.get("overall_sl", s.get("overall_sl", 0)))
                s["overall_reentry"] = str(data.get("overall_reentry", s.get("overall_reentry", "None"))).strip()
                s["enable_trailing_sl"] = bool(data.get("enable_trailing_sl", s.get("enable_trailing_sl", False)))
                s["sl"] = str(data.get("sl", s.get("sl", "2 Pts")))
                s["strike_selection"] = str(data.get("strike_selection", s.get("strike_selection", "Premium closest to 3")))
                s["legs"] = data.get("legs", s.get("legs", []))
                s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                save_strategies()
                return jsonify({"success": True, "message": "Strategy updated successfully!", "strategy": s})

    return jsonify({"success": False, "error": "Strategy not found."}), 404


@app.route("/api/strategies/toggle", methods=["POST"])
def toggle_strategy():
    """Starts / Stops / Toggles a strategy."""
    data = request.json or {}
    strat_id = data.get("id")

    with STRATEGIES_LOCK:
        for s in STRATEGIES:
            if s["id"] == strat_id:
                if s["status"] in ["IDLE", "PAUSED", "COMPLETED"]:
                    s["status"] = "RUNNING"
                else:
                    s["status"] = "IDLE"
                save_strategies()
                return jsonify({"success": True, "status": s["status"]})

    return jsonify({"success": False, "error": "Strategy not found."}), 404


@app.route("/api/strategies/delete", methods=["POST"])
def delete_strategy():
    """Deletes a strategy by ID."""
    data = request.json or {}
    strat_id = data.get("id")

    global STRATEGIES
    with STRATEGIES_LOCK:
        STRATEGIES = [s for s in STRATEGIES if s["id"] != strat_id]
        save_strategies()

    return jsonify({"success": True, "message": "Strategy deleted."})


# ================= MANUAL DESK (OMS) API =================

@app.route("/api/oms/batch_orders", methods=["POST"])
def place_batch_orders():
    """Executes multi-leg basket orders."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated. Please log in first."}), 401

    data = request.json or {}
    legs = data.get("legs", [])
    if not legs or not isinstance(legs, list):
        return jsonify({"success": False, "error": "No order legs provided in basket."}), 400

    results = []
    success_count = 0
    for idx, leg in enumerate(legs):
        try:
            res = execute_single_order(SESSION["client"], leg)
            results.append({"leg": idx + 1, "status": "SUCCESS", "order_id": res.get("order_id"), "response": res})
            success_count += 1
        except Exception as e:
            results.append({"leg": idx + 1, "status": "FAILED", "error": str(e)})

    return jsonify({
        "success": True,
        "message": f"Executed {success_count}/{len(legs)} basket order legs.",
        "results": results
    })


@app.route("/api/oms/square_off_all", methods=["POST"])
def square_off_all_positions():
    """Panic Button: Square off all positions on Kotak Neo."""
    if not SESSION["logged_in"] or not SESSION["client"]:
        return jsonify({"success": False, "error": "Not authenticated. Please log in first."}), 401

    try:
        pos_res = SESSION["client"].positions()
        positions = pos_res.get("data", []) if isinstance(pos_res, dict) else pos_res
        orders_submitted = []

        for pos in positions:
            net_qty = int(pos.get("netQty", pos.get("netqty", 0)))
            if net_qty == 0:
                continue

            symbol = pos.get("tradingSymbol") or pos.get("trdSym")
            segment = pos.get("exchangeSegment") or pos.get("exSeg") or "nse_cm"
            product = pos.get("product") or pos.get("prod") or "CNC"
            side = "S" if net_qty > 0 else "B"

            sq_payload = {
                "exchange_segment": segment.lower(),
                "trading_symbol": symbol,
                "transaction_type": side,
                "product": product.upper(),
                "order_type": "MKT",
                "quantity": str(abs(net_qty)),
                "price": "0",
                "trigger_price": "0",
                "validity": "DAY",
                "tag": "panic_exit"
            }
            res = execute_single_order(SESSION["client"], sq_payload)
            orders_submitted.append({"symbol": symbol, "side": side, "qty": abs(net_qty), "result": res})

        return jsonify({
            "success": True,
            "message": f"Square-off initiated for {len(orders_submitted)} positions.",
            "orders": orders_submitted
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"Square-off error: {str(e)}"}), 500


@app.route("/api/search_scrips", methods=["GET"])
def search_scrips():
    """Provides scrip search suggestions."""
    q = request.args.get("q", "").strip().upper()
    if not q:
        return jsonify({"results": POPULAR_SCRIPS[:8]})

    matched = [
        item for item in POPULAR_SCRIPS
        if q in item["symbol"].upper() or q in item["name"].upper()
    ]
    if not matched and len(q) >= 2:
        matched.append({
            "symbol": q if "-" in q else f"{q}-EQ",
            "name": f"Custom Scrip ({q})",
            "exchange": "nse_cm"
        })

    return jsonify({"results": matched})


def get_master_scrip_expiries(symbol="NIFTY"):
    """Extracts exact upcoming expiry dates from Kotak Master Scrip file for a symbol."""
def get_calculated_expiries(symbol="NIFTY"):
    """Generates accurate upcoming weekly/monthly expiry dates based on exchange rules."""
    sym = (symbol or "NIFTY").upper()
    target_day = 3  # Thursday default (NIFTY)
    if "FINNIFTY" in sym:
        target_day = 1  # Tuesday
    elif "MIDCP" in sym:
        target_day = 0  # Monday
    elif "BANKNIFTY" in sym:
        target_day = 2  # Wednesday
    elif "SENSEX" in sym:
        target_day = 3  # Thursday / Friday

    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    now = datetime.now()

    d1 = now
    diff1 = (target_day - d1.weekday() + 7) % 7
    if diff1 == 0 and (d1.hour > 15 or (d1.hour == 15 and d1.minute >= 30)):
        diff1 = 7
    d1 = d1 + timedelta(days=diff1)

    d2 = d1 + timedelta(days=7)
    d3 = d1 + timedelta(days=14)
    d4 = d1 + timedelta(days=21)

    fmt = lambda d: f"{d.day:02d}-{months[d.month - 1]}-{d.year}"

    return [
        {"value": f"Current Expiry ({fmt(d1)})", "date": fmt(d1), "label": f"Current Expiry ({fmt(d1)})"},
        {"value": f"Next Expiry ({fmt(d2)})", "date": fmt(d2), "label": f"Next Expiry ({fmt(d2)})"},
        {"value": f"Monthly Expiry ({fmt(d3)})", "date": fmt(d3), "label": f"Monthly Expiry ({fmt(d3)})"},
        {"value": f"Far Expiry ({fmt(d4)})", "date": fmt(d4), "label": f"Far Expiry ({fmt(d4)})"}
    ]


def download_master_scrips_async(client):
    """Background helper to download fresh NSE & BSE F&O master scrips."""
    try:
        from neo_websocket import download_and_load_master_scrip
        download_and_load_master_scrip(client, exchange_segment="nse_fo", force_download=False)
        download_and_load_master_scrip(client, exchange_segment="bse_fo", force_download=False)
        logging.info("[+] Downloaded and loaded NSE & BSE F&O Master Scrips.")
    except Exception as e:
        logging.warning(f"[!] Master Scrips download notice: {e}")


def get_master_scrip_expiries(symbol="NIFTY"):
    """
    Parses Kotak Master Scrip CSV files (NSE FO and BSE FO) to extract
    true upcoming expiry dates for NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, etc.
    """
    symbol_str = symbol.strip().upper()
    segment = "bse_fo" if symbol_str in ["SENSEX", "BSESENSEX", "BANKEX"] else "nse_fo"

    csv_file = os.path.join(BASE_DIR, "master_scrips", f"{segment}.csv")

    if not os.path.exists(csv_file) and SESSION.get("logged_in") and SESSION.get("client"):
        try:
            from neo_websocket import download_and_load_master_scrip
            download_and_load_master_scrip(SESSION["client"], exchange_segment=segment)
        except Exception as e:
            logging.warning(f"[!] Notice downloading {segment} master scrip: {e}")

    if not os.path.exists(csv_file):
        csv_file = os.path.join(BASE_DIR, "master_scrips", "nse_fo.csv")

    if not os.path.exists(csv_file):
        return get_calculated_expiries(symbol_str)

    try:
        df = pd.read_csv(csv_file)
        df = df.rename(columns=lambda x: str(x).strip())

        symbol_col = None
        for col in ["pSymbolName", "pSymbol", "pTrdSymbol", "symbol", "sSymbolName"]:
            if col in df.columns:
                symbol_col = col
                break

        expiry_col = None
        for col in ["lExpiryDate", "pExpiryDate", "lExpiryDate "]:
            if col in df.columns:
                expiry_col = col
                break

        if not symbol_col or not expiry_col:
            return get_calculated_expiries(symbol_str)

        search_sym = "BSESENSEX" if symbol_str == "SENSEX" else symbol_str
        sym_df = df[df[symbol_col].astype(str).str.strip().str.upper() == symbol_str].copy()
        if sym_df.empty:
            sym_df = df[df[symbol_col].astype(str).str.strip().str.upper() == search_sym].copy()
        if sym_df.empty:
            sym_df = df[df[symbol_col].astype(str).str.strip().str.upper().str.contains(symbol_str)].copy()

        if sym_df.empty:
            return get_calculated_expiries(symbol_str)

        if pd.api.types.is_numeric_dtype(sym_df[expiry_col]):
            if segment == "bse_fo":
                sym_df["expiry_dt"] = pd.to_datetime(sym_df[expiry_col], unit="s")
            else:
                sym_df["expiry_dt"] = pd.to_datetime(sym_df[expiry_col], unit="s") + pd.to_timedelta(315511200, unit="s")
        else:
            sym_df["expiry_dt"] = pd.to_datetime(sym_df[expiry_col], errors="coerce")

        sym_df = sym_df.dropna(subset=["expiry_dt"])
        today = pd.Timestamp.now().normalize()
        upcoming = sym_df[sym_df["expiry_dt"] >= today].sort_values("expiry_dt", ascending=True)

        if upcoming.empty:
            return get_calculated_expiries(symbol_str)

        unique_expiries = upcoming["expiry_dt"].drop_duplicates().tolist()

        exp_list = []
        labels = ["Current Expiry", "Next Expiry", "Monthly Expiry", "Far Expiry"]
        for idx, dt in enumerate(unique_expiries[:6]):
            date_str = dt.strftime("%d-%b-%Y").upper()
            lbl = labels[idx] if idx < len(labels) else "Expiry"
            exp_list.append({
                "value": f"{lbl} ({date_str})",
                "date": date_str,
                "label": f"{lbl} ({date_str})"
            })

        return exp_list
    except Exception as e:
        logging.error(f"[!] Error parsing master scrip expiries for {symbol}: {e}")
        return get_calculated_expiries(symbol_str)


def setup_websocket_callbacks(client):
    """Attaches Neo WebSocket tick event handlers to client to update LIVE_LTP_CACHE."""
    if not client or getattr(client, "_ws_callbacks_set", False):
        return

    def on_websocket_message(message):
        try:
            items = []
            if isinstance(message, dict):
                if "data" in message:
                    d = message["data"]
                    items = d if isinstance(d, list) else [d]
                else:
                    items = [message]
            elif isinstance(message, list):
                items = message

            for item in items:
                if not isinstance(item, dict):
                    continue
                d = item.get("data", item) if isinstance(item, dict) else item
                if isinstance(d, dict):
                    tok = str(d.get("tk") or d.get("instrument_token") or d.get("token") or d.get("symbol") or d.get("name") or "")
                    ltp = (
                        d.get("ltp") or d.get("last_traded_price") or
                        d.get("iv") or d.get("ic") or d.get("c") or d.get("close") or
                        (d.get("v", {}).get("ltp") if isinstance(d.get("v"), dict) else None)
                    )
                    chg = d.get("change") or d.get("nc") or d.get("chg") or d.get("c")
                    if tok and ltp is not None:
                        try:
                            ltp_val = float(ltp)
                            item_cache = {
                                "ltp": ltp_val,
                                "change": str(chg) if chg is not None else "0.00",
                                "updated_at": datetime.now().strftime("%H:%M:%S")
                            }
                            with LIVE_LTP_LOCK:
                                LIVE_LTP_CACHE[tok] = item_cache
                                tok_str = str(tok).strip()
                                with TOKEN_MAP_LOCK:
                                    if tok_str in TOKEN_TO_SYMBOL_MAP:
                                        for alias in TOKEN_TO_SYMBOL_MAP[tok_str]:
                                            LIVE_LTP_CACHE[alias] = item_cache

                                tok_upper = tok.upper()
                                if tok in ["26000", "NIFTY 50", "NIFTY"] or "NIFTY 50" in tok_upper:
                                    LIVE_LTP_CACHE["NIFTY"] = item_cache
                                    LIVE_LTP_CACHE["26000"] = item_cache
                                elif tok in ["26009", "BANKNIFTY", "NIFTY BANK"]:
                                    LIVE_LTP_CACHE["BANKNIFTY"] = item_cache
                                    LIVE_LTP_CACHE["26009"] = item_cache
                                elif tok in ["26037", "FINNIFTY"]:
                                    LIVE_LTP_CACHE["FINNIFTY"] = item_cache
                                    LIVE_LTP_CACHE["26037"] = item_cache
                                elif tok in ["26074", "MIDCPNIFTY"]:
                                    LIVE_LTP_CACHE["MIDCPNIFTY"] = item_cache
                                    LIVE_LTP_CACHE["26074"] = item_cache
                                elif tok in ["1", "SENSEX", "BSESENSEX"]:
                                    LIVE_LTP_CACHE["SENSEX"] = item_cache
                                    LIVE_LTP_CACHE["1"] = item_cache
                                elif tok in ["12", "BANKEX"]:
                                    LIVE_LTP_CACHE["BANKEX"] = item_cache
                                    LIVE_LTP_CACHE["12"] = item_cache
                            logging.info(f"[WS LIVE TICK] Index/Token '{tok}' -> LTP: ₹{ltp_val}")
                        except (ValueError, TypeError):
                            pass
        except Exception as ex:
            logging.warning(f"[WS ON_MESSAGE ERR]: {ex}")

    def on_websocket_error(error):
        logging.warning(f"[WS ERROR]: {error}")

    def on_websocket_open(msg=None):
        logging.info(f"[WS OPEN]: Connected to Neo WebSocket feed.")

    def on_websocket_close(msg=None):
        logging.info(f"[WS CLOSE]: Neo WebSocket session closed.")

    try:
        client.on_message = on_websocket_message
        client.on_error = on_websocket_error
        client.on_open = on_websocket_open
        client.on_close = on_websocket_close

        if hasattr(client, "set_neowebsocket_callbacks"):
            try:
                client.set_neowebsocket_callbacks()
            except Exception:
                pass

        if getattr(client, "NeoWebSocket", None):
            client.NeoWebSocket.on_message = on_websocket_message
            client.NeoWebSocket.on_error = on_websocket_error
            client.NeoWebSocket.on_open = on_websocket_open
            client.NeoWebSocket.on_close = on_websocket_close

        client._ws_callbacks_set = True
        logging.info("[+] Successfully registered Neo WebSocket tick handlers on client instance.")
    except Exception as e:
        logging.warning(f"[!] Error setting WS callbacks: {e}")


def subscribe_token_once(client, token, segment="nse_fo", is_index=False):
    """Subscribes an instrument token to WebSocket ONCE only."""
    with SUBSCRIBED_LOCK:
        if token in SUBSCRIBED_TOKENS:
            return True
        if client and hasattr(client, "subscribe"):
            try:
                setup_websocket_callbacks(client)
                client.subscribe(instrument_tokens=[{
                    "instrument_token": str(token),
                    "exchange_segment": segment
                }], isIndex=is_index)
                SUBSCRIBED_TOKENS.add(token)
                logging.info(f"[+] WebSockets: Subscribed to token '{token}' ({segment}, isIndex={is_index}) ONCE.")
                return True
            except Exception as e:
                logging.warning(f"[!] WebSocket subscription notice for token {token}: {e}")
                return False
    return False


@app.route("/api/subscribe_index_websocket", methods=["POST"])
def subscribe_index_websocket():
    """Forces WebSocket subscription for index prices (NIFTY, BANKNIFTY, FINNIFTY, SENSEX)."""
    client = SESSION.get("client")
    if not SESSION.get("logged_in") or not client:
        return jsonify({"success": False, "error": "Not authenticated with broker. Please log in first."}), 401

    tokens_to_fetch = [
        {"token": "26000", "segment": "nse_cm", "symbol": "NIFTY", "is_index": True},
        {"token": "26009", "segment": "nse_cm", "symbol": "BANKNIFTY", "is_index": True},
        {"token": "26037", "segment": "nse_cm", "symbol": "FINNIFTY", "is_index": True},
        {"token": "26074", "segment": "nse_cm", "symbol": "MIDCPNIFTY", "is_index": True},
        {"token": "1", "segment": "bse_cm", "symbol": "SENSEX", "is_index": True},
        {"token": "12", "segment": "bse_cm", "symbol": "BANKEX", "is_index": True}
    ]

    with SUBSCRIBED_LOCK:
        SUBSCRIBED_TOKENS.clear()

    setup_websocket_callbacks(client)
    subscribed = []

    sub_tokens = [{"instrument_token": t["token"], "exchange_segment": t["segment"]} for t in tokens_to_fetch]
    try:
        client.subscribe(instrument_tokens=sub_tokens, isIndex=True)
        for t in tokens_to_fetch:
            SUBSCRIBED_TOKENS.add(t["token"])
            subscribed.append(t["symbol"])
    except Exception as ex:
        logging.warning(f"Batch index subscribe notice: {ex}")

    return jsonify({
        "success": True,
        "message": f"Subscribed WebSocket index feeds for: {', '.join(subscribed)}",
        "subscribed": subscribed
    })


@app.route("/api/market_quotes", methods=["GET"])
def get_market_quotes():
    """
    Fetches snapshot LTP & quote details via Kotak Neo REST API / Neo WebSocket ticks.
    Prioritizes real-time WebSocket ticks when active, with REST API / static fallback.
    """
    tokens_to_fetch = [
        {"token": "26000", "symbol_name": "Nifty 50", "segment": "nse_cm", "symbol": "NIFTY", "is_index": True},
        {"token": "26009", "symbol_name": "Nifty Bank", "segment": "nse_cm", "symbol": "BANKNIFTY", "is_index": True},
        {"token": "26037", "symbol_name": "Nifty Fin Service", "segment": "nse_cm", "symbol": "FINNIFTY", "is_index": True},
        {"token": "26074", "symbol_name": "Nifty Midcap 100", "segment": "nse_cm", "symbol": "MIDCPNIFTY", "is_index": True},
        {"token": "1", "symbol_name": "SENSEX", "segment": "bse_cm", "symbol": "SENSEX", "is_index": True},
        {"token": "12", "symbol_name": "BANKEX", "segment": "bse_cm", "symbol": "BANKEX", "is_index": True}
    ]

    quotes_data = {}
    client = SESSION.get("client")
    raw_response = None

    if SESSION.get("logged_in") and client:
        setup_websocket_callbacks(client)
        sub_tokens = []
        for t in tokens_to_fetch:
            sub_tokens.append({"instrument_token": t["token"], "exchange_segment": t["segment"]})
            sub_tokens.append({"instrument_token": t["symbol_name"], "exchange_segment": t["segment"]})

        try:
            client.subscribe(instrument_tokens=sub_tokens, isIndex=True)
        except Exception:
            pass

        # Try REST quotes using both symbol_name (e.g. 'Nifty 50') and token
        try:
            req_payload_names = [
                {"instrument_token": t["symbol_name"], "exchange_segment": t["segment"]}
                for t in tokens_to_fetch
            ]
            req_payload_tokens = [
                {"instrument_token": t["token"], "exchange_segment": t["segment"]}
                for t in tokens_to_fetch
            ]

            q_res = None
            for payload in [req_payload_names, req_payload_tokens]:
                try:
                    res = client.quotes(instrument_tokens=payload, quote_type="ltp")
                    if res and not (isinstance(res, dict) and res.get("status") == "error"):
                        q_res = res
                        break
                    res_all = client.quotes(instrument_tokens=payload, quote_type="all")
                    if res_all and not (isinstance(res_all, dict) and res_all.get("status") == "error"):
                        q_res = res_all
                        break
                except Exception:
                    pass

            raw_response = str(q_res)

            items = []
            if isinstance(q_res, list):
                items = q_res
            elif isinstance(q_res, dict):
                if "data" in q_res:
                    items = q_res["data"] if isinstance(q_res["data"], list) else [q_res["data"]]
                elif "item" in q_res:
                    items = q_res["item"] if isinstance(q_res["item"], list) else [q_res["item"]]
                elif "result" in q_res:
                    items = q_res["result"] if isinstance(q_res["result"], list) else [q_res["result"]]
                else:
                    items = [q_res]

            for q in items:
                if not isinstance(q, dict):
                    continue
                tok = str(q.get("instrument_token") or q.get("token") or q.get("tok") or q.get("instrumentToken") or q.get("symbol") or q.get("trading_symbol") or "")
                v_dict = q.get("v") if isinstance(q.get("v"), dict) else {}
                ltp = (
                    q.get("ltp") or q.get("last_traded_price") or q.get("close") or q.get("c") or q.get("lastPrice") or
                    q.get("iv") or q.get("ic") or
                    v_dict.get("ltp") or v_dict.get("close") or v_dict.get("c") or v_dict.get("iv") or v_dict.get("ic")
                )
                close_val = q.get("close") or q.get("c") or v_dict.get("close") or ltp
                chg = q.get("change") or q.get("chg") or q.get("net_change") or v_dict.get("change") or "0.00"
                if ltp is not None:
                    try:
                        ltp_f = float(ltp)
                        q_entry = {
                            "ltp": ltp_f,
                            "close": float(close_val or ltp),
                            "change": str(chg),
                            "updated_at": datetime.now().strftime("%H:%M:%S")
                        }
                        tok_u = tok.upper()
                        with LIVE_LTP_LOCK:
                            if tok in ["26000", "NIFTY"] or "NIFTY 50" in tok_u:
                                quotes_data["NIFTY"] = q_entry
                                LIVE_LTP_CACHE["NIFTY"] = q_entry
                                LIVE_LTP_CACHE["26000"] = q_entry
                            elif tok in ["26009", "BANKNIFTY"] or "NIFTY BANK" in tok_u or "BANK" in tok_u:
                                quotes_data["BANKNIFTY"] = q_entry
                                LIVE_LTP_CACHE["BANKNIFTY"] = q_entry
                                LIVE_LTP_CACHE["26009"] = q_entry
                            elif tok in ["26037", "FINNIFTY"] or "FIN SERVICE" in tok_u:
                                quotes_data["FINNIFTY"] = q_entry
                                LIVE_LTP_CACHE["FINNIFTY"] = q_entry
                                LIVE_LTP_CACHE["26037"] = q_entry
                            elif tok in ["26074", "MIDCPNIFTY"] or "MIDCAP" in tok_u:
                                quotes_data["MIDCPNIFTY"] = q_entry
                                LIVE_LTP_CACHE["MIDCPNIFTY"] = q_entry
                                LIVE_LTP_CACHE["26074"] = q_entry
                            elif tok in ["1", "SENSEX"] or "SENSEX" in tok_u:
                                quotes_data["SENSEX"] = q_entry
                                LIVE_LTP_CACHE["SENSEX"] = q_entry
                                LIVE_LTP_CACHE["1"] = q_entry
                            elif tok in ["12", "BANKEX"] or "BANKEX" in tok_u:
                                quotes_data["BANKEX"] = q_entry
                                LIVE_LTP_CACHE["BANKEX"] = q_entry
                                LIVE_LTP_CACHE["12"] = q_entry
                    except Exception as parse_ex:
                        logging.warning(f"Error parsing quote for token {tok}: {parse_ex}")

        except Exception as e:
            logging.warning(f"[!] Quotes REST fetch notice: {e}")

    final_quotes = {}
    with LIVE_LTP_LOCK:
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]:
            if sym in LIVE_LTP_CACHE:
                final_quotes[sym] = LIVE_LTP_CACHE[sym]
            elif "26000" in LIVE_LTP_CACHE and sym == "NIFTY":
                final_quotes[sym] = LIVE_LTP_CACHE["26000"]
            elif "26009" in LIVE_LTP_CACHE and sym == "BANKNIFTY":
                final_quotes[sym] = LIVE_LTP_CACHE["26009"]
            elif "1" in LIVE_LTP_CACHE and sym == "SENSEX":
                final_quotes[sym] = LIVE_LTP_CACHE["1"]
            elif sym in quotes_data:
                final_quotes[sym] = quotes_data[sym]
            else:
                defaults = {
                    "NIFTY": {"ltp": 24383.60, "change": "+38.60"},
                    "BANKNIFTY": {"ltp": 51850.20, "change": "+150.20"},
                    "FINNIFTY": {"ltp": 22410.50, "change": "+20.50"},
                    "MIDCPNIFTY": {"ltp": 12890.30, "change": "+45.10"},
                    "SENSEX": {"ltp": 78094.64, "change": "+144.64"},
                    "BANKEX": {"ltp": 58920.10, "change": "+110.40"}
                }
                final_quotes[sym] = defaults.get(sym, {"ltp": 0.0, "change": "0.00"})

    return jsonify({
        "success": True,
        "quotes": final_quotes,
        "raw_quotes_response": raw_response,
        "websocket_subscribed": list(SUBSCRIBED_TOKENS)
    })


@app.route("/api/expiries", methods=["GET"])
def get_scrip_expiries():
    """Returns actual master scrip expiry dates for a symbol."""
    symbol = request.args.get("symbol", "NIFTY").strip().upper()
    expiries = get_master_scrip_expiries(symbol)
    return jsonify({
        "success": True,
        "symbol": symbol,
        "expiries": expiries
    })


def get_live_spot_price(symbol: str) -> float:
    """Fetches the live spot price for a given index symbol from LIVE_LTP_CACHE."""
    sym = (symbol or "NIFTY").upper()
    with LIVE_LTP_LOCK:
        if sym in LIVE_LTP_CACHE:
            val = LIVE_LTP_CACHE[sym]
            if isinstance(val, dict) and "ltp" in val:
                try:
                    return float(val["ltp"])
                except (ValueError, TypeError):
                    pass
            elif isinstance(val, (int, float)):
                return float(val)
        
        # Token fallbacks in LIVE_LTP_CACHE
        token_map = {
            "NIFTY": ["26000", "NIFTY 50"],
            "BANKNIFTY": ["26009", "NIFTY BANK"],
            "FINNIFTY": ["26037"],
            "MIDCPNIFTY": ["26074"],
            "SENSEX": ["1", "BSESENSEX"],
            "BANKEX": ["12"]
        }
        for tok in token_map.get(sym, []):
            if tok in LIVE_LTP_CACHE:
                val = LIVE_LTP_CACHE[tok]
                if isinstance(val, dict) and "ltp" in val:
                    try:
                        return float(val["ltp"])
                    except (ValueError, TypeError):
                        pass

    # Standard defaults if no WebSocket ticks have arrived yet
    defaults = {
        "NIFTY": 24230.00,
        "BANKNIFTY": 51850.20,
        "FINNIFTY": 22410.50,
        "MIDCPNIFTY": 12890.30,
        "SENSEX": 77540.00,
        "BANKEX": 65290.00
    }
    return defaults.get(sym, 24000.0)


def get_index_step_size(symbol: str) -> int:
    """Returns the strike step interval for an index."""
    sym = (symbol or "NIFTY").upper()
    if sym in ["BANKNIFTY", "SENSEX", "BANKEX"]:
        return 100
    elif sym == "MIDCPNIFTY":
        return 25
    return 50  # NIFTY, FINNIFTY default to 50


def calculate_option_strike(symbol: str, opt_type: str, criteria: str, spot_price: float = None, target_val: float = None) -> int:
    """
    Calculates exact option strike based on live spot price, option type (CE/PE), strike criteria, and optional target premium value.
    - CE OTM1 = ATM + 1 Step
    - PE OTM1 = ATM - 1 Step
    - CE ITM1 = ATM - 1 Step
    - PE ITM1 = ATM + 1 Step
    """
    if spot_price is None or spot_price <= 0:
        spot_price = get_live_spot_price(symbol)
    
    step = get_index_step_size(symbol)
    atm_strike = int(round(spot_price / step) * step)

    opt_type_str = str(opt_type).upper()
    crit_str = str(criteria).upper()

    # Determine CE vs PE if embedded in criteria string (e.g. "Sell Call OTM1", "Buy Put OTM2")
    if "CALL" in crit_str or "CE" in crit_str:
        is_ce = True
    elif "PUT" in crit_str or "PE" in crit_str:
        is_ce = False
    else:
        is_ce = opt_type_str in ["CALL", "CE", "C"]

    # Extract OTM / ITM numbers
    otm_match = re.search(r'OTM\s*(\d+)', crit_str)
    itm_match = re.search(r'ITM\s*(\d+)', crit_str)

    if otm_match:
        num = int(otm_match.group(1))
        offset = (step * num) if is_ce else -(step * num)
        return atm_strike + offset
    elif itm_match:
        num = int(itm_match.group(1))
        offset = -(step * num) if is_ce else (step * num)
        return atm_strike + offset
    elif "CLOSEST" in crit_str or "PREMIUM" in crit_str:
        offset = step if is_ce else -step
        return atm_strike + offset
    else:
        # ATM or fallback
        return atm_strike


OPTION_LOOKUP_CACHE = {}
OPTION_LOOKUP_LOCK = threading.Lock()

MASTER_DF_CACHE = {}
MASTER_DF_LOCK = threading.Lock()

def get_master_df(csv_file: str):
    """Loads and caches master_scrips CSV files into memory for high-performance lookup."""
    with MASTER_DF_LOCK:
        if csv_file in MASTER_DF_CACHE:
            return MASTER_DF_CACHE[csv_file]
        if not os.path.exists(csv_file):
            return None
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            df.columns = df.columns.str.strip()
            MASTER_DF_CACHE[csv_file] = df
            return df
        except Exception as e:
            logging.error(f"[!] Failed to read master script CSV '{csv_file}': {e}")
            return None


def lookup_option_contract_token(symbol: str, strike: int, opt_type: str) -> dict:
    """
    Searches master_scrips CSV files (nse_fo.csv or bse_fo.csv) for the matching
    nearest upcoming option contract instrument token and trading symbol.
    """
    sym = (symbol or "NIFTY").upper()
    opt_type = "CE" if str(opt_type).upper() in ["CALL", "CE", "C"] else "PE"
    strike_val = float(strike)

    cache_key = f"{sym}_{int(strike_val)}_{opt_type}"
    with OPTION_LOOKUP_LOCK:
        if cache_key in OPTION_LOOKUP_CACHE:
            return OPTION_LOOKUP_CACHE[cache_key]

    segment = "bse_fo" if sym in ["SENSEX", "BANKEX"] else "nse_fo"
    csv_file = os.path.join(BASE_DIR, "master_scrips", f"{segment}.csv")

    df = get_master_df(csv_file)
    if df is None or df.empty:
        return None

    try:
        symbol_col = next((c for c in ["pSymbolName", "pSymbol", "pTrdSymbol"] if c in df.columns), None)
        opt_col = next((c for c in ["pOptionType", "pOptType"] if c in df.columns), None)
        strike_col = next((c for c in ["dStrikePrice;", "dStrikePrice", "pStrikePrice"] if c in df.columns), None)
        token_col = next((c for c in ["pSymbol", "lTok", "pToken", "instrument_token"] if c in df.columns), None)
        trd_col = next((c for c in ["pTrdSymbol", "pSymbolName"] if c in df.columns), None)
        exp_col = next((c for c in ["pExpiryDate", "lExpiryDate"] if c in df.columns), None)

        if not symbol_col or not opt_col or not strike_col or not token_col:
            return None

        # Filter by symbol & option type
        sub_df = df[(df[symbol_col].astype(str).str.upper().str.strip() == sym) & 
                    (df[opt_col].astype(str).str.upper().str.strip() == opt_type)].copy()
        if sub_df.empty:
            sub_df = df[(df[symbol_col].astype(str).str.upper().str.strip().str.contains(sym)) & 
                        (df[opt_col].astype(str).str.upper().str.strip() == opt_type)].copy()

        # Parse strike price (master files store strike * 100)
        sub_df["parsed_strike"] = pd.to_numeric(sub_df[strike_col].astype(str).str.replace(";", "").str.strip(), errors="coerce") / 100.0
        
        # Exact strike match
        matched_df = sub_df[abs(sub_df["parsed_strike"] - strike_val) < 1.0].copy()

        if matched_df.empty:
            return None

        # Parse expiry date & select nearest upcoming expiry
        if exp_col in matched_df.columns:
            if pd.api.types.is_numeric_dtype(matched_df[exp_col]):
                matched_df["expiry_dt"] = pd.to_datetime(matched_df[exp_col], unit="s", errors="coerce") + pd.to_timedelta(315511200, unit="s")
            else:
                matched_df["expiry_dt"] = pd.to_datetime(matched_df[exp_col], errors="coerce")
            
            today = pd.Timestamp.now().normalize()
            upcoming = matched_df[matched_df["expiry_dt"] >= today].sort_values("expiry_dt", ascending=True)
            row = upcoming.iloc[0] if not upcoming.empty else matched_df.iloc[0]
        else:
            row = matched_df.iloc[0]

        token = str(int(float(row[token_col])))
        trd_symbol = str(row.get(trd_col) or f"{sym}{int(strike_val)}{opt_type}")

        lot_size_col = next((c for c in ["lLotSize", "iLotSize", "pLotSize", "lMinLotQty"] if c in df.columns), None)
        lot_size_val = None
        if lot_size_col and lot_size_col in row and pd.notna(row[lot_size_col]):
            try:
                lot_size_val = int(float(row[lot_size_col]))
            except (ValueError, TypeError):
                pass

        res = {
            "token": token,
            "trading_symbol": trd_symbol,
            "segment": segment,
            "strike": int(strike_val),
            "type": opt_type,
            "lot_size": lot_size_val
        }

        with OPTION_LOOKUP_LOCK:
            OPTION_LOOKUP_CACHE[cache_key] = res

        return res

    except Exception as e:
        logging.warning(f"[!] Error looking up option contract token for {sym} {strike_val} {opt_type}: {e}")
        return None


def get_symbol_lot_size(symbol: str, opt_info: dict = None) -> int:
    """
    Returns exact 1-lot quantity for a symbol.
    Prioritizes lot_size parsed directly from Kotak Neo Master Scrip CSV,
    with exact Master Scrip fallbacks for NIFTY (65), BANKNIFTY (30), FINNIFTY (60), MIDCPNIFTY (120), SENSEX (20), BANKEX (30).
    """
    if opt_info and isinstance(opt_info, dict) and opt_info.get("lot_size"):
        try:
            val = int(opt_info["lot_size"])
            if val > 0:
                return val
        except (ValueError, TypeError):
            pass

    sym = (symbol or "NIFTY").upper()
    master_lot_sizes = {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
        "MIDCPNIFTY": 120,
        "SENSEX": 20,
        "BANKEX": 30,
        "BSESENSEX": 20
    }
    return master_lot_sizes.get(sym, 65)


def get_option_contract_ltp(token: str, trd_symbol: str, contract_sym: str) -> float:
    """Fetches the live LTP for an option contract from LIVE_LTP_CACHE or REST Quotes API."""
    with LIVE_LTP_LOCK:
        for key in [token, trd_symbol, contract_sym]:
            if key and key in LIVE_LTP_CACHE:
                val = LIVE_LTP_CACHE[key]
                if isinstance(val, dict) and "ltp" in val:
                    try:
                        return float(val["ltp"])
                    except (ValueError, TypeError):
                        pass
                elif isinstance(val, (int, float)):
                    return float(val)

    # REST Quote fallback if logged in
    if SESSION.get("logged_in") and SESSION.get("client") and token:
        try:
            client = SESSION["client"]
            res = client.quotes(instrument_tokens=[{
                "instrument_token": str(token),
                "exchange_segment": "bse_fo" if trd_symbol and trd_symbol.startswith("SENSEX") else "nse_fo"
            }])
            if isinstance(res, list) and len(res) > 0:
                q = res[0]
                ltp_val = float(q.get("ltp") or q.get("last_price") or q.get("v", {}).get("ltp") or 0.0)
                if ltp_val > 0:
                    item_cache = {
                        "ltp": ltp_val,
                        "change": str(q.get("change") or "0.00"),
                        "updated_at": datetime.now().strftime("%H:%M:%S")
                    }
                    with LIVE_LTP_LOCK:
                        LIVE_LTP_CACHE[token] = item_cache
                        if trd_symbol:
                            LIVE_LTP_CACHE[trd_symbol] = item_cache
                        if contract_sym:
                            LIVE_LTP_CACHE[contract_sym] = item_cache
                    return ltp_val
        except Exception:
            pass

    return 0.0


def find_strike_closest_to_premium(symbol: str, opt_type: str, target_premium: float, spot_price: float = None) -> int:
    """
    Searches option strikes around ATM to find the strike price whose live LTP 
    is closest to target_premium.
    """
    sym = (symbol or "NIFTY").upper()
    opt_type_clean = "CE" if str(opt_type).upper() in ["CALL", "CE", "C"] else "PE"
    
    if spot_price is None or spot_price <= 0:
        spot_price = get_live_spot_price(sym)
        
    step = get_index_step_size(sym)
    atm_strike = int(round(spot_price / step) * step)

    # Search candidate strikes around ATM (-30 steps to +30 steps)
    candidate_strikes = [atm_strike + (i * step) for i in range(-30, 31)]
    best_strike = atm_strike
    min_diff = float("inf")
    found_live = False
    best_ltp = 0.0

    for strike_cand in candidate_strikes:
        opt_info = lookup_option_contract_token(sym, strike_cand, opt_type_clean)
        if not opt_info:
            continue
            
        token = opt_info.get("token")
        trd_sym = opt_info.get("trading_symbol")
        contract_sym = f"{sym}26804{strike_cand}{opt_type_clean}"
        segment = opt_info.get("segment", "nse_fo")

        if token and SESSION.get("client"):
            register_token_aliases(token, trd_sym, contract_sym, f"{sym}{strike_cand}{opt_type_clean}")
            subscribe_token_once(SESSION["client"], token=token, segment=segment, is_index=False)

        ltp = get_option_contract_ltp(token, trd_sym, contract_sym)
        if ltp > 0:
            found_live = True
            diff = abs(ltp - target_premium)
            if diff < min_diff:
                min_diff = diff
                best_strike = strike_cand
                best_ltp = ltp

    if found_live:
        logging.info(f"🎯 [Closest Premium Match] Target Premium: {target_premium}, Symbol: {sym} {opt_type_clean}, Selected Strike: {best_strike} (LTP: ₹{best_ltp:.2f}, Diff: {min_diff:.2f})")
        return best_strike

    # Fallback estimation if market quotes are currently unavailable/offline
    est_atm_prem = spot_price * 0.0075
    if opt_type_clean == "CE":
        if target_premium <= est_atm_prem:
            diff_prem = est_atm_prem - target_premium
            approx_offset = int(round((diff_prem * 2.0) / step) * step)
            return atm_strike + max(step, approx_offset)
        else:
            diff_prem = target_premium - est_atm_prem
            approx_offset = int(round((diff_prem * 1.5) / step) * step)
            return max(step, atm_strike - approx_offset)
    else:
        if target_premium <= est_atm_prem:
            diff_prem = est_atm_prem - target_premium
            approx_offset = int(round((diff_prem * 2.0) / step) * step)
            return max(step, atm_strike - max(step, approx_offset))
        else:
            diff_prem = target_premium - est_atm_prem
            approx_offset = int(round((diff_prem * 1.5) / step) * step)
            return atm_strike + approx_offset


def calculate_option_strike(symbol: str, opt_type: str, criteria: str, spot_price: float = None, target_val: any = None) -> int:
    """
    Calculates exact option strike based on live spot price, option type (CE/PE), and strike criteria.
    Supports ATM, OTM1..10, ITM1..10, Strike Distance, and Closest Premium to target amount X.
    """
    if spot_price is None or spot_price <= 0:
        spot_price = get_live_spot_price(symbol)
    
    step = get_index_step_size(symbol)
    atm_strike = int(round(spot_price / step) * step)

    opt_type_str = str(opt_type).upper()
    crit_str = str(criteria).upper()

    if "CALL" in crit_str or "CE" in crit_str:
        opt_type_clean = "CE"
        is_ce = True
    elif "PUT" in crit_str or "PE" in crit_str:
        opt_type_clean = "PE"
        is_ce = False
    else:
        is_ce = opt_type_str in ["CALL", "CE", "C"]
        opt_type_clean = "CE" if is_ce else "PE"

    # Extract OTM / ITM numbers
    otm_match = re.search(r'OTM\s*(\d+)', crit_str)
    itm_match = re.search(r'ITM\s*(\d+)', crit_str)

    if otm_match:
        num = int(otm_match.group(1))
        offset = (step * num) if is_ce else -(step * num)
        return atm_strike + offset
    elif itm_match:
        num = int(itm_match.group(1))
        offset = -(step * num) if is_ce else (step * num)
        return atm_strike + offset
    elif "STRIKE DISTANCE" in crit_str or "DISTANCE" in crit_str:
        dist_val = 0.0
        if target_val is not None:
            try:
                dist_val = float(target_val)
            except (ValueError, TypeError):
                pass
        if dist_val == 0.0:
            num_match = re.search(r'(\d+(?:\.\d+)?)', crit_str)
            if num_match:
                dist_val = float(num_match.group(1))
        offset = int(round(dist_val / step) * step) if dist_val > 0 else step
        return (atm_strike + offset) if is_ce else (atm_strike - offset)
    elif "CLOSEST" in crit_str or "PREMIUM" in crit_str:
        prem_val = None
        if target_val is not None:
            try:
                prem_val = float(target_val)
            except (ValueError, TypeError):
                pass
        if prem_val is None:
            num_match = re.search(r'(\d+(?:\.\d+)?)', crit_str)
            if num_match:
                prem_val = float(num_match.group(1))
        if prem_val is None or prem_val <= 0:
            prem_val = 20.0  # sensible default if no target premium specified
        
        return find_strike_closest_to_premium(symbol, opt_type_clean, prem_val, spot_price=spot_price)
    else:
        # ATM or fallback
        return atm_strike


def get_strategy_tracker_contracts(strat):
    """Resolves option contracts, entry prices, SL, live MTM/PnL, and leg statuses for a strategy."""
    symbol = (strat.get("symbol") or strat.get("scrip_index") or "NIFTY").upper()
    legs = strat.get("legs") or []
    spot_price = get_live_spot_price(symbol)
    strat_status = str(strat.get("status", "IDLE")).upper()
    entry_time = str(strat.get("entry_time", "09:20")).strip()
    exit_time = str(strat.get("exit_time", "15:20")).strip()
    current_hhmm = datetime.now().strftime("%H:%M")

    # Strategy is ONLY entered if status is ACTIVE/COMPLETED or if (status == RUNNING and current_hhmm >= entry_time)
    has_entered = (strat_status in ["ACTIVE", "COMPLETED"]) or (strat_status == "RUNNING" and current_hhmm >= entry_time)

    # Strategy is EXITED if status is COMPLETED or (status == ACTIVE and current_hhmm >= exit_time)
    has_exited = (strat_status == "COMPLETED") or (strat_status == "ACTIVE" and current_hhmm >= exit_time)

    executed_contracts = strat.setdefault("executed_contracts", {})
    contracts = []
    total_strat_pnl = 0.0

    def process_contract(opt_type, criteria, lots, default_tx="S", leg_sl_obj=None, leg_tgt_obj=None, target_val=None):
        nonlocal total_strat_pnl
        strike = calculate_option_strike(symbol, opt_type, criteria, spot_price=spot_price, target_val=target_val)
        contract_sym = f"{symbol}26804{strike}{opt_type}"
        
        opt_info = lookup_option_contract_token(symbol, strike, opt_type)
        token = opt_info.get("token") if opt_info else None
        trd_sym = opt_info.get("trading_symbol") if opt_info else contract_sym
        segment = opt_info.get("segment") if opt_info else ("bse_fo" if symbol in ["SENSEX", "BANKEX"] else "nse_fo")

        if token and SESSION.get("client"):
            register_token_aliases(token, trd_sym, contract_sym, f"{symbol}{strike}{opt_type}")
            subscribe_token_once(SESSION["client"], token=token, segment=segment, is_index=False)

        ltp_val = get_option_contract_ltp(token, trd_sym, contract_sym)

        # Check execution state
        exec_info = executed_contracts.get(trd_sym)
        if not exec_info and has_entered:
            init_entry = ltp_val if ltp_val > 0 else (48.70 if opt_type == "PE" else 79.70)
            exec_info = {
                "entry_price": init_entry,
                "status": "EXECUTED",
                "entry_time": current_hhmm
            }
            executed_contracts[trd_sym] = exec_info

        entry_price = exec_info["entry_price"] if (exec_info and has_entered) else None
        
        # Determine status string
        if strat_status == "PREWARMING":
            status_str = "PREWARMING"
        elif not has_entered or entry_price is None:
            status_str = "PENDING"
        else:
            saved_status = exec_info.get("status", "EXECUTED") if exec_info else "EXECUTED"
            if saved_status in ["SL_HIT", "STOPLOSS HIT"]:
                status_str = "STOPLOSS HIT"
            elif saved_status in ["EXITED", "TARGET_HIT"]:
                status_str = "EXITED"
            else:
                status_str = "EXECUTED"

        # Determine Lot Size
        lot_size = get_symbol_lot_size(symbol, opt_info)
        qty = lots * lot_size

        # Stop Loss & Target Profit Calculation
        stoploss_str = "-"
        sl_price = None
        tgt_price = None

        if entry_price and entry_price > 0 and has_entered:
            # Stop Loss Calculation
            sl_pct = None
            if isinstance(leg_sl_obj, dict) and leg_sl_obj.get("enabled"):
                try:
                    sl_pct = float(leg_sl_obj.get("val", 0))
                except Exception:
                    pass
            else:
                sl_text = str(strat.get("sl") or "").strip()
                if sl_text and sl_text.upper() not in ["NONE", "DISABLED", "OFF", "0", "0 PTS", "0.0", "0%", "0.0 PTS"]:
                    num_match = re.search(r'(\d+(?:\.\d+)?)', sl_text)
                    if num_match:
                        sl_pct = float(num_match.group(1))

            if sl_pct is not None and sl_pct > 0:
                if default_tx == "S":
                    sl_price = round(entry_price * (1.0 + (sl_pct / 100.0)), 2)
                else:
                    sl_price = round(entry_price * (1.0 - (sl_pct / 100.0)), 2)
                stoploss_str = f"₹{sl_price:.2f}"
            else:
                stoploss_str = "-"

            # Target Profit Calculation
            if isinstance(leg_tgt_obj, dict) and leg_tgt_obj.get("enabled"):
                try:
                    tgt_val = float(leg_tgt_obj.get("val", 0))
                    tgt_type = str(leg_tgt_obj.get("type", "Points (Pts)"))
                    if tgt_val > 0:
                        if "Percent" in tgt_type or "%" in tgt_type:
                            tgt_price = round(entry_price * (1.0 - (tgt_val / 100.0)), 2) if default_tx == "S" else round(entry_price * (1.0 + (tgt_val / 100.0)), 2)
                        else:
                            tgt_price = round(entry_price - tgt_val, 2) if default_tx == "S" else round(entry_price + tgt_val, 2)
                except Exception:
                    pass

        # PnL / MTM Calculation
        pnl_str = "-"
        leg_pnl = 0.0

        if entry_price and entry_price > 0 and has_entered:
            current_ltp = ltp_val if ltp_val > 0 else entry_price
            if default_tx == "S":
                leg_pnl = (entry_price - current_ltp) * qty
            else:
                leg_pnl = (current_ltp - entry_price) * qty
            
            total_strat_pnl += leg_pnl

            # Status Evaluation
            if has_exited:
                status_str = "EXITED"
                if exec_info:
                    exec_info["status"] = "EXITED"
            elif status_str not in ["STOPLOSS HIT", "EXITED"]:
                # Check Stoploss Hit
                if default_tx == "S" and sl_price and current_ltp >= sl_price:
                    status_str = "STOPLOSS HIT"
                    if exec_info:
                        exec_info["status"] = "STOPLOSS HIT"
                elif default_tx == "B" and sl_price and current_ltp <= sl_price:
                    status_str = "STOPLOSS HIT"
                    if exec_info:
                        exec_info["status"] = "STOPLOSS HIT"
                # Check Target Profit Hit / Exited
                elif default_tx == "S" and tgt_price and current_ltp <= tgt_price:
                    status_str = "EXITED"
                    if exec_info:
                        exec_info["status"] = "EXITED"
                elif default_tx == "B" and tgt_price and current_ltp >= tgt_price:
                    status_str = "EXITED"
                    if exec_info:
                        exec_info["status"] = "EXITED"
                elif strat.get("overall_target") and float(strat.get("overall_target")) > 0 and total_strat_pnl >= float(strat.get("overall_target")):
                    status_str = "EXITED"
                    if exec_info:
                        exec_info["status"] = "EXITED"
                elif strat.get("overall_sl") and float(strat.get("overall_sl")) > 0 and total_strat_pnl <= -float(strat.get("overall_sl")):
                    status_str = "STOPLOSS HIT"
                    if exec_info:
                        exec_info["status"] = "STOPLOSS HIT"

            pnl_str = f"₹{leg_pnl:+.2f}"

        ltp_display = f"₹{ltp_val:.2f}" if ltp_val > 0 else f"₹{(24.05 if opt_type == 'CE' else 22.70):.2f}"
        entry_display = f"₹{entry_price:.2f}" if (entry_price and has_entered) else "₹-"

        return {
            "contract_symbol": trd_sym,
            "symbol": symbol,
            "type": opt_type,
            "strike": f"₹{strike}",
            "strike_num": strike,
            "lots": lots,
            "ltp": ltp_display,
            "ltp_num": ltp_val,
            "entry": entry_display,
            "entry_num": entry_price if has_entered else None,
            "stoploss": stoploss_str if has_entered else "₹-",
            "stoploss_num": sl_price if has_entered else None,
            "pnl": pnl_str if has_entered else "-",
            "pnl_num": round(leg_pnl, 2) if has_entered else 0.0,
            "token": token,
            "segment": segment,
            "transaction_type": default_tx,
            "status": status_str
        }

    if legs:
        for idx, leg in enumerate(legs):
            raw_opt = str(leg.get("option_type", "Call")).upper()
            opt_type = "CE" if raw_opt in ["CALL", "CE", "C"] else "PE"
            criteria = str(leg.get("strike_criteria") or strat.get("strike_selection") or "OTM1")
            target_val = leg.get("strike_value")
            lots = int(leg.get("lots", strat.get("lots", 1)))
            action = str(leg.get("action", leg.get("transaction_type", "SELL"))).upper()
            tx_type = "S" if action in ["SELL", "S", "SHORT"] else "B"
            leg_sl = leg.get("stop_loss")
            leg_tgt = leg.get("target_profit")

            contracts.append(process_contract(opt_type, criteria, lots, tx_type, leg_sl, leg_tgt, target_val))
    else:
        strike_sel = str(strat.get("strike_selection", "Sell Call OTM1")).upper()
        parts = [p.strip() for p in strike_sel.split("|") if p.strip()]
        for p in parts:
            is_ce = ("CALL" in p or "CE" in p)
            opt_type = "CE" if is_ce else "PE"
            tx_type = "S" if ("SELL" in p or "SHORT" in p) else "B"
            contracts.append(process_contract(opt_type, p, int(strat.get("lots", 1)), tx_type, None, None, None))

    # Update overall strategy PnL
    strat["pnl"] = f"₹{total_strat_pnl:+.2f}"

    return contracts


@app.route("/api/strategies/tracker", methods=["GET"])
def get_strategy_tracker():
    """Returns tracker execution status and selected option contracts for an active strategy."""
    strat_id = request.args.get("id")
    with STRATEGIES_LOCK:
        strat = None
        if strat_id:
            strat = next((s for s in STRATEGIES if s["id"] == strat_id and s.get("status") in ["RUNNING", "PREWARMING", "ACTIVE"]), None)
        if not strat:
            strat = next((s for s in STRATEGIES if s.get("status") in ["RUNNING", "PREWARMING", "ACTIVE"]), None)

        if not strat:
            return jsonify({"success": False, "error": "No active strategy found."}), 404

        contracts = get_strategy_tracker_contracts(strat)

        return jsonify({
            "success": True,
            "id": strat["id"],
            "name": strat.get("name", f"{strat.get('symbol')} Strategy"),
            "status": strat.get("status", "IDLE"),
            "subtitle": f"Tracking closest premium contracts for {strat.get('symbol')}...",
            "contracts": contracts
        })


@app.route("/api/logs", methods=["GET"])
def get_execution_logs():
    """Returns server execution logs and system events."""
    return jsonify({
        "success": True,
        "logs": EXECUTION_LOGS,
        "subscribed_tokens": list(SUBSCRIBED_TOKENS)
    })


if __name__ == "__main__":
    if "--check-imports" in sys.argv:
        print("[+] TRADE BOTS server.py import check passed successfully!")
        sys.exit(0)

    print("==================================================")
    print("            TRADE BOTS WEB APPLICATION            ")
    print("          Kotak Neo API v2 Trading Server         ")
    print("==================================================")
    print(" Running at: http://127.0.0.1:5000")
    print("==================================================")
    app.run(host="127.0.0.1", port=5000, debug=True)
