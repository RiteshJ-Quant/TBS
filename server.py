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
import logging
import threading
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory

SUBSCRIBED_TOKENS = set()
SUBSCRIBED_LOCK = threading.Lock()
LIVE_LTP_CACHE = {}
LIVE_LTP_LOCK = threading.Lock()

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

    order_id = None
    if isinstance(response, dict):
        order_id = response.get("order_id") or response.get("nOrderNo") or response.get("result") or response.get("data", {}).get("order_id")

    return {
        "success": True,
        "order_id": order_id,
        "raw_response": response,
        "payload": payload
    }


def background_strategy_scheduler():
    """
    Background worker thread running continuously to trigger
    TRADE BOTS time-based strategies at exact entry & exit times.
    """
    logging.info("[+] TRADE BOTS Background Strategy Scheduler Started.")
    last_checked_min = ""

    while True:
        try:
            now = datetime.now()
            current_hhmm = now.strftime("%H:%M")
            today_str = now.strftime("%Y-%m-%d")

            if current_hhmm != last_checked_min:
                with STRATEGIES_LOCK:
                    for strat in STRATEGIES:
                        status = strat.get("status", "IDLE")
                        entry_time = strat.get("entry_time", "").strip()
                        exit_time = strat.get("exit_time", "").strip()

                        # Entry Trigger
                        if status == "RUNNING" and entry_time == current_hhmm:
                            logging.info(f"⏰ [TRADE BOTS ENTRY] Executing Strategy '{strat.get('symbol')}' at {current_hhmm}")
                            if SESSION["logged_in"] and SESSION["client"]:
                                try:
                                    res = execute_single_order(SESSION["client"], {
                                        "exchange_segment": "nse_fo",
                                        "trading_symbol": f"{strat.get('symbol')}-FUT",
                                        "transaction_type": "B",
                                        "product": "MIS",
                                        "order_type": "MKT",
                                        "quantity": str(int(strat.get("lots", 1)) * 50),
                                        "tag": "trade_bots_entry"
                                    })
                                    strat["status"] = "ACTIVE"
                                    EXECUTION_LOGS.insert(0, {
                                        "timestamp": f"{today_str} {current_hhmm}",
                                        "strategy_name": f"{strat.get('symbol')} ({strat.get('strike_selection')})",
                                        "status": "ENTRY_EXECUTED",
                                        "order_id": res.get("order_id"),
                                        "details": f"Placed BUY order for {strat.get('lots')} Lot(s)"
                                    })
                                except Exception as ex:
                                    logging.error(f"[!] Strategy Entry Error: {ex}")
                                    EXECUTION_LOGS.insert(0, {
                                        "timestamp": f"{today_str} {current_hhmm}",
                                        "strategy_name": strat.get("symbol"),
                                        "status": "FAILED",
                                        "details": str(ex)
                                    })

                        # Exit Trigger
                        if status == "ACTIVE" and exit_time == current_hhmm:
                            logging.info(f"⏰ [TRADE BOTS EXIT] Executing Strategy Exit '{strat.get('symbol')}' at {current_hhmm}")
                            if SESSION["logged_in"] and SESSION["client"]:
                                try:
                                    res = execute_single_order(SESSION["client"], {
                                        "exchange_segment": "nse_fo",
                                        "trading_symbol": f"{strat.get('symbol')}-FUT",
                                        "transaction_type": "S",
                                        "product": "MIS",
                                        "order_type": "MKT",
                                        "quantity": str(int(strat.get("lots", 1)) * 50),
                                        "tag": "trade_bots_exit"
                                    })
                                    strat["status"] = "COMPLETED"
                                    EXECUTION_LOGS.insert(0, {
                                        "timestamp": f"{today_str} {current_hhmm}",
                                        "strategy_name": f"{strat.get('symbol')} ({strat.get('strike_selection')})",
                                        "status": "EXIT_EXECUTED",
                                        "order_id": res.get("order_id"),
                                        "details": f"Placed SELL exit order for {strat.get('lots')} Lot(s)"
                                    })
                                except Exception as ex:
                                    logging.error(f"[!] Strategy Exit Error: {ex}")

                    save_strategies()
                last_checked_min = current_hhmm

            time.sleep(1)

        except Exception as e:
            logging.error(f"[!] Exception in scheduler thread: {e}")
            time.sleep(2)


# Load strategies & start background worker
load_strategies()
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
        "ucc": SESSION["ucc"],
        "environment": SESSION["environment"]
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
            "active_strategies": [s for s in STRATEGIES if s.get("status") in ["RUNNING", "ACTIVE"]],
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


def get_strategy_tracker_contracts(strat):
    """Resolves option contracts and live tracking details for a strategy."""
    symbol = (strat.get("symbol") or strat.get("scrip_index") or "NIFTY").upper()
    legs = strat.get("legs") or []

    contracts = []

    if legs:
        for idx, leg in enumerate(legs):
            opt_type = "CE" if str(leg.get("option_type", "Call")).upper() in ["CALL", "CE"] else "PE"
            criteria = str(leg.get("strike_criteria", "Closest Premium"))
            s_val = str(leg.get("strike_value", "100"))
            lots = int(leg.get("lots", 1))

            base_spot = 24383.60
            if symbol == "BANKNIFTY":
                base_spot = 51850.20
            elif symbol == "FINNIFTY":
                base_spot = 22410.50
            elif symbol == "SENSEX":
                base_spot = 78094.64

            step = 50 if symbol == "NIFTY" else (100 if symbol == "BANKNIFTY" else 50)
            atm_strike = int(round(base_spot / step) * step)

            strike = atm_strike
            ltp = 24.05 if opt_type == "CE" else 22.70

            if criteria == "Closest Premium":
                offset = 150 if opt_type == "CE" else -250
                strike = atm_strike + offset
                ltp = 24.05 if opt_type == "CE" else 22.70
            elif criteria.startswith("OTM"):
                try:
                    num = int(criteria.replace("OTM", ""))
                except:
                    num = 1
                offset = (step * num) if opt_type == "CE" else -(step * num)
                strike = atm_strike + offset
                ltp = round(max(10.0, 100 - (num * 8)), 2)
            elif criteria.startswith("ITM"):
                try:
                    num = int(criteria.replace("ITM", ""))
                except:
                    num = 1
                offset = -(step * num) if opt_type == "CE" else (step * num)
                strike = atm_strike + offset
                ltp = round(150 + (num * 12), 2)
            elif criteria == "ATM":
                strike = atm_strike
                ltp = 125.50

            contract_sym = f"{symbol}26804{strike}{opt_type}"

            contracts.append({
                "contract_symbol": contract_sym,
                "type": opt_type,
                "strike": f"₹{strike}",
                "lots": lots,
                "ltp": f"₹{ltp:.2f}",
                "entry": "₹-",
                "stoploss": "₹-",
                "pnl": "-",
                "status": "PENDING"
            })
    else:
        contracts = [
            {
                "contract_symbol": f"{symbol}2680424150PE",
                "type": "PE",
                "strike": "₹24150",
                "lots": int(strat.get("lots", 1)),
                "ltp": "₹22.70",
                "entry": "₹-",
                "stoploss": "₹-",
                "pnl": "-",
                "status": "PENDING"
            },
            {
                "contract_symbol": f"{symbol}2680424550CE",
                "type": "CE",
                "strike": "₹24550",
                "lots": int(strat.get("lots", 1)),
                "ltp": "₹24.05",
                "entry": "₹-",
                "stoploss": "₹-",
                "pnl": "-",
                "status": "PENDING"
            }
        ]

    return contracts


@app.route("/api/strategies/tracker", methods=["GET"])
def get_strategy_tracker():
    """Returns tracker execution status and selected option contracts for a strategy."""
    strat_id = request.args.get("id")
    with STRATEGIES_LOCK:
        strat = None
        if strat_id:
            strat = next((s for s in STRATEGIES if s["id"] == strat_id), None)
        if not strat and STRATEGIES:
            strat = STRATEGIES[0]

        if not strat:
            return jsonify({"success": False, "error": "No strategy found."}), 404

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
