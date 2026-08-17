"""
Kotak Neo API v2 - Order Placement Script

This script allows you to place buy/sell orders on Kotak Neo by editing
the payload dictionary directly in the code below.

API Specification Reference:
- exchange_segment : "nse_cm" (NSE Cash), "bse_cm" (BSE Cash), "nse_fo" (NSE F&O), "bse_fo" (BSE F&O), "mcx_fo" (MCX)
- product          : "CNC" (Cash & Carry / Delivery), "MIS" (Intraday), "NRML" (Normal / F&O Carryforward)
- order_type       : "MKT" (Market Order), "L" (Limit Order), "SL" (Stop Loss Limit), "SL-M" (Stop Loss Market)
- price            : Limit price as a string (e.g., "150.00"), or "0" for Market orders
- quantity         : Order quantity as a string (e.g., "1")
- validity         : "DAY" or "IOC"
- trading_symbol   : Stock / Scrip symbol (e.g., "SBIN-EQ", "RELIANCE-EQ", "INFY-EQ")
# - transaction_type : "B" / "Buy" or "S" / "Sell"
- amo              : "NO" or "YES" (After Market Order)
- trigger_price    : Trigger price as string for SL orders (e.g., "145.00"), default "0"
"""

import os
import sys

# Ensure local SDK 'Kotak-neo-api-v2' is in sys.path
sdk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Kotak-neo-api-v2")
if os.path.exists(sdk_path) and sdk_path not in sys.path:
    sys.path.insert(0, sdk_path)

from Login import login_kotak_neo

# ==============================================================================
# EDITABLE ORDER PAYLOAD
# ==============================================================================
# Edit the parameters below to configure your order before executing this script.

ORDER_PAYLOAD = {
    # 1. Exchange Segment: "nse_cm", "bse_cm", "nse_fo", "bse_fo", "mcx_fo"
    "exchange_segment": "nse_cm",

    # 2. Scrip / Stock Trading Symbol (e.g. "SBIN-EQ", "RELIANCE-EQ", "TATAMOTORS-EQ")
    "trading_symbol": "SBIN-EQ",

    # 3. Transaction Type: "B" (or "Buy") or "S" (or "Sell")
    "transaction_type": "B",

    # 4. Product Type: "CNC" (Delivery), "MIS" (Intraday), "NRML" (F&O Carryforward)
    "product": "CNC",

    # 5. Order Type: "MKT" (Market), "L" (Limit), "SL" (Stop Loss Limit), "SL-M" (Stop Loss Market)
    "order_type": "MKT",

    # 6. Quantity (Must be string, e.g. "1")
    "quantity": "1",

    # 7. Price (Must be string, e.g. "0" for Market order, or "800.50" for Limit order)
    "price": "0",

    # 8. Order Validity: "DAY" or "IOC"
    "validity": "DAY",

    # 9. Trigger Price (Must be string, required for SL / SL-M orders, default "0")
    "trigger_price": "0",

    # 10. After Market Order (AMO): "NO" or "YES"
    "amo": "NO",

    # 11. Disclosed Quantity: "0" (Optional)
    "disclosed_quantity": "0",

    # 12. Market Protection: "0" (Optional)
    "market_protection": "0",

    # 13. Portfolio Order Flag: "N" (Optional)
    "pf": "N",

    # 14. Custom Order Tag / ID for tracking (Optional)
    "tag": "python_script"
}


def place_broker_order(client, payload: dict):
    """
    Executes order placement using Kotak Neo API client and the specified payload dictionary.
    """
    # Normalize transaction type to allowed values: "B", "S", "Buy", "Sell"
    raw_tx_type = str(payload.get("transaction_type", "B")).strip()
    if raw_tx_type.upper() in ["BUY", "B"]:
        tx_type = "B"
    elif raw_tx_type.upper() in ["SELL", "S"]:
        tx_type = "S"
    else:
        tx_type = raw_tx_type

    print("\n==================================================")
    print("           PLACING ORDER VIA KOTAK NEO            ")
    print("==================================================")
    print(f"  Exchange Segment : {payload.get('exchange_segment')}")
    print(f"  Trading Symbol   : {payload.get('trading_symbol')}")
    print(f"  Transaction Type : {tx_type}")
    print(f"  Product          : {payload.get('product')}")
    print(f"  Order Type       : {payload.get('order_type')}")
    print(f"  Quantity         : {payload.get('quantity')}")
    print(f"  Price            : {payload.get('price')}")
    print(f"  Validity         : {payload.get('validity')}")
    print(f"  AMO Order        : {payload.get('amo')}")
    if payload.get('trigger_price') != "0":
        print(f"  Trigger Price    : {payload.get('trigger_price')}")
    print("==================================================")

    try:
        response = client.place_order(
            exchange_segment=payload["exchange_segment"],
            product=payload["product"],
            price=str(payload["price"]),
            order_type=payload["order_type"],
            quantity=str(payload["quantity"]),
            validity=payload["validity"],
            trading_symbol=payload["trading_symbol"],
            transaction_type=tx_type,
            amo=payload.get("amo", "NO"),
            disclosed_quantity=str(payload.get("disclosed_quantity", "0")),
            market_protection=str(payload.get("market_protection", "0")),
            pf=payload.get("pf", "N"),
            trigger_price=str(payload.get("trigger_price", "0")),
            tag=payload.get("tag", "python_order")
        )

        print("\n[*] Broker API Response:")
        print(response)

        if isinstance(response, dict) and ("order_id" in response or "nOrderNo" in response or response.get("status") == "success" or response.get("stat") == "Ok"):
            order_id = response.get("order_id") or response.get("nOrderNo") or response.get("result")
            print(f"\n[SUCCESS] Order successfully placed! Order ID: {order_id}")
        else:
            print("\n[!] Order placement response received. Check response output above.")

        return response

    except Exception as e:
        print(f"\n[!] Exception occurred while placing order: {e}")
        return None


if __name__ == "__main__":
    # Step 1: Perform 2FA authentication using Login.py
    print("[*] Authenticating with Kotak Neo API...")
    client = login_kotak_neo()

    if client:
        # Step 2: Place Order using the editable payload dictionary above
        place_broker_order(client, ORDER_PAYLOAD)
    else:
        print("\n[!] Authentication failed. Order placement cancelled.")
