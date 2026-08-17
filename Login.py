"""
Kotak Neo API v2 Login Script
Official SDK Reference: Kotak-neo-api-v2

This script handles the full 2FA authentication flow for Kotak Neo API v2:
1. Client Initialization (using Consumer Key / Access Token)
2. 1st Factor Authentication via TOTP (using Mobile Number, UCC, and TOTP)
3. 2nd Factor Authentication via MPIN Validation
4. Session Verification
"""

import os
import sys
import getpass

# Ensure local SDK 'Kotak-neo-api-v2' is accessible in sys.path
sdk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Kotak-neo-api-v2")
if os.path.exists(sdk_path) and sdk_path not in sys.path:
    sys.path.insert(0, sdk_path)

try:
    from neo_api_client import NeoAPI
except ImportError:
    print("Error: Could not import 'neo_api_client'. Ensure 'Kotak-neo-api-v2' SDK is present or installed.")
    sys.exit(1)


# ==========================================
# CONFIGURATION CREDENTIALS
# ==========================================
# Enter your Kotak Neo API credentials below,
# or leave them empty ("") to be prompted interactively at runtime.

CONSUMER_KEY = ""    # API / Consumer Key from Kotak Neo app/website (Invest tab -> Trade API)
MOBILE_NUMBER = ""   # Registered mobile number with country code (e.g. "+919876543210" or "919876543210")
UCC = ""             # Unique Client Code (Profile section in Neo App)
MPIN = ""            # 6-digit MPIN for your Kotak Neo account
TOTP = ""            # 6-digit TOTP from Google Authenticator / Auth app
ACCESS_TOKEN = None  # Optional Access Token if available (default: None)
ENVIRONMENT = "prod" # Environment: 'prod' for Live Trading, 'uat' for Testing
TOTP_SECRET = ""     # Optional: 32-character TOTP secret key for automatic TOTP generation


def get_totp(totp_secret: str = "") -> str:
    """Auto-generates TOTP if TOTP_SECRET is provided and pyotp is installed."""
    if totp_secret:
        try:
            import pyotp
            totp_code = pyotp.TOTP(totp_secret).now()
            print(f"[+] Auto-generated TOTP code: {totp_code}")
            return totp_code
        except ImportError:
            print("[!] 'pyotp' package not installed. Falling back to manual TOTP input.")
        except Exception as e:
            print(f"[!] Error generating TOTP: {e}")
    return ""


def login_kotak_neo():
    """
    Executes the Kotak Neo API v2 login flow:
    - Step 1: Initialize NeoAPI client
    - Step 2: Perform TOTP login (First factor authentication)
    - Step 3: Validate MPIN (2FA completion)
    
    Returns:
        NeoAPI: Authenticated client instance if login succeeds, None otherwise.
    """
    print("==================================================")
    print("       KOTAK NEO API v2 LOGIN AUTHENTICATION      ")
    print("==================================================")

    # Prompt user interactively if credentials are missing
    consumer_key = CONSUMER_KEY or input("Enter Consumer Key (API Token): ").strip()
    mobile_number = MOBILE_NUMBER or input("Enter Registered Mobile Number (with country code, e.g. +91XXXXXXXXXX): ").strip()
    ucc = UCC or input("Enter UCC (Unique Client Code): ").strip()
    
    mpin = str(MPIN).strip() if MPIN else ""
    if not mpin:
        mpin = input("Enter 6-digit numeric MPIN: ").strip()

    if not mpin.isdigit():
        print(f"[!] Warning: MPIN '{mpin}' contains non-numeric characters. Kotak Neo MPIN must be a 6-digit number.")
        mpin = input("Please re-enter your 6-digit numeric MPIN (numbers only): ").strip()

    print(f"[+] Loaded credentials for UCC: {ucc} | MPIN length: {len(mpin)} digits")

    totp = str(TOTP or get_totp(TOTP_SECRET)).strip()
    if not totp:
        totp = input("Enter 6-digit TOTP code from Authenticator App: ").strip()

    # Step 1: Initialize NeoAPI Client
    print("\n[*] Step 1: Initializing NeoAPI client...")
    try:
        client = NeoAPI(
            environment=ENVIRONMENT,
            consumer_key=consumer_key,
            access_token=ACCESS_TOKEN,
            neo_fin_key=None
        )
        print("[+] Client initialized successfully.")
    except Exception as e:
        print(f"[!] Failed to initialize NeoAPI client: {e}")
        return None

    # Step 2: TOTP Login (Generates View Token & Session ID)
    print("\n[*] Step 2: Initiating TOTP login...")
    try:
        totp_login_res = client.totp_login(
            mobile_number=mobile_number,
            ucc=ucc,
            totp=totp
        )
        print(f"[*] TOTP Login Response: {totp_login_res}")

        # Check for login errors in response
        if isinstance(totp_login_res, dict) and ("error" in totp_login_res or totp_login_res.get("status") == "error"):
            print(f"[!] TOTP Login Failed: {totp_login_res}")
            return None
        
        # Verify view token generation
        if not getattr(client.configuration, "view_token", None):
            if isinstance(totp_login_res, dict) and "data" in totp_login_res:
                client.configuration.view_token = totp_login_res["data"].get("token")
                client.configuration.sid = totp_login_res["data"].get("sid")
                
        print("[+] Step 1 Authentication (TOTP Login) Success!")

    except Exception as e:
        print(f"[!] Exception during TOTP Login: {e}")
        return None

    # Step 3: Validate MPIN (Complete 2FA -> Generates Trade Token)
    print("\n[*] Step 3: Validating MPIN for 2FA completion...")
    try:
        totp_validate_res = client.totp_validate(mpin=mpin)
        print(f"[*] MPIN Validate Response: {totp_validate_res}")

        if isinstance(totp_validate_res, dict) and ("error" in totp_validate_res or totp_validate_res.get("status") == "error"):
            print(f"[!] MPIN Validation Failed: {totp_validate_res}")
            if "10300" in str(totp_validate_res):
                print("\n[!] Troubleshooting Error 10300 ('Validation Errors'):")
                print("    1. Incorrect MPIN: Ensure you entered your 6-digit numeric Kotak Neo MPIN (used to log into the mobile app).")
                print("    2. Pre-fill credentials: You can set MPIN = 'XXXXXX' at line 37 in Login.py to avoid getpass typos.")
                print("    3. TOTP Delay: Enter the TOTP code immediately when prompted so the view session does not expire.")
            return None

        # Verify edit token and edit sid set in configuration
        edit_token = getattr(client.configuration, "edit_token", None)
        edit_sid = getattr(client.configuration, "edit_sid", None)

        if edit_token and edit_sid:
            print("\n==================================================")
            print(" [SUCCESS] Kotak Neo API v2 2FA Login Completed! ")
            print("==================================================")
            return client
        else:
            print("[!] 2FA verification incomplete. Session tokens missing.")
            return None

    except Exception as e:
        print(f"[!] Exception during MPIN Validation: {e}")
        return None


if __name__ == "__main__":
    client = login_kotak_neo()

    if client:
        print("\n[*] Testing Session by fetching Order Book...")
        try:
            order_book = client.order_report()
            print(f"[+] Order Book Response: {order_book}")
        except Exception as e:
            print(f"[!] Error fetching order book: {e}")
    else:
        print("\n[!] Login process failed. Please check your credentials and try again.")
