import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
SYMBOL = "NSE:NIFTY50-INDEX"


def get_access_token():
    if not os.path.exists("access_token.txt"):
        raise FileNotFoundError("access_token.txt not found. Run generate_token.py first!")
    with open("access_token.txt", "r") as f:
        return f.read().strip()


def check_fyers_history():
    print(f"--- Testing FYERS REST History API for {SYMBOL} ---")

    try:
        token = get_access_token()
        fyers = fyersModel.FyersModel(client_id=CLIENT_ID, is_async=False, token=token, log_path="")

        to_date = datetime.now()
        from_date = to_date - timedelta(days=5)  # Covers weekends/holidays

        data = {
            "symbol": SYMBOL,
            "resolution": "5",  # 5-minute resolution
            "date_format": "1",
            "range_from": from_date.strftime("%Y-%m-%d"),
            "range_to": to_date.strftime("%Y-%m-%d"),
            "cont_flag": "1"
        }

        print(f"Requesting data from {data['range_from']} to {data['range_to']}...")
        response = fyers.history(data=data)

        print("\n--- RAW API RESPONSE STATUS ---")
        print("Status Code ('s'):", response.get("s"))

        if response.get("s") == "ok" and "candles" in response:
            all_candles = response["candles"]
            last_10 = all_candles[-10:]  # Pick latest 10 candles for printing

            print(f"\nSUCCESS: Total Candles Received: {len(all_candles)}")
            print("\n--- Sample (Latest 5-Minute Candles) ---")
            print(f"{'TIMESTAMP':<20} | {'OPEN':<10} | {'HIGH':<10} | {'LOW':<10} | {'CLOSE':<10} | {'VOLUME':<10}")
            print("-" * 75)

            for c in last_10:
                # Convert epoch timestamp to readable format
                readable_time = datetime.fromtimestamp(c[0]).strftime('%Y-%m-%d %H:%M')
                print(f"{readable_time:<20} | {c[1]:<10} | {c[2]:<10} | {c[3]:<10} | {c[4]:<10} | {c[5]:<10}")

        else:
            print("\nFAILED: FYERS returned an error response:")
            print(response)

    except Exception as e:
        print(f"\nEXCEPTION ERROR: {e}")


if __name__ == "__main__":
    check_fyers_history()