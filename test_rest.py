import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

print("[1/5] Imports loaded successfully.")

load_dotenv()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
SYMBOL = "NSE:NIFTY50-INDEX"

print(f"[2/5] Client ID loaded: {CLIENT_ID}")


def get_access_token():
    if not os.path.exists("access_token.txt"):
        raise FileNotFoundError("access_token.txt not found in this folder!")
    with open("access_token.txt", "r") as f:
        return f.read().strip()


try:
    token = get_access_token()
    print("[3/5] Access token loaded from file.")

    to_date = datetime.now()
    from_date = to_date - timedelta(days=5)

    url = "https://api-t1.fyers.in/data/history"
    headers = {
        "Authorization": f"{CLIENT_ID}:{token}"
    }
    params = {
        "symbol": SYMBOL,
        "resolution": "5",
        "date_format": "1",
        "range_from": from_date.strftime("%Y-%m-%d"),
        "range_to": to_date.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }

    print("[4/5] Sending direct HTTP request to FYERS REST API...")
    response = requests.get(url, headers=headers, params=params, timeout=10)

    print(f"[5/5] HTTP Status Code: {response.status_code}")
    data = response.json()

    print("\n================ FYERS RESPONSE ================")
    print(f"Status ('s'): {data.get('s')}")

    if data.get("s") == "ok" and "candles" in data:
        candles = data["candles"]
        print(f"SUCCESS! Total Candles Received: {len(candles)}")
        print("\nLast 3 Candles:")
        for c in candles[-3:]:
            readable = datetime.fromtimestamp(c[0]).strftime("%Y-%m-%d %H:%M")
            print(f"{readable} | Open: {c[1]} | High: {c[2]} | Low: {c[3]} | Close: {c[4]}")
    else:
        print("API Error Response:")
        print(json.dumps(data, indent=2))

except Exception as e:
    print(f"\nCRITICAL ERROR: {e}")