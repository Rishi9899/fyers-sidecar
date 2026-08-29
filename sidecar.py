import os
import time
import json
import asyncio
import logging
import threading
import requests
from datetime import datetime
from dotenv import load_dotenv
import websockets
from fyers_apiv3.FyersWebsocket import data_ws

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
SYMBOLS = [
    # // Indices
    "NSE:NIFTY50-INDEX",
    "NSE:NIFTYBANK-INDEX",
    "NSE:FINNIFTY-INDEX",
    
    # // Equities (Stocks)
    "NSE:RELIANCE-EQ",
    "NSE:TCS-EQ",
    "NSE:INFY-EQ",
    "NSE:SBIN-EQ",
    
    # // MCX Commodities
    "MCX:CRUDEOIL26OCTFUT",
    "MCX:GOLD26OCTFUT",
    "MCX:SILVER26OCTFUT"
]

CONNECTED_CLIENTS = set()
MAIN_LOOP = None

fyers_socket = None
fyers_thread = None

# Track connection state and last active tick time
is_fyers_connected = False
last_tick_time = None


def get_access_token():
    if not os.path.exists("access_token.txt"):
        raise FileNotFoundError("access_token.txt not found!")
    with open("access_token.txt", "r") as f:
        return f.read().strip()


# ------------------------------------------------------------------
# 1. Direct REST API Call (History Fetch)
# ------------------------------------------------------------------
def fetch_historical_5m_candles(symbol: str, num_candles: int = 150):
    logging.info(f"Fetching {num_candles} historical 5m candles for {symbol}...")
    try:
        token = get_access_token()
        to_time = int(time.time())
        from_time = to_time - (7 * 86400)

        url = "https://api-t1.fyers.in/data/history"
        headers = {"Authorization": f"{CLIENT_ID}:{token}"}
        params = {
            "symbol": symbol,
            "resolution": "5",
            "date_format": "0",
            "range_from": str(from_time),
            "range_to": str(to_time),
            "cont_flag": "1"
        }

        response = requests.get(url, headers=headers, params=params, timeout=10)
        data = response.json()

        if data.get("s") == "ok" and "candles" in data:
            raw_candles = data["candles"][-num_candles:]
            return [
                {
                    "timestamp": c[0] * 1000,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5])
                }
                for c in raw_candles
            ]
        else:
            logging.error(f"Failed historical fetch for {symbol}: {data}")
            return []
    except Exception as e:
        logging.error(f"Exception during historical fetch for {symbol}: {e}", exc_info=True)
        return []


# ------------------------------------------------------------------
# 2. Live FYERS WebSocket Stream
# ------------------------------------------------------------------
def on_message(message):
    global last_tick_time
    if isinstance(message, dict) and "symbol" in message and "ltp" in message:
        last_tick_time = time.time()

        tt = message.get("tt") or message.get("last_traded_time")
        timestamp_ms = int(tt * 1000) if tt else int(time.time() * 1000)
        volume = message.get("vol_traded") or message.get("vol_traded_today") or 0

        payload = json.dumps({
            "type": "tick",
            "symbol": message["symbol"],
            "price": float(message["ltp"]),
            "volume": int(volume),
            "timestamp": timestamp_ms
        })

        if MAIN_LOOP and MAIN_LOOP.is_running():
            asyncio.run_coroutine_threadsafe(broadcast_payload(payload), MAIN_LOOP)


async def broadcast_payload(payload: str):
    """Broadcasts any payload (tick, heartbeat, history_complete) to all connected clients."""
    for ws in CONNECTED_CLIENTS.copy():
        try:
            await ws.send(payload)
        except Exception as e:
            logging.error(f"Error broadcasting to Spring Boot: {e}")


def on_error(message):
    logging.error(f"FYERS WS Error: {message}")


def on_open():
    global is_fyers_connected
    is_fyers_connected = True
    logging.info("FYERS Live WS Connected successfully! Subscribing to symbols...")
    fyers_socket.subscribe(symbols=SYMBOLS, data_type="SymbolUpdate")


def on_close(message):
    global is_fyers_connected, fyers_thread
    is_fyers_connected = False
    fyers_thread = None
    logging.warning(f"FYERS WS Disconnected: {message}")

    # Schedule auto-reconnect after 5 seconds
    if MAIN_LOOP and MAIN_LOOP.is_running():
        asyncio.run_coroutine_threadsafe(retry_fyers_connection(), MAIN_LOOP)


async def retry_fyers_connection():
    """Waits 5 seconds then attempts to reconnect the FYERS WebSocket."""
    logging.info("Will retry FYERS WebSocket connection in 5 seconds...")
    await asyncio.sleep(5)
    logging.info("Retrying FYERS WebSocket connection...")
    await asyncio.to_thread(start_fyers_socket_if_needed)


def force_reconnect_fyers():
    """Force-kill stale thread reference and reconnect from scratch."""
    global fyers_socket, fyers_thread
    logging.info("[FORCE RECONNECT] Clearing stale FYERS state and reconnecting...")
    fyers_thread = None
    fyers_socket = None
    start_fyers_socket_if_needed()


def start_fyers_socket_if_needed():
    global fyers_socket, fyers_thread

    if fyers_thread and fyers_thread.is_alive():
        logging.info("FYERS socket thread is already alive, skipping re-creation.")
        return

    def run():
        global fyers_socket, fyers_thread
        try:
            token = get_access_token()
            auth_string = f"{CLIENT_ID}:{token}"

            fyers_socket = data_ws.FyersDataSocket(
                access_token=auth_string,
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=on_open,
                on_close=on_close,
                on_error=on_error,
                on_message=on_message
            )
            fyers_socket.connect()
        except Exception as e:
            logging.error(f"FYERS socket thread crashed: {e}", exc_info=True)
            fyers_thread = None

    logging.info("Starting FYERS live WebSocket connection in background thread...")
    fyers_thread = threading.Thread(target=run, daemon=True)
    fyers_thread.start()


# ------------------------------------------------------------------
# 3. Connection Health & Heartbeat Monitor (with force-reconnect)
# ------------------------------------------------------------------
async def health_monitor_loop():
    """Periodically emits heartbeat and force-retries FYERS if stuck disconnected."""
    fyers_disconnect_count = 0

    while True:
        await asyncio.sleep(15)

        heartbeat_payload = json.dumps({
            "type": "heartbeat",
            "timestamp": int(time.time() * 1000),
            "fyersConnected": is_fyers_connected
        })
        await broadcast_payload(heartbeat_payload)

        if is_fyers_connected:
            fyers_disconnect_count = 0
            if last_tick_time is None:
                logging.info("[HEALTH OK] FYERS WS Connected | Listening for market open (No ticks received yet).")
            else:
                elapsed_seconds = int(time.time() - last_tick_time)
                logging.info(f"[HEALTH OK] FYERS WS Connected | Last tick was received {elapsed_seconds}s ago.")
        else:
            fyers_disconnect_count += 1
            logging.warning(f"[HEALTH ALERT] FYERS WS Disconnected | Consecutive checks: {fyers_disconnect_count}")

            # Force retry every 60 seconds (4 health checks × 15s) if still disconnected
            if fyers_disconnect_count % 4 == 0:
                logging.info("[HEALTH] Force-retrying FYERS WebSocket connection...")
                await asyncio.to_thread(force_reconnect_fyers)


# ------------------------------------------------------------------
# 4. Local Server Handler
# ------------------------------------------------------------------
async def local_ws_handler(websocket):
    CONNECTED_CLIENTS.add(websocket)
    logging.info("Spring Boot client connected to sidecar.")

    try:
        # Step 1: Send historical candles
        for symbol in SYMBOLS:
            candles = await asyncio.to_thread(fetch_historical_5m_candles, symbol, 500)
            if candles:
                chunk_size = 50
                for i in range(0, len(candles), chunk_size):
                    chunk = candles[i:i + chunk_size]
                    await websocket.send(json.dumps({
                        "type": "history",
                        "symbol": symbol,
                        "candles": chunk
                    }))
                    await asyncio.sleep(0.01)

                await websocket.send(json.dumps({
                    "type": "history_complete",
                    "symbol": symbol
                }))
                logging.info(f"Delivered {len(candles)} historical candles for {symbol} to Spring Boot.")

        # Step 2: Connect to FYERS WebSocket stream
        start_fyers_socket_if_needed()

        # Step 3: Keep connection open
        async for _ in websocket:
            pass
    except Exception as e:
        logging.info(f"Spring Boot client disconnected: {e}")
    finally:
        CONNECTED_CLIENTS.remove(websocket)


# ------------------------------------------------------------------
# 5. Main Entry Point
# ------------------------------------------------------------------
async def main():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    # Start health monitor background loop
    asyncio.create_task(health_monitor_loop())

    async with websockets.serve(local_ws_handler, "localhost", 8765):
        logging.info("Sidecar server listening on ws://localhost:8765")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down sidecar...")