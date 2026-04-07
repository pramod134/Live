import asyncio
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
import websockets

# =========================
# CONFIG
# =========================
TOKEN = "PASTE_YOUR_TRADIER_TOKEN_HERE"
SYMBOLS = ["SPY", "AMD", "TSLA", "QQQ"]
FILTER = ["timesale"]

WS_URL = "wss://ws.tradier.com/v1/markets/events"
SESSION_URL = "https://api.tradier.com/v1/markets/events/session"

ET = ZoneInfo("America/New_York")
RECONNECT_DELAY_SECONDS = 3


# =========================
# HELPERS
# =========================
def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_session():
    r = requests.post(
        SESSION_URL,
        headers={
            "Authorization": f"Bearer QoiXId2fsKadZ7rOdLtUhGxMslYu",
            "Accept": "application/json",
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["stream"]["sessionid"]


# =========================
# 1M CANDLE BUILDER
# =========================
class CandleBuilder1m:
    def __init__(self):
        self.current = {}

    @staticmethod
    def minute_bucket_from_ms(ts_ms: int) -> int:
        return (ts_ms // 60000) * 60000

    def process_tick(self, symbol, price, size, ts_ms):
        if price <= 0 or size <= 0:
            return

        bucket_ms = self.minute_bucket_from_ms(ts_ms)
        cur = self.current.get(symbol)

        if cur is None:
            self.current[symbol] = {
                "bucket_ms": bucket_ms,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
                "trade_count": 1,
            }
            return

        if bucket_ms > cur["bucket_ms"]:
            self.finalize(symbol)
            self.current[symbol] = {
                "bucket_ms": bucket_ms,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
                "trade_count": 1,
            }
            return

        if bucket_ms < cur["bucket_ms"]:
            return

        cur["high"] = max(cur["high"], price)
        cur["low"] = min(cur["low"], price)
        cur["close"] = price
        cur["volume"] += size
        cur["trade_count"] += 1

    def finalize(self, symbol):
        cur = self.current.get(symbol)
        if not cur:
            return

        ts_utc = datetime.fromtimestamp(cur["bucket_ms"] / 1000, tz=timezone.utc)

        candle = {
            "symbol": symbol,
            "ts": iso_z(ts_utc),
            "open": cur["open"],
            "high": cur["high"],
            "low": cur["low"],
            "close": cur["close"],
            "volume": cur["volume"],
            "trade_count": cur["trade_count"],
        }

        print(json.dumps(candle), flush=True)

        del self.current[symbol]


# =========================
# FLUSHER
# =========================
async def flush_closed_minutes(builder):
    while True:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_bucket = (now_ms // 60000) * 60000

        for symbol, cur in list(builder.current.items()):
            if cur["bucket_ms"] < current_bucket:
                builder.finalize(symbol)

        await asyncio.sleep(1)


# =========================
# STREAM
# =========================
async def stream_once(builder):
    session_id = create_session()
    print("SESSION ID:", session_id)

    async with websockets.connect(
        WS_URL,
        ping_interval=None,
        close_timeout=5,
        max_queue=10000,
    ) as ws:

        payload = {
            "symbols": SYMBOLS,
            "sessionid": session_id,
            "filter": FILTER,
        }

        await ws.send(json.dumps(payload))
        print("SUBSCRIBED:", payload)

        while True:
            raw = await ws.recv()
            msg = json.loads(raw)

            if "error" in msg:
                raise RuntimeError(msg)

            if msg.get("type") != "timesale":
                continue

            try:
                symbol = msg["symbol"]
                price = float(msg["last"])
                size = int(msg["size"])
                ts_ms = int(msg["date"])
            except:
                continue

            builder.process_tick(symbol, price, size, ts_ms)


async def stream_forever(builder):
    while True:
        try:
            await stream_once(builder)
        except Exception as e:
            print("DISCONNECTED:", e)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# =========================
# MAIN
# =========================
async def main():
    builder = CandleBuilder1m()

    await asyncio.gather(
        stream_forever(builder),
        flush_closed_minutes(builder),
    )


if __name__ == "__main__":
    asyncio.run(main())