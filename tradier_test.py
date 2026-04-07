import asyncio
import json
import requests
import websockets

TOKEN = "YOUR_TRADIER_TOKEN"
SYMBOLS = ["SPY"]
FILTER = ["timesale"]

def create_session():
    r = requests.post(
        "https://api.tradier.com/v1/markets/events/session",
        headers={
            "Authorization": f"Bearer QoiXId2fsKadZ7rOdLtUhGxMslYu",
            "Accept": "application/json",
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["stream"]["sessionid"]

async def main():
    session_id = create_session()
    print("SESSION ID:", session_id)

    async with websockets.connect("wss://ws.tradier.com/v1/markets/events") as ws:
        payload = {
            "symbols": SYMBOLS,
            "sessionid": session_id,
            "filter": FILTER,
        }
        await ws.send(json.dumps(payload))
        print("SUBSCRIBED:", payload)

        while True:
            print(await ws.recv())

asyncio.run(main())