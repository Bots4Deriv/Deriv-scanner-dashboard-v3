import asyncio
import json
import websockets
from config import SYMBOLS, DERIV_WS


async def start_deriv_stream():

    print("Connecting to Deriv...")

    async with websockets.connect(DERIV_WS) as ws:

        for symbol in SYMBOLS:

            await ws.send(json.dumps({
                "ticks": symbol,
                "subscribe": 1
            }))

            print("Subscribed:", symbol)

        while True:

            data = await ws.recv()
            tick = json.loads(data)

            if "tick" in tick:

                symbol = tick["tick"]["symbol"]
                price = tick["tick"]["quote"]

                print(symbol, price)
