import asyncio
import json
import websockets

from config import Config
from indicators import TechnicalIndicators
from broadcaster import broadcast_update


class DerivAPI:

    def __init__(self, market_state):

        self.market_state = market_state
        self.ws = None

    async def connect(self):

        uri = f"{Config.DERIV_WS_URL}?app_id={Config.DERIV_APP_ID}"

        while True:

            try:

                print("Connecting to Deriv...")

                async with websockets.connect(uri) as ws:

                    self.ws = ws

                    await self.subscribe()

                    async for msg in ws:

                        data = json.loads(msg)

                        if "tick" in data:
                            await self.process_tick(data["tick"])

            except Exception as e:

                print("Connection error:", e)

                await asyncio.sleep(5)

    async def subscribe(self):

        for symbol in Config.SYMBOLS:

            msg = {
                "ticks": symbol,
                "subscribe": 1
            }

            await self.ws.send(json.dumps(msg))

            print("Subscribed:", symbol)

    async def process_tick(self, tick):

        symbol = tick["symbol"]
        price = float(tick["quote"])

        state = self.market_state[symbol]

        state.price = price
        state.price_history.append(price)

        if len(state.price_history) > Config.HISTORY_LENGTH:
            state.price_history.pop(0)

        prices = state.price_history

        if len(prices) >= 21:

            state.ema_fast = TechnicalIndicators.ema(prices, 9)
            state.ema_slow = TechnicalIndicators.ema(prices, 21)
            state.rsi = TechnicalIndicators.rsi(prices)

            signal, trend, score = TechnicalIndicators.signal(
                prices,
                state.ema_fast,
                state.ema_slow,
                state.rsi
            )

            state.signal = signal
            state.trend = trend
            state.score = score

        await broadcast_update(self.market_state)
