import asyncio
import json
import websockets
from typing import List, Dict
from config import Config
from analyzer import SignalAnalyzer
from models import MarketData

class DerivWebSocket:
    def __init__(self, symbol: str, market_state: Dict[str, MarketData], last_signals: Dict[str, str], analyzer: SignalAnalyzer):
        self.symbol = symbol
        self.ws = None
        self.market_state = market_state
        self.last_signals = last_signals
        self.analyzer = analyzer
        self.closes: List[float] = []
        self.highs: List[float] = []
        self.lows: List[float] = []
        self.running = True

    async def connect(self):
        url = f"wss://ws.derivws.com/websockets/v3?app_id={Config.APP_ID}"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    await self._authorize()
                    await self._fetch_history()
                    await self._subscribe_ticks()
                    await self._message_loop()
            except Exception as e:
                print(f"[{self.symbol}] WS error: {e}")
                await asyncio.sleep(5)

    async def _authorize(self):
        await self.ws.send(json.dumps({"authorize": Config.API_TOKEN}))
        resp = await self.ws.recv()
        data = json.loads(resp)
        if "error" in data:
            raise Exception(data["error"])

    async def _fetch_history(self):
        msg = {"ticks_history": self.symbol, "style": "candles", "granularity": Config.TIMEFRAME, "count": Config.CANDLE_COUNT}
        await self.ws.send(json.dumps(msg))
        resp = json.loads(await self.ws.recv())
        candles = resp.get("candles", [])
        self.closes = [c["close"] for c in candles]
        self.highs = [c["high"] for c in candles]
        self.lows = [c["low"] for c in candles]

    async def _subscribe_ticks(self):
        await self.ws.send(json.dumps({"ticks": self.symbol, "subscribe": 1}))

    async def _message_loop(self):
        async for msg in self.ws:
            data = json.loads(msg)
            if "tick" in data:
                tick = data["tick"]
                price = float(tick["quote"])
                self.closes.append(price)
                self.highs.append(price)
                self.lows.append(price)
                if len(self.closes) > Config.MAX_HISTORY:
                    self.closes.pop(0)
                    self.highs.pop(0)
                    self.lows.pop(0)
                if len(self.closes) >= 25:
                    self.analyzer.analyze(self.symbol, self.closes, self.highs, self.lows, self.market_state, self.last_signals)
                await asyncio.sleep(Config.SCAN_INTERVAL)
