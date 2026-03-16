"""
Deriv Elite Scanner V3 - Volatility Market Scanner
Manual trading signals with real-time WebSocket data
"""

import os
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import numpy as np
import requests
import websockets
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
class Config:
    API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    APP_ID = os.getenv("DERIV_APP_ID", "1089")
    TIMEFRAME = int(os.getenv("TIMEFRAME", "60"))
    CANDLE_COUNT = int(os.getenv("CANDLE_COUNT", "80"))
    MAX_HISTORY = int(os.getenv("MAX_HISTORY", "150"))
    SCAN_INTERVAL = float(os.getenv("SCAN_INTERVAL", "1.0"))
    
    SYMBOLS = ["R_10", "R_25", "R_50", "R_75", "R_100"]
    
    @classmethod
    def validate(cls):
        if not cls.API_TOKEN:
            logger.warning("DERIV_API_TOKEN not set!")
        if not cls.TELEGRAM_TOKEN or not cls.TELEGRAM_CHAT_ID:
            logger.warning("Telegram notifications disabled - tokens not set")

Config.validate()

# Data Models
@dataclass
class MarketData:
    symbol: str
    price: float = 0.0
    signal: str = "SCANNING"
    score: int = 0
    trend: str = "NEUTRAL"
    atr: float = 0.0
    rsi: float = 0.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    last_update: datetime = field(default_factory=datetime.now)
    history: List[Dict] = field(default_factory=list)

class SignalRequest(BaseModel):
    symbol: str
    signal_type: str
    message: Optional[str] = None

# Global State
market_state: Dict[str, MarketData] = {
    s: MarketData(symbol=s) for s in Config.SYMBOLS
}
signal_history: List[Dict] = []
last_signals: Dict[str, str] = {}
connection_status: Dict[str, bool] = {s: False for s in Config.SYMBOLS}

# ==================== TELEGRAM ALERTS ====================

class TelegramNotifier:
    def __init__(self):
        self.enabled = bool(Config.TELEGRAM_TOKEN and Config.TELEGRAM_CHAT_ID)
        self.url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
        self.rate_limit_delay = 1.0
        self.last_send = datetime.min
    
    def send(self, message: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            logger.info(f"Telegram disabled. Message: {message[:50]}...")
            return False
        
        # Rate limiting
        elapsed = (datetime.now() - self.last_send).total_seconds()
        if elapsed < self.rate_limit_delay:
            asyncio.create_task(self._delayed_send(message, parse_mode))
            return True
        
        return self._send_now(message, parse_mode)
    
    def _send_now(self, message: str, parse_mode: str) -> bool:
        try:
            payload = {
                "chat_id": Config.TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            response = requests.post(self.url, json=payload, timeout=10)
            self.last_send = datetime.now()
            
            if response.status_code == 200:
                logger.info(f"Telegram sent: {message[:30]}...")
                return True
            else:
                logger.error(f"Telegram failed: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False
    
    async def _delayed_send(self, message: str, parse_mode: str):
        await asyncio.sleep(self.rate_limit_delay)
        self._send_now(message, parse_mode)

telegram = TelegramNotifier()

# ==================== TECHNICAL INDICATORS ====================

class Indicators:
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate Average True Range"""
        if len(highs) < period + 1:
            return None
        
        trs = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            trs.append(tr)
        
        return float(np.mean(trs[-period:]))
    
    @staticmethod
    def rsi(closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index"""
        if len(closes) < period + 1:
            return None
        
        deltas = np.diff(closes)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = np.mean(gains[-period:]) if len(gains) > 0 else 0
        avg_loss = np.mean(losses[-period:]) if len(losses) > 0 else 0
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    
    @staticmethod
    def ema(data: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        if len(data) < period:
            return None
        
        multiplier = 2.0 / (period + 1)
        ema_val = np.mean(data[:period])  # SMA seed
        
        for price in data[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        
        return float(ema_val)
    
    @staticmethod
    def sma(data: List[float], period: int) -> Optional[float]:
        """Simple Moving Average"""
        if len(data) < period:
            return None
        return float(np.mean(data[-period:]))

# ==================== SIGNAL ANALYSIS ====================

class SignalAnalyzer:
    def __init__(self):
        self.thresholds = {
            "atr_low": 0.25,
            "atr_medium": 0.35,
            "rsi_neutral_low": 45,
            "rsi_neutral_high": 55,
            "rsi_trend_high": 60,
            "rsi_trend_low": 40,
            "range_threshold": 7,
            "compression_range": 5,
            "compression_atr": 0.20,
            "score_threshold": 70
        }
    
    def calculate_score(self, atr_val: float, rsi_val: float, range_val: float) -> int:
        """Calculate signal strength score 0-100"""
        score = 0
        
        # Low volatility bonus (range trading favorable)
        if atr_val < self.thresholds["atr_medium"]:
            score += 30
        if atr_val < self.thresholds["atr_low"]:
            score += 20
        
        # RSI neutrality (sideways market)
        if self.thresholds["rsi_neutral_low"] < rsi_val < self.thresholds["rsi_neutral_high"]:
            score += 20
        
        # Tight range bonus
        if range_val < self.thresholds["range_threshold"]:
            score += 30
        
        return min(score, 100)
    
    def analyze(self, symbol: str, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
        """Perform full market analysis"""
        global last_signals
        
        if len(closes) < 25:
            return {"signal": "INSUFFICIENT_DATA"}
        
        # Calculate indicators
        atr_val = Indicators.atr(highs, lows, closes) or 0
        rsi_val = Indicators.rsi(closes) or 50
        ema_fast = Indicators.ema(closes, 9) or closes[-1]
        ema_slow = Indicators.ema(closes, 21) or closes[-1]
        
        # Recent range
        recent_high = max(highs[-5:])
        recent_low = min(lows[-5:])
        range_val = recent_high - recent_low
        
        price = closes[-1]
        score = self.calculate_score(atr_val, rsi_val, range_val)
        
        # Determine signal
        signal_data = {
            "price": price,
            "atr": round(atr_val, 4),
            "rsi": round(rsi_val, 2),
            "ema_fast": round(ema_fast, 2),
            "ema_slow": round(ema_slow, 2),
            "range": round(range_val, 2),
            "score": score,
            "signal": "NEUTRAL",
            "trend": "NEUTRAL"
        }
        
        # RANGE MARKET (High score)
        if score >= self.thresholds["score_threshold"]:
            signal_data["signal"] = "RANGE"
            signal_data["trend"] = "SIDEWAYS"
            self._notify_signal(symbol, "RANGE", signal_data)
        
        # VOLATILITY COMPRESSION (Breakout setup)
        elif atr_val < self.thresholds["compression_atr"] and range_val < self.thresholds["compression_range"]:
            signal_data["signal"] = "COMPRESSION"
            signal_data["trend"] = "CONSOLIDATION"
            self._notify_signal(symbol, "COMPRESSION", signal_data)
        
        # TRENDING CONDITIONS
        elif ema_fast and ema_slow:
            if ema_fast > ema_slow and rsi_val > self.thresholds["rsi_trend_high"]:
                signal_data["signal"] = "UPTREND"
                signal_data["trend"] = "BULLISH"
                self._notify_signal(symbol, "UPTREND", signal_data)
            
            elif ema_fast < ema_slow and rsi_val < self.thresholds["rsi_trend_low"]:
                signal_data["signal"] = "DOWNTREND"
                signal_data["trend"] = "BEARISH"
                self._notify_signal(symbol, "DOWNTREND", signal_data)
        
        # Update global state
        market_state[symbol].price = round(price, 2)
        market_state[symbol].signal = signal_data["signal"]
        market_state[symbol].score = score
        market_state[symbol].trend = signal_data["trend"]
        market_state[symbol].atr = signal_data["atr"]
        market_state[symbol].rsi = signal_data["rsi"]
        market_state[symbol].ema_fast = signal_data["ema_fast"]
        market_state[symbol].ema_slow = signal_data["ema_slow"]
        market_state[symbol].last_update = datetime.now()
        
        return signal_data
    
    def _notify_signal(self, symbol: str, signal_type: str, data: Dict):
        """Send notification for new signals"""
        global last_signals
        
        if last_signals.get(symbol) == signal_type:
            return
        
        last_signals[symbol] = signal_type
        
        emoji_map = {
            "RANGE": "➡️",
            "COMPRESSION": "⚡",
            "UPTREND": "📈",
            "DOWNTREND": "📉"
        }
        
        emoji = emoji_map.get(signal_type, "🔔")
        
        messages = {
            "RANGE": (
                f"{emoji} <b>{symbol} RANGE MARKET DETECTED</b>\n\n"
                f"Score: <code>{data['score']}/100</code>\n"
                f"Price: <code>{data['price']}</code>\n"
                f"ATR: <code>{data['atr']}</code>\n"
                f"RSI: <code>{data['rsi']}</code>\n"
                f"Expected Range: <code>±3.5</code>\n\n"
                f"<i>Strategy: Consider boundary trading</i>"
            ),
            "COMPRESSION": (
                f"{emoji} <b>{symbol} VOLATILITY COMPRESSION</b>\n\n"
                f"Price: <code>{data['price']}</code>\n"
                f"Range: <code>{data['range']}</code>\n"
                f"ATR: <code>{data['atr']}</code>\n\n"
                f"<i>⚠️ Big breakout likely soon!</i>"
            ),
            "UPTREND": (
                f"{emoji} <b>{symbol} STRONG UPTREND</b>\n\n"
                f"Price: <code>{data['price']}</code>\n"
                f"RSI: <code>{data['rsi']}</code>\n"
                f"EMA9/21: <code>{data['ema_fast']}/{data['ema_slow']}</code>\n\n"
                f"<i>Strategy: Look for pullback entries</i>"
            ),
            "DOWNTREND": (
                f"{emoji} <b>{symbol} STRONG DOWNTREND</b>\n\n"
                f"Price: <code>{data['price']}</code>\n"
                f"RSI: <code>{data['rsi']}</code>\n"
                f"EMA9/21: <code>{data['ema_fast']}/{data['ema_slow']}</code>\n\n"
                f"<i>Strategy: Look for rally entries</i>"
            )
        }
        
        message = messages.get(signal_type, f"{emoji} {symbol}: {signal_type}")
        telegram.send(message)
        
        # Log to history
        signal_history.append({
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "signal": signal_type,
            "data": data
        })
        
        # Keep history manageable
        if len(signal_history) > 1000:
            signal_history.pop(0)

analyzer = SignalAnalyzer()

# ==================== WEBSOCKET CLIENT ====================

class DerivWebSocket:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.ws = None
        self.connected = False
        self.closes: List[float] = []
        self.highs: List[float] = []
        self.lows: List[float] = []
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.running = True
    
    async def connect(self):
        """Establish WebSocket connection with auto-reconnect"""
        url = f"wss://ws.derivws.com/websockets/v3?app_id={Config.APP_ID}"
        
        while self.running:
            try:
                logger.info(f"[{self.symbol}] Connecting...")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    self.connected = True
                    connection_status[self.symbol] = True
                    
                    # Authorize
                    await self._authorize()
                    
                    # Get historical data
                    await self._fetch_history()
                    
                    # Subscribe to ticks
                    await self._subscribe_ticks()
                    
                    # Process messages
                    await self._message_loop()
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[{self.symbol}] Connection closed: {e}")
            except Exception as e:
                logger.error(f"[{self.symbol}] Error: {e}")
            finally:
                self.connected = False
                connection_status[self.symbol] = False
            
            # Exponential backoff
            logger.info(f"[{self.symbol}] Reconnecting in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
    
    async def _authorize(self):
        """Authorize with Deriv API"""
        auth_msg = {"authorize": Config.API_TOKEN}
        await self.ws.send(json.dumps(auth_msg))
        response = json.loads(await self.ws.recv())
        
        if "error" in response:
            raise Exception(f"Auth failed: {response['error']}")
        logger.info(f"[{self.symbol}] Authorized")
    
    async def _fetch_history(self):
        """Fetch historical candle data"""
        history_msg = {
            "ticks_history": self.symbol,
            "style": "candles",
            "granularity": Config.TIMEFRAME,
            "count": Config.CANDLE_COUNT
        }
        await self.ws.send(json.dumps(history_msg))
        response = json.loads(await self.ws.recv())
        
        if "error" in response:
            raise Exception(f"History failed: {response['error']}")
        
        candles = response.get("candles", [])
        self.closes = [c["close"] for c in candles]
        self.highs = [c["high"] for c in candles]
        self.lows = [c["low"] for c in candles]
        
        logger.info(f"[{self.symbol}] Loaded {len(candles)} candles")
    
    async def _subscribe_ticks(self):
        """Subscribe to real-time ticks"""
        tick_msg = {"ticks": self.symbol, "subscribe": 1}
        await self.ws.send(json.dumps(tick_msg))
    
    async def _message_loop(self):
        """Main message processing loop"""
        self.reconnect_delay = 5  # Reset on successful connection
        
        async for message in self.ws:
            try:
                msg = json.loads(message)
                
                if "tick" in msg:
                    await self._handle_tick(msg["tick"])
                elif "candles" in msg:
                    await self._handle_candles(msg["candles"])
                elif "error" in msg:
                    logger.error(f"[{self.symbol}] WS Error: {msg['error']}")
                
            except Exception as e:
                logger.error(f"[{self.symbol}] Message processing error: {e}")
    
    async def _handle_tick(self, tick: Dict):
        """Process incoming tick"""
        price = float(tick["quote"])
        
        # Update arrays (simulate OHLC from ticks for real-time feel)
        self.closes.append(price)
        self.highs.append(price)
        self.lows.append(price)
        
        # Maintain max history
        if len(self.closes) > Config.MAX_HISTORY:
            self.closes.pop(0)
            self.highs.pop(0)
            self.lows.pop(0)
        
        # Analyze on interval to prevent CPU overload
        if len(self.closes) >= 25:
            analyzer.analyze(self.symbol, self.closes, self.highs, self.lows)
        
        await asyncio.sleep(Config.SCAN_INTERVAL)
    
    async def _handle_candles(self, candles: List[Dict]):
        """Handle candle updates"""
        for c in candles:
            self.closes.append(c["close"])
            self.highs.append(c["high"])
            self.lows.append(c["low"])
    
    async def stop(self):
        """Graceful shutdown"""
        self.running = False
        if self.ws:
            await self.ws.close()

# ==================== FASTAPI APPLICATION ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info("Starting Deriv Elite Scanner V3...")
    
    # Start WebSocket clients for each symbol
    clients = [DerivWebSocket(symbol) for symbol in Config.SYMBOLS]
    tasks = [asyncio.create_task(client.connect()) for client in clients]
    
    app.state.clients = clients
    app.state.tasks = tasks
    
    # Send startup notification
    telegram.send(
        "🚀 <b>Deriv Elite Scanner V3 Started</b>\n\n"
        f"Monitoring: <code>{', '.join(Config.SYMBOLS)}</code>\n"
        f"Timeframe: <code>{Config.TIMEFRAME}s</code>"
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    for client in clients:
        await client.stop()
    for task in tasks:
        task.cancel()
    
    telegram.send("⛔ <b>Scanner Stopped</b>")

app = FastAPI(
    title="Deriv Elite Scanner V3",
    description="Real-time volatility market scanner with manual trading signals",
    version="3.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== API ENDPOINTS ====================

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Main dashboard with live market data"""
    
    rows = ""
    for symbol in Config.SYMBOLS:
        data = market_state[symbol]
        
        # Color coding
        colors = {
            "RANGE": "#ff9800",
            "COMPRESSION": "#9c27b0",
            "UPTREND": "#4caf50",
            "DOWNTREND": "#f44336",
            "NEUTRAL": "#757575",
            "SCANNING": "#2196f3",
            "INSUFFICIENT_DATA": "#607d8b"
        }
        color = colors.get(data.signal, "white")
        status = "🟢" if connection_status[symbol] else "🔴"
        
        rows += f"""
        <tr>
            <td>{status} {symbol}</td>
            <td>{data.price:.2f}</td>
            <td style="color:{color};font-weight:bold">{data.signal}</td>
            <td>
                <div style="background:#333;border-radius:4px;height:20px;width:100px">
                    <div style="background:{color};width:{data.score}%;height:100%;border-radius:4px">
                    </div>
                </div>
                <small>{data.score}/100</small>
            </td>
            <td>{data.trend}</td>
            <td>{data.rsi:.1f}</td>
            <td>{data.atr:.4f}</td>
            <td>{data.last_update.strftime('%H:%M:%S')}</td>
        </tr>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Deriv Elite Scanner V3</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="3">
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                background: linear-gradient(135deg, #0f0f0f 0%, #1a1a2e 100%); 
                color: #fff; 
                min-height: 100vh;
                padding: 20px;
            }}
            .container {{ max-width: 1400px; margin: 0 auto; }}
            h1 {{ 
                text-align: center; 
                color: #00d4ff; 
                margin-bottom: 10px;
                text-shadow: 0 0 20px rgba(0,212,255,0.5);
            }}
            .subtitle {{ 
                text-align: center; 
                color: #888; 
                margin-bottom: 30px;
                font-size: 0.9em;
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 30px;
            }}
            .stat-card {{
                background: rgba(255,255,255,0.05);
                padding: 20px;
                border-radius: 10px;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .stat-label {{ color: #888; font-size: 0.8em; text-transform: uppercase; }}
            .stat-value {{ font-size: 1.5em; font-weight: bold; color: #00d4ff; }}
            table {{ 
                width: 100%; 
                border-collapse: collapse; 
                background: rgba(255,255,255,0.03);
                border-radius: 10px;
                overflow: hidden;
            }}
            th, td {{ 
                padding: 15px; 
                text-align: left; 
                border-bottom: 1px solid rgba(255,255,255,0.1);
            }}
            th {{ 
                background: rgba(0,212,255,0.1); 
                color: #00d4ff;
                font-weight: 600;
                text-transform: uppercase;
                font-size: 0.8em;
                letter-spacing: 1px;
            }}
            tr:hover {{ background: rgba(255,255,255,0.05); }}
            .signal-badge {{
                padding: 5px 10px;
                border-radius: 20px;
                font-size: 0.8em;
                font-weight: bold;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                color: #666;
                font-size: 0.8em;
            }}
            @media (max-width: 768px) {{
                table {{ font-size: 0.8em; }}
                th, td {{ padding: 8px; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>⚡ Deriv Elite Scanner V3</h1>
            <p class="subtitle">Real-time Volatility Market Analysis | Manual Trading Signals</p>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-label">Active Symbols</div>
                    <div class="stat-value">{sum(connection_status.values())}/{len(Config.SYMBOLS)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Signals</div>
                    <div class="stat-value">{len(signal_history)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Timeframe</div>
                    <div class="stat-value">{Config.TIMEFRAME}s</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Last Update</div>
                    <div class="stat-value">{datetime.now().strftime('%H:%M:%S')}</div>
                </div>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Price</th>
                        <th>Signal</th>
                        <th>Strength</th>
                        <th>Trend</th>
                        <th>RSI</th>
                        <th>ATR</th>
                        <th>Updated</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            
            <div class="footer">
                <p>Auto-refresh every 3 seconds | WebSocket Real-time Data</p>
                <p>Manual Trading Only - No Automated Execution</p>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

@app.get("/api/status")
async def api_status():
    """JSON API for market status"""
    return {
        "timestamp": datetime.now().isoformat(),
        "connections": connection_status,
        "markets": {
            symbol: {
                "price": data.price,
                "signal": data.signal,
                "score": data.score,
                "trend": data.trend,
                "rsi": data.rsi,
                "atr": data.atr,
                "last_update": data.last_update.isoformat()
            }
            for symbol, data in market_state.items()
        },
        "recent_signals": signal_history[-10:]
    }

@app.get("/api/signals")
async def get_signals(limit: int = 50):
    """Get signal history"""
    return {
        "signals": signal_history[-limit:],
        "count": len(signal_history)
    }

@app.post("/api/test-signal")
async def test_signal(request: SignalRequest):
    """Manually trigger a test signal"""
    message = (
        f"🧪 <b>TEST SIGNAL</b>\n\n"
        f"Symbol: <code>{request.symbol}</code>\n"
        f"Type: <code>{request.signal_type}</code>\n"
        f"Message: {request.message or 'N/A'}\n"
        f"Time: <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
    )
    
    success = telegram.send(message)
    return {"sent": success, "message": message}

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    healthy = sum(connection_status.values()) >= len(Config.SYMBOLS) // 2
    return JSONResponse(
        content={
            "status": "healthy" if healthy else "degraded",
            "connections": connection_status,
            "timestamp": datetime.now().isoformat()
        },
        status_code=200 if healthy else 503
    )

# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
