"""
Deriv Elite Scanner V3 - Volatility Market Scanner
Manual trading signals with real-time WebSocket data
"""

import os
import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Literal
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import numpy as np
import requests
import websockets
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Path
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, ValidationError, conint, confloat

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
    VALID_SIGNAL_TYPES = ["RANGE", "COMPRESSION", "UPTREND", "DOWNTREND", "NEUTRAL", "BUY", "SELL"]
    VALID_TIMEFRAMES = [60, 300, 900, 1800, 3600, 14400, 86400]
    
    @classmethod
    def validate(cls):
        errors = []
        
        if not cls.API_TOKEN:
            errors.append("DERIV_API_TOKEN not set!")
        elif len(cls.API_TOKEN) < 10:
            errors.append("DERIV_API_TOKEN appears invalid (too short)")
            
        if cls.TIMEFRAME not in cls.VALID_TIMEFRAMES:
            logger.warning(f"TIMEFRAME {cls.TIMEFRAME} not standard. Valid: {cls.VALID_TIMEFRAMES}")
            
        if cls.CANDLE_COUNT < 20 or cls.CANDLE_COUNT > 1000:
            errors.append(f"CANDLE_COUNT must be 20-1000, got {cls.CANDLE_COUNT}")
            
        if cls.SCAN_INTERVAL < 0.1 or cls.SCAN_INTERVAL > 60:
            errors.append(f"SCAN_INTERVAL must be 0.1-60, got {cls.SCAN_INTERVAL}")
        
        if errors:
            for error in errors:
                logger.error(f"Config Error: {error}")
            raise ValueError(f"Configuration validation failed: {', '.join(errors)}")
        
        if not cls.TELEGRAM_TOKEN or not cls.TELEGRAM_CHAT_ID:
            logger.warning("Telegram notifications disabled - tokens not set")

Config.validate()

# Data Models with Validation
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
    """Validated signal request model"""
    symbol: str = Field(
        ...,
        min_length=3,
        max_length=10,
        regex=r"^R_\d+$",
        description="Symbol must be R_10, R_25, R_50, R_75, or R_100"
    )
    signal_type: str = Field(
        ...,
        min_length=3,
        max_length=20,
        description="Type of trading signal"
    )
    message: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional message (max 500 chars)"
    )
    confidence: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Confidence score 0-100"
    )
    
    @validator('symbol')
    def validate_symbol(cls, v):
        allowed = Config.SYMBOLS
        if v not in allowed:
            raise ValueError(f"Symbol must be one of: {', '.join(allowed)}")
        return v
    
    @validator('signal_type')
    def validate_signal_type(cls, v):
        v = v.upper()
        allowed = Config.VALID_SIGNAL_TYPES
        if v not in allowed:
            raise ValueError(f"Signal type must be one of: {', '.join(allowed)}")
        return v
    
    @validator('message')
    def sanitize_message(cls, v):
        if v is None:
            return v
        # Remove potentially dangerous characters
        v = re.sub(r'[<>\"\'%;()&+\\]', '', v)
        return v.strip()

class PriceAlertRequest(BaseModel):
    """Price alert configuration"""
    symbol: str = Field(..., description="Symbol to monitor")
    price_above: Optional[float] = Field(None, gt=0, description="Alert when price goes above")
    price_below: Optional[float] = Field(None, gt=0, description="Alert when price goes below")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        if v not in Config.SYMBOLS:
            raise ValueError(f"Invalid symbol. Choose from: {', '.join(Config.SYMBOLS)}")
        return v
    
    @root_validator
    def check_at_least_one_threshold(cls, values):
        above = values.get('price_above')
        below = values.get('price_below')
        if above is None and below is None:
            raise ValueError('Must specify either price_above or price_below')
        if above is not None and below is not None and above <= below:
            raise ValueError('price_above must be greater than price_below')
        return values

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
        self._validate_tokens()
    
    def _validate_tokens(self):
        """Validate telegram configuration"""
        if self.enabled:
            if not re.match(r'^\d+:[A-Za-z0-9_-]+$', Config.TELEGRAM_TOKEN):
                logger.error("TELEGRAM_BOT_TOKEN format appears invalid")
                self.enabled = False
            if not re.match(r'^-?\d+$', Config.TELEGRAM_CHAT_ID):
                logger.error("TELEGRAM_CHAT_ID format appears invalid")
                self.enabled = False
    
    def send(self, message: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            logger.info(f"Telegram disabled. Message: {message[:50]}...")
            return False
        
        # Validate message length
        if len(message) > 4096:
            logger.error("Message too long for Telegram (max 4096 chars)")
            message = message[:4093] + "..."
        
        # Rate limiting
        elapsed = (datetime.now() - self.last_send).total_seconds()
        if elapsed < self.rate_limit_delay:
            asyncio.create_task(self._delayed_send(message, parse_mode))
            return True
        
        return self._send_now(message, parse_mode)
    
    def _send_now(self, message: str, parse_mode: str) -> bool:
        try:
            # Validate parse_mode
            if parse_mode not in ["HTML", "Markdown", "MarkdownV2"]:
                parse_mode = "HTML"
            
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
    def validate_price_data(prices: List[float], min_length: int = 1) -> bool:
        """Validate price data integrity"""
        if not prices or len(prices) < min_length:
            return False
        if any(not isinstance(p, (int, float)) or np.isnan(p) or np.isinf(p) for p in prices):
            return False
        if any(p <= 0 for p in prices):
            return False
        return True
    
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate Average True Range with validation"""
        if not (Indicators.validate_price_data(highs, period + 1) and 
                Indicators.validate_price_data(lows, period + 1) and 
                Indicators.validate_price_data(closes, period + 1)):
            return None
        
        if len(highs) != len(lows) or len(highs) != len(closes):
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
        """Calculate Relative Strength Index with validation"""
        if not Indicators.validate_price_data(closes, period + 1):
            return None
        
        deltas = np.diff(closes)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = np.mean(gains[-period:]) if len(gains) > 0 else 0
        avg_loss = np.mean(losses[-period:]) if len(losses) > 0 else 0
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi_val = 100.0 - (100.0 / (1.0 + rs))
        
        # Clamp to valid range
        return max(0.0, min(100.0, rsi_val))
    
    @staticmethod
    def ema(data: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average with validation"""
        if not Indicators.validate_price_data(data, period):
            return None
        
        multiplier = 2.0 / (period + 1)
        ema_val = np.mean(data[:period])
        
        for price in data[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        
        return float(ema_val)

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
        """Calculate signal strength score 0-100 with validation"""
        # Validate inputs
        if not (0 <= rsi_val <= 100):
            rsi_val = 50
        if atr_val < 0:
            atr_val = 0
        if range_val < 0:
            range_val = 0
        
        score = 0
        
        if atr_val < self.thresholds["atr_medium"]:
            score += 30
        if atr_val < self.thresholds["atr_low"]:
            score += 20
        
        if self.thresholds["rsi_neutral_low"] < rsi_val < self.thresholds["rsi_neutral_high"]:
            score += 20
        
        if range_val < self.thresholds["range_threshold"]:
            score += 30
        
        return min(max(score, 0), 100)  # Ensure 0-100 range
    
    def analyze(self, symbol: str, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
        """Perform full market analysis with validation"""
        global last_signals
        
        # Input validation
        if symbol not in Config.SYMBOLS:
            logger.error(f"Invalid symbol in analyze: {symbol}")
            return {"signal": "ERROR", "error": "Invalid symbol"}
        
        if len(closes) < 25:
            return {"signal": "INSUFFICIENT_DATA"}
        
        # Validate data integrity
        if not (Indicators.validate_price_data(closes, 25) and 
                Indicators.validate_price_data(highs, 25) and 
                Indicators.validate_price_data(lows, 25)):
            logger.warning(f"[{symbol}] Invalid price data detected")
            return {"signal": "DATA_ERROR"}
        
        # Calculate indicators
        atr_val = Indicators.atr(highs, lows, closes) or 0
        rsi_val = Indicators.rsi(closes) or 50
        ema_fast = Indicators.ema(closes, 9) or closes[-1]
        ema_slow = Indicators.ema(closes, 21) or closes[-1]
        
        # Validate calculated values
        if any(np.isnan([atr_val, rsi_val, ema_fast, ema_slow])):
            return {"signal": "CALCULATION_ERROR"}
        
        recent_high = max(highs[-5:])
        recent_low = min(lows[-5:])
        range_val = recent_high - recent_low
        
        price = closes[-1]
        score = self.calculate_score(atr_val, rsi_val, range_val)
        
        signal_data = {
            "price": round(price, 2),
            "atr": round(atr_val, 4),
            "rsi": round(rsi_val, 2),
            "ema_fast": round(ema_fast, 2),
            "ema_slow": round(ema_slow, 2),
            "range": round(range_val, 2),
            "score": score,
            "signal": "NEUTRAL",
            "trend": "NEUTRAL"
        }
        
        # Signal logic with boundary checks
        if score >= self.thresholds["score_threshold"]:
            signal_data["signal"] = "RANGE"
            signal_data["trend"] = "SIDEWAYS"
            self._notify_signal(symbol, "RANGE", signal_data)
        
        elif (atr_val < self.thresholds["compression_atr"] and 
              range_val < self.thresholds["compression_range"] and 
              atr_val > 0):  # Ensure positive ATR
            signal_data["signal"] = "COMPRESSION"
            signal_data["trend"] = "CONSOLIDATION"
            self._notify_signal(symbol, "COMPRESSION", signal_data)
        
        elif ema_fast and ema_slow:
            if ema_fast > ema_slow and rsi_val > self.thresholds["rsi_trend_high"]:
                signal_data["signal"] = "UPTREND"
                signal_data["trend"] = "BULLISH"
                self._notify_signal(symbol, "UPTREND", signal_data)
            
            elif ema_fast < ema_slow and rsi_val < self.thresholds["rsi_trend_low"]:
                signal_data["signal"] = "DOWNTREND"
                signal_data["trend"] = "BEARISH"
                self._notify_signal(symbol, "DOWNTREND", signal_data)
        
        # Update global state safely
        self._update_market_state(symbol, signal_data, price)
        
        return signal_data
    
    def _update_market_state(self, symbol: str, signal_data: Dict, price: float):
        """Safely update market state"""
        try:
            state = market_state[symbol]
            state.price = round(price, 2)
            state.signal = signal_data["signal"]
            state.score = signal_data["score"]
            state.trend = signal_data["trend"]
            state.atr = signal_data["atr"]
            state.rsi = signal_data["rsi"]
            state.ema_fast = signal_data["ema_fast"]
            state.ema_slow = signal_data["ema_slow"]
            state.last_update = datetime.now()
        except Exception as e:
            logger.error(f"Error updating market state for {symbol}: {e}")
    
    def _notify_signal(self, symbol: str, signal_type: str, data: Dict):
        """Send notification for new signals with validation"""
        global last_signals
        
        if not isinstance(signal_type, str) or signal_type not in Config.VALID_SIGNAL_TYPES:
            logger.error(f"Invalid signal type: {signal_type}")
            return
        
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
        
        # Validate data before formatting
        safe_data = {
            'score': data.get('score', 0),
            'price': data.get('price', 0),
            'atr': data.get('atr', 0),
            'rsi': data.get('rsi', 0),
            'ema_fast': data.get('ema_fast', 0),
            'ema_slow': data.get('ema_slow', 0),
            'range': data.get('range', 0)
        }
        
        messages = {
            "RANGE": (
                f"{emoji} <b>{symbol} RANGE MARKET DETECTED</b>\n\n"
                f"Score: <code>{safe_data['score']}/100</code>\n"
                f"Price: <code>{safe_data['price']}</code>\n"
                f"ATR: <code>{safe_data['atr']}</code>\n"
                f"RSI: <code>{safe_data['rsi']}</code>\n"
                f"Expected Range: <code>±3.5</code>\n\n"
                f"<i>Strategy: Consider boundary trading</i>"
            ),
            "COMPRESSION": (
                f"{emoji} <b>{symbol} VOLATILITY COMPRESSION</b>\n\n"
                f"Price: <code>{safe_data['price']}</code>\n"
                f"Range: <code>{safe_data['range']}</code>\n"
                f"ATR: <code>{safe_data['atr']}</code>\n\n"
                f"<i>⚠️ Big breakout likely soon!</i>"
            ),
            "UPTREND": (
                f"{emoji} <b>{symbol} STRONG UPTREND</b>\n\n"
                f"Price: <code>{safe_data['price']}</code>\n"
                f"RSI: <code>{safe_data['rsi']}</code>\n"
                f"EMA9/21: <code>{safe_data['ema_fast']}/{safe_data['ema_slow']}</code>\n\n"
                f"<i>Strategy: Look for pullback entries</i>"
            ),
            "DOWNTREND": (
                f"{emoji} <b>{symbol} STRONG DOWNTREND</b>\n\n"
                f"Price: <code>{safe_data['price']}</code>\n"
                f"RSI: <code>{safe_data['rsi']}</code>\n"
                f"EMA9/21: <code>{safe_data['ema_fast']}/{safe_data['ema_slow']}</code>\n\n"
                f"<i>Strategy: Look for rally entries</i>"
            )
        }
        
        message = messages.get(signal_type, f"{emoji} {symbol}: {signal_type}")
        telegram.send(message)
        
        # Log to history with size limit
        signal_history.append({
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "signal": signal_type,
            "data": safe_data
        })
        
        # Keep history manageable
        if len(signal_history) > 1000:
            signal_history.pop(0)

analyzer = SignalAnalyzer()

# ==================== WEBSOCKET CLIENT ====================

class DerivWebSocket:
    def __init__(self, symbol: str):
        if symbol not in Config.SYMBOLS:
            raise ValueError(f"Invalid symbol: {symbol}")
        
        self.symbol = symbol
        self.ws = None
        self.connected = False
        self.closes: List[float] = []
        self.highs: List[float] = []
        self.lows: List[float] = []
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.running = True
        self._price_validator = re.compile(r'^\d+\.?\d*$')
    
    def _validate_tick(self, tick: Dict) -> Optional[float]:
        """Validate tick data structure"""
        if not isinstance(tick, dict):
            return None
        if "quote" not in tick:
            return None
        
        quote = tick["quote"]
        if isinstance(quote, str):
            if not self._price_validator.match(quote):
                return None
            try:
                price = float(quote)
            except ValueError:
                return None
        elif isinstance(quote, (int, float)):
            price = float(quote)
        else:
            return None
        
        if price <= 0 or np.isnan(price) or np.isinf(price):
            return None
        
        return price
    
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
                    
                    await self._authorize()
                    await self._fetch_history()
                    await self._subscribe_ticks()
                    await self._message_loop()
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[{self.symbol}] Connection closed: {e}")
            except Exception as e:
                logger.error(f"[{self.symbol}] Error: {e}")
            finally:
                self.connected = False
                connection_status[self.symbol] = False
            
            logger.info(f"[{self.symbol}] Reconnecting in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
    
    async def _authorize(self):
        """Authorize with Deriv API with validation"""
        if not Config.API_TOKEN:
            raise Exception("API_TOKEN not configured")
        
        auth_msg = {"authorize": Config.API_TOKEN}
        await self.ws.send(json.dumps(auth_msg))
        response = json.loads(await self.ws.recv())
        
        if "error" in response:
            raise Exception(f"Auth failed: {response['error']}")
        if "authorize" not in response:
            raise Exception("Invalid auth response")
        
        logger.info(f"[{self.symbol}] Authorized")
    
    async def _fetch_history(self):
        """Fetch historical candle data with validation"""
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
        if not isinstance(candles, list) or len(candles) == 0:
            raise Exception("No candles received")
        
        # Validate candle structure
        validated_candles = []
        for c in candles:
            if all(k in c for k in ["close", "high", "low", "open"]):
                try:
                    validated_candles.append({
                        "close": float(c["close"]),
                        "high": float(c["high"]),
                        "low": float(c["low"]),
                        "open": float(c["open"])
                    })
                except (ValueError, TypeError):
                    continue
        
        if len(validated_candles) < 25:
            raise Exception(f"Insufficient valid candles: {len(validated_candles)}")
        
        self.closes = [c["close"] for c in validated_candles]
        self.highs = [c["high"] for c in validated_candles]
        self.lows = [c["low"] for c in validated_candles]
        
        logger.info(f"[{self.symbol}] Loaded {len(validated_candles)} candles")
    
    async def _subscribe_ticks(self):
        """Subscribe to real-time ticks"""
        tick_msg = {"ticks": self.symbol, "subscribe": 1}
        await self.ws.send(json.dumps(tick_msg))
    
    async def _message_loop(self):
        """Main message processing loop"""
        self.reconnect_delay = 5
        
        async for message in self.ws:
            try:
                if not isinstance(message, str):
                    continue
                
                msg = json.loads(message)
                
                if not isinstance(msg, dict):
                    continue
                
                if "tick" in msg and isinstance(msg["tick"], dict):
                    await self._handle_tick(msg["tick"])
                elif "candles" in msg and isinstance(msg["candles"], list):
                    await self._handle_candles(msg["candles"])
                elif "error" in msg:
                    logger.error(f"[{self.symbol}] WS Error: {msg['error']}")
                
            except json.JSONDecodeError:
                logger.error(f"[{self.symbol}] Invalid JSON received")
            except Exception as e:
                logger.error(f"[{self.symbol}] Message processing error: {e}")
    
    async def _handle_tick(self, tick: Dict):
        """Process incoming tick with validation"""
        price = self._validate_tick(tick)
        if price is None:
            logger.warning(f"[{self.symbol}] Invalid tick data: {tick}")
            return
        
        # Update arrays
        self.closes.append(price)
        self.highs.append(price)
        self.lows.append(price)
        
        # Maintain max history
        if len(self.closes) > Config.MAX_HISTORY:
            self.closes.pop(0)
            self.highs.pop(0)
            self.lows.pop(0)
        
        # Analyze on interval
        if len(self.closes) >= 25:
            analyzer.analyze(self.symbol, self.closes, self.highs, self.lows)
        
        await asyncio.sleep(Config.SCAN_INTERVAL)
    
    async def _handle_candles(self, candles: List[Dict]):
        """Handle candle updates with validation"""
        for c in candles:
            if not all(k in c for k in ["close", "high", "low"]):
                continue
            try:
                self.closes.append(float(c["close"]))
                self.highs.append(float(c["high"]))
                self.lows.append(float(c["low"]))
            except (ValueError, TypeError):
                continue
    
    async def stop(self):
        """Graceful shutdown"""
        self.running = False
        if self.ws:
            await self.ws.close()

# ==================== FASTAPI APPLICATION ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Starting Deriv Elite Scanner V3...")
    
    clients = [DerivWebSocket(symbol) for symbol in Config.SYMBOLS]
    tasks = [asyncio.create_task(client.connect()) for client in clients]
    
    app.state.clients = clients
    app.state.tasks = tasks
    
    telegram.send(
        "🚀 <b>Deriv Elite Scanner V3 Started</b>\n\n"
        f"Monitoring: <code>{', '.join(Config.SYMBOLS)}</code>\n"
        f"Timeframe: <code>{Config.TIMEFRAME}s</code>"
    )
    
    yield
    
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== API ENDPOINTS WITH VALIDATION ====================

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Main dashboard with live market data"""
    rows = ""
    for symbol in Config.SYMBOLS:
        data = market_state[symbol]
        
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
            <td style="color: {color}"><strong>{data.signal}</strong></td>
            <td>{data.score}/100</td>
            <td>{data.price}</td>
            <td>{data.trend}</td>
            <td>{data.atr}</td>
            <td>{data.rsi}</td>
            <td>{data.last_update.strftime('%H:%M:%S') if data.last_update else 'N/A'}</td>
        </tr>
        """
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Deriv Elite Scanner V3</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body {{ font-family: Arial, sans-serif; background: #1a1a1a; color: #fff; padding: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #333; }}
            th {{ background: #2c2c2c; }}
            tr:hover {{ background: #2a2a2a; }}
            .header {{ color: #4caf50; }}
        </style>
    </head>
    <body>
        <h1 class="header">📊 Deriv Elite Scanner V3</h1>
        <p>Real-time Volatility Market Analysis</p>
        <table>
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Signal</th>
                    <th>Score</th>
                    <th>Price</th>
                    <th>Trend</th>
                    <th>ATR</th>
                    <th>RSI</th>
                    <th>Last Update</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/api/status")
async def get_status():
    """Get system status"""
    return {
        "status": "running",
        "symbols": Config.SYMBOLS,
        "connections": connection_status,
        "uptime": "active"
    }

@app.get("/api/signals/{symbol}")
async def get_signals(
    symbol: str = Path(..., description="Symbol to query", regex=r"^R_\d+$"),
    limit: int = Query(50, ge=1, le=200, description="Number of signals to return")
):
    """Get signal history for a symbol with validation"""
    if symbol not in Config.SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Invalid symbol. Must be one of: {', '.join(Config.SYMBOLS)}")
    
    filtered_history = [
        s for s in signal_history 
        if s["symbol"] == symbol
    ][-limit:]
    
    return {
        "symbol": symbol,
        "current": market_state[symbol].__dict__,
        "history": filtered_history
    }

@app.post("/api/signals/manual", response_model=Dict[str, Any])
async def create_manual_signal(request: SignalRequest):
    """Create a manual trading signal with full validation"""
    try:
        # Additional business logic validation
        if request.confidence and request.confidence < 50:
            raise HTTPException(
                status_code=400, 
                detail="Confidence must be at least 50 for manual signals"
            )
        
        signal_record = {
            "timestamp": datetime.now().isoformat(),
            "symbol": request.symbol,
            "signal_type": request.signal_type,
            "message": request.message,
            "confidence": request.confidence,
            "source": "manual"
        }
        
        signal_history.append(signal_record)
        
        # Send notification
        emoji_map = {
            "RANGE": "➡️", "COMPRESSION": "⚡", "UPTREND": "📈", 
            "DOWNTREND": "📉", "BUY": "🟢", "SELL": "🔴"
        }
        emoji = emoji_map.get(request.signal_type, "🔔")
        
        msg = (
            f"{emoji} <b>MANUAL SIGNAL: {request.symbol}</b>\n\n"
            f"Type: <code>{request.signal_type}</code>\n"
            f"Confidence: <code>{request.confidence or 'N/A'}%</code>\n"
        )
        if request.message:
            msg += f"Note: <i>{request.message}</i>\n"
        
        telegram.send(msg)
        
        return {
            "status": "success",
            "signal": signal_record
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating manual signal: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/alerts/price")
async def set_price_alert(request: PriceAlertRequest):
    """Set a price alert with validation"""
    # Implementation would store alert in database/memory
    return {
        "status": "success",
        "alert": {
            "symbol": request.symbol,
            "price_above": request.price_above,
            "price_below": request.price_below,
            "created_at": datetime.now().isoformat()
        }
    }

@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    """Handle Pydantic validation errors"""
    return JSONResponse(
        status_code=422,
        content={"detail": "Validation error", "errors": exc.errors()}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
