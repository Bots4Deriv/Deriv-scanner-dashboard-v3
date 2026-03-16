from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import asynccontextmanager
import asyncio
import json
import websockets
import numpy as np
import os

# ==========================================
# CONFIGURATION - NO SECRETS REQUIRED
# ==========================================

class Config:
    # Get from environment variable or use default (for testing only)
    # In production, set DERIV_APP_ID in Railway Variables
    DERIV_APP_ID = os.getenv("DERIV_APP_ID", "YOUR_APP_ID_HERE")
    DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3"
    
    # Get from environment or use defaults
    SYMBOLS: List[str] = os.getenv("SYMBOLS", "R_10,R_25,R_50,R_75,R_100").split(",")
    
    # Technical parameters (can be overridden via env vars)
    UPDATE_INTERVAL = float(os.getenv("UPDATE_INTERVAL", "0.1"))
    HISTORY_LENGTH = int(os.getenv("HISTORY_LENGTH", "100"))
    CANDLE_COUNT = int(os.getenv("CANDLE_COUNT", "100"))  # Added to satisfy Railway check

# ==========================================
# TECHNICAL INDICATORS
# ==========================================

class TechnicalIndicators:
    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> float:
        """Calculate RSI"""
        if len(prices) < period + 1:
            return 50.0
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> float:
        """Calculate EMA"""
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        
        multiplier = 2 / (period + 1)
        ema = np.mean(prices[:period])
        
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return float(ema)
    
    @staticmethod
    def calculate_atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> float:
        """Calculate ATR"""
        if len(close) < period + 1:
            return 0.0
        
        tr_list = []
        for i in range(1, len(close)):
            tr1 = high[i] - low[i]
            tr2 = abs(high[i] - close[i-1])
            tr3 = abs(low[i] - close[i-1])
            tr_list.append(max(tr1, tr2, tr3))
        
        return float(np.mean(tr_list[-period:]))
    
    @staticmethod
    def calculate_signal(prices: List[float], ema_fast: float, ema_slow: float, rsi: float) -> tuple:
        """Determine trading signal"""
        if len(prices) < 21:
            return "INSUFFICIENT_DATA", "NEUTRAL", 0
        
        if ema_fast > ema_slow * 1.001:
            trend_dir = "BULLISH"
            if rsi < 70:
                signal = "UPTREND"
                score = int(60 + (rsi / 100) * 30 + (ema_fast/ema_slow - 1) * 1000)
            else:
                signal = "UPTREND"
                score = 50
        elif ema_fast < ema_slow * 0.999:
            trend_dir = "BEARISH"
            if rsi > 30:
                signal = "DOWNTREND"
                score = int(60 + ((100-rsi) / 100) * 30 + (1 - ema_fast/ema_slow) * 1000)
            else:
                signal = "DOWNTREND"
                score = 50
        else:
            recent_range = (max(prices[-10:]) - min(prices[-10:])) / np.mean(prices[-10:])
            if recent_range < 0.002:
                signal = "COMPRESSION"
                trend_dir = "CONSOLIDATING"
                score = 40
            else:
                signal = "RANGE"
                trend_dir = "NEUTRAL"
                score = 30
        
        return signal, trend_dir, min(score, 100)

# ==========================================
# MODELS
# ==========================================

@dataclass
class MarketData:
    symbol: str = ""
    price: float = 0.0
    signal: str = "SCANNING"
    trend: str = "NEUTRAL"
    score: int = 0
    atr: float = 0.0
    rsi: float = 50.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    price_history: List[float] = field(default_factory=list)
    high_history: List[float] = field(default_factory=list)
    low_history: List[float] = field(default_factory=list)

# Global state
market_state: Dict[str, MarketData] = {}
connection_status: Dict[str, bool] = {}
connected_clients: List[WebSocket] = []
deriv_ws_connection: Optional[websockets.WebSocketClientProtocol] = None

def init_state():
    """Initialize market state for all symbols"""
    for symbol in Config.SYMBOLS:
        market_state[symbol] = MarketData(symbol=symbol)
        connection_status[symbol] = False

# ==========================================
# DERIV API CONNECTION
# ==========================================

class DerivAPI:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.reconnect_delay = 5
    
    async def connect(self):
        """Connect to Deriv WebSocket API"""
        global deriv_ws_connection
        
        # Check if App ID is set
        if Config.DERIV_APP_ID == "YOUR_APP_ID_HERE":
            print("⚠️  WARNING: Using default App ID. Set DERIV_APP_ID environment variable!")
            print("📝 Get your App ID from: https://deriv.com/developers/")
        
        while True:
            try:
                print(f"🔌 Connecting to Deriv API...")
                uri = f"{Config.DERIV_WS_URL}?app_id={Config.DERIV_APP_ID}"
                
                self.ws = await websockets.connect(uri)
                deriv_ws_connection = self.ws
                self.connected = True
                self.reconnect_delay = 5  # Reset on success
                
                print("✅ Connected to Deriv API")
                
                await self.handle_messages()
                
            except Exception as e:
                print(f"❌ Deriv connection error: {e}")
                self.connected = False
                deriv_ws_connection = None
                print(f"🔄 Reconnecting in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, 60)
    
    async def handle_messages(self):
        """Handle incoming WebSocket messages"""
        try:
            async for message in self.ws:
                data = json.loads(message)
                
                if "tick" in data:
                    await self.process_tick(data["tick"])
                elif "history" in data:
                    await self.process_history(data["history"])
                elif "error" in data:
                    print(f"⚠️ Deriv API error: {data['error']}")
                    # Mark all as disconnected on error
                    for sym in Config.SYMBOLS:
                        connection_status[sym] = False
                elif "authorize" in data:
                    print("🔐 Authorized")
                    await self.subscribe_to_ticks()
                    
        except websockets.exceptions.ConnectionClosed:
            print("🔌 Deriv connection closed")
            self.connected = False
            for sym in Config.SYMBOLS:
                connection_status[sym] = False
    
    async def process_tick(self, tick_data: dict):
        """Process real-time tick data"""
        symbol = tick_data.get("symbol", "")
        quote = tick_data.get("quote", 0)
        epoch = tick_data.get("epoch", 0)
        
        if symbol not in market_state:
            return
        
        state = market_state[symbol]
        
        state.price = float(quote)
        state.timestamp = datetime.fromtimestamp(epoch)
        
        state.price_history.append(state.price)
        state.high_history.append(float(tick_data.get("high", state.price)))
        state.low_history.append(float(tick_data.get("low", state.price)))
        
        if len(state.price_history) > Config.HISTORY_LENGTH:
            state.price_history.pop(0)
            state.high_history.pop(0)
            state.low_history.pop(0)
        
        if len(state.price_history) >= 21:
            state.ema_fast = TechnicalIndicators.calculate_ema(state.price_history, 9)
            state.ema_slow = TechnicalIndicators.calculate_ema(state.price_history, 21)
            state.rsi = TechnicalIndicators.calculate_rsi(state.price_history, 14)
            
            if len(state.high_history) >= 14:
                state.atr = TechnicalIndicators.calculate_atr(
                    state.high_history, 
                    state.low_history, 
                    state.price_history, 
                    14
                )
            
            state.signal, state.trend, state.score = TechnicalIndicators.calculate_signal(
                state.price_history,
                state.ema_fast,
                state.ema_slow,
                state.rsi
            )
        
        connection_status[symbol] = True
        await broadcast_update()
    
    async def process_history(self, history_data: dict):
        """Process historical data"""
        symbol = history_data.get("symbol", "")
        prices = history_data.get("prices", [])
        
        if symbol not in market_state or not prices:
            return
        
        state = market_state[symbol]
        state.price_history = [float(p) for p in prices[-Config.HISTORY_LENGTH:]]
        state.high_history = state.price_history.copy()
        state.low_history = state.price_history.copy()
        
        print(f"📊 Loaded history for {symbol}: {len(prices)} candles")
    
    async def subscribe_to_ticks(self):
        """Subscribe to tick streams"""
        for symbol in Config.SYMBOLS:
            subscribe_msg = {
                "ticks": symbol,
                "subscribe": 1
            }
            try:
                await self.ws.send(json.dumps(subscribe_msg))
                print(f"📡 Subscribed to {symbol}")
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Failed to subscribe to {symbol}: {e}")

# ==========================================
# DASHBOARD HTML
# ==========================================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Deriv Elite Scanner V3 | Live Market Data</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        :root {
            --bg-primary: #0a0e1a;
            --bg-secondary: #151b2d;
            --bg-tertiary: #1e2740;
            --text-primary: #ffffff;
            --text-secondary: #8b9dc3;
            --success: #00d084;
            --danger: #ff4757;
            --warning: #ffa502;
            --info: #3742fa;
            --purple: #8b5cf6;
        }
        
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }
        
        .bg-grid {
            position: fixed;
            top: 0; left: 0; width: 100%; height: 100%;
            background-image: 
                linear-gradient(rgba(55, 66, 250, 0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(55, 66, 250, 0.03) 1px, transparent 1px);
            background-size: 50px 50px;
            pointer-events: none;
            z-index: 0;
        }
        
        .container { 
            position: relative; 
            z-index: 1; 
            max-width: 1600px; 
            margin: 0 auto; 
        }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding: 24px 30px;
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-tertiary));
            border-radius: 20px;
            border: 1px solid rgba(255,255,255,0.1);
            box-shadow: 0 20px 60px rgba(0,0,0,0.5);
        }
        
        .brand { display: flex; align-items: center; gap: 15px; }
        
        .logo {
            width: 56px; height: 56px;
            background: linear-gradient(135deg, #3742fa, #00d084);
            border-radius: 16px;
            display: flex; align-items: center; justify-content: center;
            font-size: 28px;
            box-shadow: 0 0 30px rgba(55, 66, 250, 0.4);
            animation: pulse-glow 3s ease-in-out infinite;
        }
        
        @keyframes pulse-glow {
            0%, 100% { box-shadow: 0 0 20px rgba(55, 66, 250, 0.4); }
            50% { box-shadow: 0 0 40px rgba(0, 208, 132, 0.4); }
        }
        
        .brand h1 { 
            font-size: 26px; 
            font-weight: 800;
            background: linear-gradient(135deg, #fff, #8b9dc3);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .brand span { 
            font-size: 12px; 
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 2px;
        }
        
        .connection-badge {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 12px 24px;
            background: rgba(0, 208, 132, 0.1);
            border-radius: 30px;
            border: 1px solid rgba(0, 208, 132, 0.3);
            transition: all 0.3s;
        }
        
        .connection-badge.disconnected {
            background: rgba(255, 71, 87, 0.1);
            border-color: rgba(255, 71, 87, 0.3);
        }
        
        .connection-badge.connecting {
            background: rgba(255, 165, 2, 0.1);
            border-color: rgba(255, 165, 2, 0.3);
        }
        
        .pulse-dot {
            width: 10px; height: 10px;
            background: var(--success);
            border-radius: 50%;
            position: relative;
        }
        
        .pulse-dot::after {
            content: '';
            position: absolute;
            top: 50%; left: 50%;
            transform: translate(-50%, -50%);
            width: 100%; height: 100%;
            background: var(--success);
            border-radius: 50%;
            animation: ripple 2s infinite;
        }
        
        .disconnected .pulse-dot, .disconnected .pulse-dot::after { 
            background: var(--danger); 
            animation: none;
        }
        
        .connecting .pulse-dot, .connecting .pulse-dot::after { 
            background: var(--warning); 
            animation: ripple 1s infinite;
        }
        
        @keyframes ripple {
            0% { transform: translate(-50%, -50%) scale(1); opacity: 1; }
            100% { transform: translate(-50%, -50%) scale(3); opacity: 0; }
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-tertiary));
            padding: 24px;
            border-radius: 16px;
            border: 1px solid rgba(255,255,255,0.05);
            position: relative;
            overflow: hidden;
            transition: transform 0.3s, box-shadow 0.3s;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 20px 40px rgba(0,0,0,0.3);
        }
        
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; width: 100%; height: 3px;
            background: linear-gradient(90deg, #3742fa, #00d084);
            transform: scaleX(0);
            transition: transform 0.3s;
        }
        
        .stat-card:hover::before { transform: scaleX(1); }
        
        .stat-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        
        .stat-label {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .stat-icon { font-size: 24px; opacity: 0.6; }
        
        .stat-value {
            font-size: 36px;
            font-weight: 800;
            font-family: 'Courier New', monospace;
            background: linear-gradient(135deg, #fff, #8b9dc3);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .table-container {
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-tertiary));
            border-radius: 20px;
            border: 1px solid rgba(255,255,255,0.05);
            overflow: hidden;
            box-shadow: 0 25px 50px rgba(0,0,0,0.5);
        }
        
        .table-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 24px 30px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        
        .table-title { font-size: 18px; font-weight: 700; }
        
        .live-pill {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            background: rgba(0, 208, 132, 0.1);
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            color: var(--success);
            border: 1px solid rgba(0, 208, 132, 0.2);
        }
        
        .live-dot {
            width: 6px; height: 6px;
            background: var(--success);
            border-radius: 50%;
            animation: blink 1s infinite;
        }
        
        @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
        
        table { width: 100%; border-collapse: collapse; }
        
        thead { background: rgba(10, 14, 26, 0.8); }
        
        th {
            padding: 18px 24px;
            text-align: left;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
        }
        
        td {
            padding: 18px 24px;
            border-bottom: 1px solid rgba(255,255,255,0.03);
            font-size: 14px;
            transition: background 0.2s;
        }
        
        tr:hover td { background: rgba(255,255,255,0.02); }
        
        .symbol-cell { display: flex; align-items: center; gap: 12px; }
        
        .conn-indicator {
            width: 8px; height: 8px;
            border-radius: 50%;
            background: var(--success);
            box-shadow: 0 0 10px var(--success);
        }
        
        .conn-indicator.offline {
            background: var(--danger);
            box-shadow: 0 0 10px var(--danger);
        }
        
        .symbol-name { font-weight: 700; font-size: 15px; }
        
        .price-cell {
            font-family: 'Courier New', monospace;
            font-size: 16px;
            font-weight: 700;
        }
        
        .price-up { color: var(--success); }
        .price-down { color: var(--danger); }
        
        .signal-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 14px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border: 1px solid;
        }
        
        .signal-uptrend {
            background: rgba(0, 208, 132, 0.1);
            color: var(--success);
            border-color: rgba(0, 208, 132, 0.3);
            box-shadow: 0 0 20px rgba(0, 208, 132, 0.1);
        }
        
        .signal-downtrend {
            background: rgba(255, 71, 87, 0.1);
            color: var(--danger);
            border-color: rgba(255, 71, 87, 0.3);
            box-shadow: 0 0 20px rgba(255, 71, 87, 0.1);
        }
        
        .signal-range {
            background: rgba(255, 165, 2, 0.1);
            color: var(--warning);
            border-color: rgba(255, 165, 2, 0.3);
        }
        
        .signal-compression {
            background: rgba(139, 92, 246, 0.1);
            color: var(--purple);
            border-color: rgba(139, 92, 246, 0.3);
        }
        
        .signal-scanning, .signal-insufficient_data {
            background: rgba(55, 66, 250, 0.1);
            color: var(--info);
            border-color: rgba(55, 66, 250, 0.3);
        }
        
        .score-container { display: flex; align-items: center; gap: 12px; }
        
        .score-bar {
            width: 100px;
            height: 8px;
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }
        
        .score-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
        }
        
        .score-fill::after {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0; bottom: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        
        .score-value { font-weight: 700; font-size: 14px; min-width: 30px; }
        
        .metric {
            font-family: 'Courier New', monospace;
            font-size: 14px;
            color: var(--text-secondary);
        }
        
        .rsi-value {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 8px;
            font-weight: 700;
            font-size: 13px;
        }
        
        .rsi-overbought {
            background: rgba(255, 71, 87, 0.2);
            color: var(--danger);
        }
        
        .rsi-oversold {
            background: rgba(0, 208, 132, 0.2);
            color: var(--success);
        }
        
        .rsi-normal {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-secondary);
        }
        
        .ema-pair {
            display: flex;
            align-items: center;
            gap: 8px;
            font-family: 'Courier New', monospace;
            font-size: 13px;
        }
        
        .ema-fast { color: #60a5fa; font-weight: 700; }
        .ema-slow { color: #f472b6; font-weight: 700; }
        .ema-sep { color: var(--text-secondary); opacity: 0.5; }
        
        .flash-up { animation: flashGreen 0.5s ease; }
        .flash-down { animation: flashRed 0.5s ease; }
        
        @keyframes flashGreen {
            0% { background: rgba(0, 208, 132, 0.3); }
            100% { background: transparent; }
        }
        
        @keyframes flashRed {
            0% { background: rgba(255, 71, 87, 0.3); }
            100% { background: transparent; }
        }
        
        .footer {
            margin-top: 30px;
            text-align: center;
            color: var(--text-secondary);
            font-size: 12px;
            padding: 20px;
        }
        
        .update-time {
            font-family: 'Courier New', monospace;
            color: var(--info);
            font-weight: 600;
        }
        
        @media (max-width: 1200px) {
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            th, td { padding: 14px 16px; font-size: 12px; }
            .score-bar { width: 60px; }
        }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    
    <div class="container">
        <header>
            <div class="brand">
                <div class="logo">⚡</div>
                <div>
                    <h1>Deriv Elite Scanner</h1>
                    <span>Live Market Data</span>
                </div>
            </div>
            <div class="connection-badge" id="connBadge">
                <div class="pulse-dot"></div>
                <span style="font-weight: 600;">Connected</span>
            </div>
        </header>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-header">
                    <span class="stat-label">Active Symbols</span>
                    <span class="stat-icon">📊</span>
                </div>
                <div class="stat-value" id="statSymbols">--</div>
            </div>
            <div class="stat-card">
                <div class="stat-header">
                    <span class="stat-label">Live Feeds</span>
                    <span class="stat-icon">🔴</span>
                </div>
                <div class="stat-value" id="statConnected" style="color: var(--success);">--</div>
            </div>
            <div class="stat-card">
                <div class="stat-header">
                    <span class="stat-label">Top Signal</span>
                    <span class="stat-icon">🎯</span>
                </div>
                <div class="stat-value" id="statBest" style="font-size: 20px;">--</div>
            </div>
            <div class="stat-card">
                <div class="stat-header">
                    <span class="stat-label">Data Latency</span>
                    <span class="stat-icon">⚡</span>
                </div>
                <div class="stat-value" id="statLatency">--</div>
            </div>
        </div>
        
        <div class="table-container">
            <div class="table-header">
                <h3 class="table-title">Real-Time Market Monitor</h3>
                <div class="live-pill">
                    <div class="live-dot"></div>
                    <span>LIVE</span>
                </div>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Price</th>
                        <th>Signal</th>
                        <th>Trend</th>
                        <th>Score</th>
                        <th>ATR</th>
                        <th>RSI</th>
                        <th>EMA 9/21</th>
                    </tr>
                </thead>
                <tbody id="marketData"></tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Last Tick: <span class="update-time" id="lastUpdate">Connecting...</span></p>
            <p style="margin-top: 8px; opacity: 0.6;">Data provided by Deriv API | Technical Analysis Real-Time</p>
        </div>
    </div>
    
    <script>
        const signalIcons = {
            'UPTREND': '↗️', 'DOWNTREND': '↘️', 'RANGE': '↔️',
            'COMPRESSION': '🌀', 'SCANNING': '🔍', 
            'NEUTRAL': '➡️', 'INSUFFICIENT_DATA': '⚠️'
        };
        
        const signalClasses = {
            'UPTREND': 'signal-uptrend', 'DOWNTREND': 'signal-downtrend',
            'RANGE': 'signal-range', 'COMPRESSION': 'signal-compression',
            'SCANNING': 'signal-scanning', 'NEUTRAL': 'signal-scanning',
            'INSUFFICIENT_DATA': 'signal-insufficient_data'
        };
        
        let ws, previousPrices = {}, reconnectAttempts = 0;
        let lastUpdateTime = Date.now();
        
        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const badge = document.getElementById('connBadge');
            
            badge.className = 'connection-badge connecting';
            badge.querySelector('span').textContent = 'Connecting...';
            
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                console.log('✅ Dashboard connected');
                badge.className = 'connection-badge';
                badge.querySelector('span').textContent = 'Live';
                reconnectAttempts = 0;
            };
            
            ws.onmessage = (e) => {
                const data = JSON.parse(e.data);
                updateDashboard(data);
                lastUpdateTime = Date.now();
            };
            
            ws.onclose = () => {
                console.log('❌ Disconnected, retrying...');
                badge.className = 'connection-badge disconnected';
                badge.querySelector('span').textContent = 'Reconnecting...';
                
                const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
                reconnectAttempts++;
                setTimeout(connect, delay);
            };
            
            ws.onerror = (err) => {
                console.error('WebSocket error:', err);
            };
        }
        
        function updateDashboard(data) {
            const tbody = document.getElementById('marketData');
            const symbols = Object.keys(data.symbols).sort();
            
            // Update stats
            document.getElementById('statSymbols').textContent = symbols.length;
            
            const connected = symbols.filter(s => data.symbols[s].connected).length;
            document.getElementById('statConnected').textContent = `${connected}/${symbols.length}`;
            
            // Find best signal
            let best = { score: -1, symbol: '-', signal: '-' };
            symbols.forEach(s => {
                const sym = data.symbols[s];
                if (sym.score > best.score && sym.signal !== 'INSUFFICIENT_DATA') {
                    best = { score: sym.score, symbol: s, signal: sym.signal };
                }
            });
            const bestEl = document.getElementById('statBest');
            bestEl.textContent = best.score > 0 ? `${best.symbol}` : '-';
            bestEl.style.color = best.signal === 'UPTREND' ? '#00d084' : 
                                best.signal === 'DOWNTREND' ? '#ff4757' : '#ffa502';
            
            // Calculate latency
            const latency = Date.now() - new Date(data.timestamp).getTime();
            document.getElementById('statLatency').textContent = `${latency}ms`;
            
            // Update table
            symbols.forEach(symbol => {
                const s = data.symbols[symbol];
                let row = document.getElementById(`row-${symbol}`);
                
                if (!row) {
                    row = document.createElement('tr');
                    row.id = `row-${symbol}`;
                    tbody.appendChild(row);
                }
                
                // Price flash effect
                let flashClass = '';
                if (previousPrices[symbol] !== undefined) {
                    if (s.price > previousPrices[symbol]) flashClass = 'flash-up';
                    else if (s.price < previousPrices[symbol]) flashClass = 'flash-down';
                }
                previousPrices[symbol] = s.price;
                
                if (flashClass) {
                    row.className = flashClass;
                    setTimeout(() => row.className = '', 500);
                }
                
                // RSI styling
                let rsiClass = 'rsi-normal';
                if (s.rsi > 70) rsiClass = 'rsi-overbought';
                else if (s.rsi < 30) rsiClass = 'rsi-oversold';
                
                // Score color
                let scoreColor = s.score > 70 ? '#00d084' : s.score > 40 ? '#ffa502' : '#ff4757';
                
                // Price color
                const priceClass = s.price > previousPrices[symbol] ? 'price-up' : 
                                  s.price < previousPrices[symbol] ? 'price-down' : '';
                
                row.innerHTML = `
                    <td>
                        <div class="symbol-cell">
                            <div class="conn-indicator ${s.connected ? '' : 'offline'}"></div>
                            <span class="symbol-name">${symbol}</span>
                        </div>
                    </td>
                    <td class="price-cell ${priceClass}">${s.price.toFixed(5)}</td>
                    <td>
                        <span class="signal-badge ${signalClasses[s.signal] || 'signal-scanning'}">
                            ${signalIcons[s.signal] || '⚪'} ${s.signal.replace(/_/g, ' ')}
                        </span>
                    </td>
                    <td>${s.trend}</td>
                    <td>
                        <div class="score-container">
                            <div class="score-bar">
                                <div class="score-fill" style="width: ${Math.min(s.score, 100)}%; background: ${scoreColor};"></div>
                            </div>
                            <span class="score-value" style="color: ${scoreColor};">${s.score}</span>
                        </div>
                    </td>
                    <td class="metric">${s.atr.toFixed(5)}</td>
                    <td>
                        <span class="rsi-value ${rsiClass}">${s.rsi.toFixed(1)}</span>
                    </td>
                    <td>
                        <div class="ema-pair">
                            <span class="ema-fast">${s.ema_fast.toFixed(2)}</span>
                            <span class="ema-sep">/</span>
                            <span class="ema-slow">${s.ema_slow.toFixed(2)}</span>
                        </div>
                    </td>
                `;
            });
            
            document.getElementById('lastUpdate').textContent = new Date(data.timestamp).toLocaleTimeString();
        }
        
        // Check for stale data
        setInterval(() => {
            const stale = Date.now() - lastUpdateTime > 5000;
            if (stale) {
                document.getElementById('connBadge').classList.add('disconnected');
            }
        }, 1000);
        
        connect();
    </script>
</body>
</html>
"""

# ==========================================
# WEBSOCKET BROADCAST
# ==========================================

async def broadcast_update():
    """Broadcast market data to all connected dashboard clients"""
    if not connected_clients:
        return
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "symbols": {}
    }
    
    for symbol, state in market_state.items():
        data["symbols"][symbol] = {
            "price": round(state.price, 5),
            "signal": state.signal,
            "trend": state.trend,
            "score": state.score,
            "atr": round(state.atr, 5),
            "rsi": round(state.rsi, 2),
            "ema_fast": round(state.ema_fast, 2),
            "ema_slow": round(state.ema_slow, 2),
            "connected": connection_status.get(symbol, False)
        }
    
    dead_clients = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except Exception as e:
            dead_clients.append(client)
    
    for client in dead_clients:
        if client in connected_clients:
            connected_clients.remove(client)

# ==========================================
# FASTAPI APPLICATION
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_state()
    print("🚀 Starting Deriv Elite Scanner V3...")
    print(f"📊 Monitoring {len(Config.SYMBOLS)} symbols")
    print(f"🔌 Connecting to Deriv API (App ID: {Config.DERIV_APP_ID})...")
    
    # Start Deriv API connection in background
    deriv_api = DerivAPI()
    deriv_task = asyncio.create_task(deriv_api.connect())
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    deriv_task.cancel()
    try:
        await deriv_task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="Deriv Elite Scanner V3",
    description="Real-time market analysis with Deriv API",
    version="3.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"📱 Dashboard client connected. Total: {len(connected_clients)}")
    
    # Send initial data immediately
    await broadcast_update()
    
    try:
        while True:
            msg = await websocket.receive_text()
            if msg == "ping":
                await websocket.send_text("pong")
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
        print(f"📱 Client disconnected. Total: {len(connected_clients)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
