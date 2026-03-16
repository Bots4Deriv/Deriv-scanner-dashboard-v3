import asyncio
import json
import websockets
import numpy as np
import statistics
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import threading

API_TOKEN = "YOUR_DERIV_TOKEN"
SYMBOLS = ["R_10", "R_25", "R_50", "R_75", "R_100"]
TIMEFRAME = 60
CANDLE_COUNT = 80
TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

app = FastAPI()
market_state = {s: {"price": 0, "signal": "SCANNING", "score": 0} for s in SYMBOLS}
last_signal = {}  # Moved to global scope properly

# ---------------- TELEGRAM ----------------
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg})
    except:
        pass

# ---------------- INDICATORS ----------------
def atr(highs, lows, closes, period=14):
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
    return statistics.mean(trs[-period:])

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = closes[-i] - closes[-i-1]
        if diff > 0:
            gains.append(diff)
        else:
            losses.append(abs(diff))
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def ema(data, period):
    if len(data) < period:
        return None
    # Note: This is SMA, not true EMA. Real EMA requires recursive calculation
    return np.mean(data[-period:])

# ---------------- SIGNAL STRENGTH ----------------
def signal_strength(atr_val, rsi_val, range_val):
    score = 0
    if atr_val < 0.35:
        score += 30
    if 45 < rsi_val < 55:
        score += 20
    if range_val < 7:
        score += 30
    if atr_val < 0.25:
        score += 20
    return score

# ---------------- ANALYSIS ----------------
def analyze(symbol, closes, highs, lows):
    global last_signal
    atr_val = atr(highs, lows, closes)
    rsi_val = rsi(closes)
    
    if atr_val is None or rsi_val is None:
        return
    
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    price = closes[-1]
    high = max(highs[-5:])
    low = min(lows[-5:])
    range_val = high - low
    score = signal_strength(atr_val, rsi_val, range_val)
    
    market_state[symbol]["price"] = round(price, 2)
    market_state[symbol]["score"] = score
    
    # RANGE SIGNAL
    if score >= 70:
        market_state[symbol]["signal"] = "RANGE"
        if last_signal.get(symbol) != "range":
            msg = f"{symbol} RANGE MARKET\nScore {score}/100\nPrice {price}\nExpected ±3.5 range"
            send_telegram(msg)
            last_signal[symbol] = "range"
    
    # COMPRESSION SIGNAL
    elif atr_val < 0.20 and range_val < 5:
        market_state[symbol]["signal"] = "COMPRESSION"
        if last_signal.get(symbol) != "compression":
            msg = f"{symbol} VOLATILITY COMPRESSION\nBig breakout likely soon\nPrice {price}"
            send_telegram(msg)
            last_signal[symbol] = "compression"
    
    # TREND SIGNALS
    elif ema9 and ema21:
        if ema9 > ema21 and rsi_val > 60:
            market_state[symbol]["signal"] = "UPTREND"
            if last_signal.get(symbol) != "uptrend":
                send_telegram(f"{symbol} STRONG UP TREND")
                last_signal[symbol] = "uptrend"
        elif ema9 < ema21 and rsi_val < 40:
            market_state[symbol]["signal"] = "DOWNTREND"
            if last_signal.get(symbol) != "downtrend":
                send_telegram(f"{symbol} STRONG DOWN TREND")
                last_signal[symbol] = "downtrend"
        else:
            market_state[symbol]["signal"] = "NEUTRAL"
    else:
        market_state[symbol]["signal"] = "NEUTRAL"

# ---------------- SCANNER ----------------
async def scan(symbol):
    closes = []
    highs = []
    lows = []
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"authorize": API_TOKEN}))
        await ws.recv()
        
        await ws.send(json.dumps({
            "ticks_history": symbol,
            "style": "candles",
            "granularity": TIMEFRAME,
            "count": CANDLE_COUNT
        }))
        
        data = json.loads(await ws.recv())
        
        # Check for errors in response
        if "error" in data:
            print(f"Error fetching history for {symbol}: {data['error']}")
            return
            
        for c in data.get("candles", []):
            closes.append(c["close"])
            highs.append(c["high"])
            lows.append(c["low"])
        
        await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
        
        while True:
            try:
                msg = json.loads(await ws.recv())
                
                if "tick" in msg:
                    price = msg["tick"]["quote"]
                    closes.append(price)
                    highs.append(price)
                    lows.append(price)
                    
                    if len(closes) > 150:
                        closes.pop(0)
                        highs.pop(0)
                        lows.pop(0)
                    
                    analyze(symbol, closes, highs, lows)
                    
                elif "error" in msg:
                    print(f"WebSocket error for {symbol}: {msg['error']}")
                    
            except websockets.exceptions.ConnectionClosed:
                print(f"Connection closed for {symbol}, reconnecting...")
                break
            except Exception as e:
                print(f"Error in {symbol} scan: {e}")

# ---------------- DASHBOARD ----------------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    rows = ""
    for s, data in market_state.items():
        signal_color = {
            "RANGE": "#ff9800",
            "COMPRESSION": "#9c27b0", 
            "UPTREND": "#4caf50",
            "DOWNTREND": "#f44336",
            "NEUTRAL": "#757575",
            "SCANNING": "#2196f3"
        }.get(data['signal'], 'white')
        
        rows += f"""
        <tr>
            <td>{s}</td>
            <td>{data['price']}</td>
            <td style="color:{signal_color};font-weight:bold">{data['signal']}</td>
            <td>{data['score']}</td>
        </tr>
        """
    
    html = f"""
    <html>
    <head>
        <title>Deriv Scanner</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: Arial; background: #111; color: white; text-align: center; padding: 20px; }}
            table {{ margin: auto; border-collapse: collapse; width: 80%; max-width: 800px; }}
            td, th {{ border: 1px solid #444; padding: 12px; }}
            th {{ background: #222; }}
            tr:hover {{ background: #1a1a1a; }}
            h2 {{ color: #00bcd4; }}
        </style>
    </head>
    <body>
        <h2>Deriv Volatility Market Scanner</h2>
        <table>
            <tr>
                <th>Symbol</th>
                <th>Price</th>
                <th>Signal</th>
                <th>Strength</th>
            </tr>
            {rows}
        </table>
        <p style="margin-top:20px;color:#666;font-size:12px">Auto-refresh every 2 seconds</p>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ---------------- RUN ----------------
async def main_scanner():
    tasks = [scan(s) for s in SYMBOLS]
    await asyncio.gather(*tasks)

def start_scanner():
    asyncio.run(main_scanner())

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=start_scanner, daemon=True).start()

# For direct execution (optional)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
