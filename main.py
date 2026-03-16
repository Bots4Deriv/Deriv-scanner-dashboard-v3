import asyncio
import json
import websockets
import statistics
import time
import os
import requests
import signal
import sys
from aiohttp import web
from datetime import datetime, timedelta
import pytz
from collections import deque
import uuid
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# =========================
# TIMEZONE & CONFIG
# =========================
EAT = pytz.timezone('Africa/Nairobi')

def get_eat_time():
    return datetime.now(EAT)

def get_eat_timestamp():
    return get_eat_time().strftime("%Y-%m-%d %H:%M:%S")

def get_eat_clock():
    return get_eat_time().strftime("%H:%M:%S")

# Railway & Core Config
PORT = int(os.getenv("PORT", "8080"))
AUTO_TRADE_ENABLED = os.getenv("AUTO_TRADE_ENABLED", "false").lower() == "true"
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")
SYMBOL = os.getenv("SYMBOL", "R_25")

# Risk Settings
STAKE_AMOUNT = float(os.getenv("STAKE_AMOUNT", "1.0"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "50.0"))
MAX_DAILY_PROFIT = float(os.getenv("MAX_DAILY_PROFIT", "100.0"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "20"))
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
MAX_STAKE_PERCENT = float(os.getenv("MAX_STAKE_PERCENT", "5.0"))

# Martingale
MARTINGALE_ENABLED = os.getenv("MARTINGALE_ENABLED", "false").lower() == "true"
MARTINGALE_MAX_STEPS = int(os.getenv("MARTINGALE_MAX_STEPS", "3"))
MARTINGALE_MULTIPLIER = float(os.getenv("MARTINGALE_MULTIPLIER", "2.0"))

# Trade Settings
TRADE_DURATION = int(os.getenv("TRADE_DURATION", "5"))
TRADE_DURATION_UNIT = os.getenv("TRADE_DURATION_UNIT", "m")
COOLDOWN_AFTER_LOSS = int(os.getenv("COOLDOWN_AFTER_LOSS", "3"))
COOLDOWN_AFTER_WIN = int(os.getenv("COOLDOWN_AFTER_WIN", "1"))

# Signal Settings
MAX_TICKS = int(os.getenv("MAX_TICKS", "200"))
GENERAL_TREND_LOOKBACK = int(os.getenv("GENERAL_TREND_LOOKBACK", "300"))
RANGE_LIMIT_30 = float(os.getenv("RANGE_LIMIT_30", "1.8"))
STRETCH_LIMIT = float(os.getenv("STRETCH_LIMIT", "0.7"))
MOMENTUM_LIMIT = int(os.getenv("MOMENTUM_LIMIT", "11"))
SPIKE_LIMIT = float(os.getenv("SPIKE_LIMIT", "0.8"))

# Notifications
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
PRINT_EVERY_TICK = os.getenv("PRINT_EVERY_TICK", "false").lower() == "true"
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "2"))

# =========================
# GLOBAL STATE
# =========================
prices = []
times = []
last_signal = None
last_signal_time = 0
previous_price = None
price_history = []
general_trend_history = deque(maxlen=GENERAL_TREND_LOOKBACK)
shutdown_event = asyncio.Event()

# Separate WebSocket connections
market_ws = None      # For ticks/market data
trading_ws = None     # For trading operations
trading_ws_lock = asyncio.Lock()  # Lock for trading WS operations

bot_status = {
    "running": False,
    "last_price": None,
    "last_signal": None,
    "started_at": None,
    "trend": "➡️",
    "price_change": 0,
    "general_trend": {},
    "balance": 0.0,
    "daily_pnl": 0.0,
    "total_trades_today": 0,
    "win_count": 0,
    "loss_count": 0,
    "concurrent_trades": 0,
    "last_trade_time": None,
    "trading_enabled": AUTO_TRADE_ENABLED,
    "cooldown_until": None,
    "daily_limit_hit": False
}

active_trades = {}
trade_history = deque(maxlen=100)
daily_stats = {
    "date": get_eat_time().date(),
    "trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss": 0.0
}

martingale_state = {
    "active": False,
    "current_step": 0,
    "base_stake": STAKE_AMOUNT,
    "consecutive_losses": 0
}

# =========================
# RISK MANAGER
# =========================
class RiskManager:
    def check_daily_reset(self):
        current_date = get_eat_time().date()
        if current_date != daily_stats["date"]:
            daily_stats.update({
                "date": current_date, "trades": 0, "wins": 0, "losses": 0,
                "profit": 0.0, "loss": 0.0
            })
            bot_status.update({
                "daily_pnl": 0.0, "total_trades_today": 0,
                "win_count": 0, "loss_count": 0, "daily_limit_hit": False
            })
            martingale_state["consecutive_losses"] = 0
            logger.info(f"🌅 Daily reset: {current_date}")

    def can_trade(self):
        if not AUTO_TRADE_ENABLED:
            return False, "Auto-trading disabled"
        if not DERIV_API_TOKEN:
            return False, "No API token"
        
        self.check_daily_reset()
        
        if bot_status["daily_limit_hit"]:
            return False, "Daily limit hit"
        
        if bot_status["daily_pnl"] <= -MAX_DAILY_LOSS:
            bot_status["daily_limit_hit"] = True
            send_telegram(f"🛑 STOP LOSS: ${bot_status['daily_pnl']:.2f}")
            return False, "Daily loss limit"
        
        if bot_status["daily_pnl"] >= MAX_DAILY_PROFIT:
            bot_status["daily_limit_hit"] = True
            send_telegram(f"🎯 PROFIT TARGET: ${bot_status['daily_pnl']:.2f}")
            return False, "Daily profit target"
        
        if daily_stats["trades"] >= MAX_TRADES_PER_DAY:
            return False, "Max trades reached"
        
        if bot_status["concurrent_trades"] >= MAX_CONCURRENT_TRADES:
            return False, "Max concurrent trades"
        
        if bot_status["cooldown_until"] and get_eat_time() < bot_status["cooldown_until"]:
            remaining = int((bot_status["cooldown_until"] - get_eat_time()).total_seconds() / 60)
            return False, f"Cooldown {remaining}m"
        
        return True, "OK"

    def calculate_stake(self):
        balance = bot_status.get("balance", 0)
        max_by_balance = (balance * MAX_STAKE_PERCENT) / 100
        
        if MARTINGALE_ENABLED and martingale_state["consecutive_losses"] > 0:
            step = min(martingale_state["consecutive_losses"], MARTINGALE_MAX_STEPS)
            stake = STAKE_AMOUNT * (MARTINGALE_MULTIPLIER ** step)
            stake = min(stake, max_by_balance, STAKE_AMOUNT * 10)
            return round(stake, 2)
        
        return round(min(STAKE_AMOUNT, max_by_balance), 2)

    def update_after_trade(self, profit_loss):
        daily_stats["trades"] += 1
        bot_status["total_trades_today"] = daily_stats["trades"]
        bot_status["concurrent_trades"] = max(0, bot_status["concurrent_trades"] - 1)
        
        if profit_loss > 0:
            daily_stats["wins"] += 1
            daily_stats["profit"] += profit_loss
            bot_status["win_count"] = daily_stats["wins"]
            bot_status["daily_pnl"] += profit_loss
            martingale_state["consecutive_losses"] = 0
            bot_status["cooldown_until"] = get_eat_time() + timedelta(minutes=COOLDOWN_AFTER_WIN)
        else:
            daily_stats["losses"] += 1
            daily_stats["loss"] += abs(profit_loss)
            bot_status["loss_count"] = daily_stats["losses"]
            bot_status["daily_pnl"] += profit_loss
            martingale_state["consecutive_losses"] += 1
            bot_status["cooldown_until"] = get_eat_time() + timedelta(minutes=COOLDOWN_AFTER_LOSS)

risk_manager = RiskManager()

# =========================
# TRADING WEBSOCKET (Separate Connection)
# =========================
async def get_trading_ws():
    """Get or create dedicated trading WebSocket"""
    global trading_ws
    
    async with trading_ws_lock:
        if trading_ws is None or trading_ws.closed:
            try:
                url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
                trading_ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
                
                # Authorize immediately
                if DERIV_API_TOKEN:
                    await trading_ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                    response = await asyncio.wait_for(trading_ws.recv(), timeout=5.0)
                    data = json.loads(response)
                    
                    if "authorize" in data:
                        # Get balance
                        await trading_ws.send(json.dumps({"balance": 1}))
                        response = await asyncio.wait_for(trading_ws.recv(), timeout=5.0)
                        data = json.loads(response)
                        if "balance" in data:
                            bot_status["balance"] = float(data["balance"]["balance"])
                
                logger.info("🔌 Trading WebSocket connected")
            except Exception as e:
                logger.error(f"Trading WS error: {e}")
                trading_ws = None
                return None
        
        return trading_ws

async def close_trading_ws():
    """Close trading WebSocket gracefully"""
    global trading_ws
    async with trading_ws_lock:
        if trading_ws and not trading_ws.closed:
            await trading_ws.close()
            trading_ws = None

async def place_trade(contract_type="CALL"):
    """Place trade using dedicated WebSocket"""
    can_trade, reason = risk_manager.can_trade()
    if not can_trade:
        logger.info(f"Trade blocked: {reason}")
        return None
    
    stake = risk_manager.calculate_stake()
    if stake <= 0:
        return None
    
    # Get fresh trading connection
    ws = await get_trading_ws()
    if not ws:
        logger.error("No trading connection available")
        return None
    
    trade_id = str(uuid.uuid4())[:8]
    
    try:
        async with trading_ws_lock:
            contract = {
                "buy": 1,
                "price": stake,
                "parameters": {
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": contract_type,
                    "currency": "USD",
                    "duration": TRADE_DURATION,
                    "duration_unit": TRADE_DURATION_UNIT,
                    "symbol": SYMBOL
                }
            }
            
            await ws.send(json.dumps(contract))
            response = await asyncio.wait_for(ws.recv(), timeout=10.0)
            data = json.loads(response)
            
            if "buy" in data:
                contract_id = data["buy"]["contract_id"]
                
                trade_info = {
                    "id": trade_id,
                    "contract_id": contract_id,
                    "type": contract_type,
                    "stake": stake,
                    "entry_price": bot_status["last_price"],
                    "symbol": SYMBOL,
                    "start_time": get_eat_timestamp(),
                    "status": "OPEN"
                }
                
                active_trades[trade_id] = trade_info
                bot_status["concurrent_trades"] += 1
                
                # Start monitor in separate task with its own connection
                asyncio.create_task(monitor_trade_with_new_ws(trade_id, contract_id))
                
                msg = f"🚀 TRADE: {contract_type} ${stake:.2f} | ID: {trade_id}"
                logger.info(msg)
                send_telegram(msg)
                return trade_info
            else:
                error = data.get("error", {}).get("message", "Unknown")
                logger.error(f"Buy failed: {error}")
                return None
                
    except Exception as e:
        logger.error(f"Place trade error: {e}")
        return None

async def monitor_trade_with_new_ws(trade_id, contract_id):
    """
    Monitor trade using a COMPLETELY SEPARATE WebSocket connection.
    This avoids the 'another coroutine' error.
    """
    ws = None
    try:
        # Create independent connection for this monitor
        url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
        ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
        
        # Authorize
        if DERIV_API_TOKEN:
            await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
            await asyncio.wait_for(ws.recv(), timeout=5.0)
        
        # Subscribe to contract updates
        subscribe_msg = {
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1
        }
        await ws.send(json.dumps(subscribe_msg))
        
        while not shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                data = json.loads(msg)
                
                if "proposal_open_contract" in data:
                    contract = data["proposal_open_contract"]
                    
                    # Update current profit
                    current_profit = float(contract.get("profit", 0))
                    if trade_id in active_trades:
                        active_trades[trade_id]["current_profit"] = current_profit
                    
                    # Check if sold/completed
                    if contract.get("is_sold"):
                        profit = float(contract.get("profit", 0))
                        status = "WIN" if profit > 0 else "LOSS"
                        
                        if trade_id in active_trades:
                            active_trades[trade_id].update({
                                "status": status,
                                "profit": profit,
                                "exit_time": get_eat_timestamp()
                            })
                            trade_history.append(active_trades[trade_id].copy())
                        
                        risk_manager.update_after_trade(profit)
                        
                        emoji = "✅" if profit > 0 else "❌"
                        msg = f"{emoji} {status} ${profit:+.2f} | ID: {trade_id} | Daily: ${bot_status['daily_pnl']:.2f}"
                        logger.info(msg)
                        send_telegram(msg)
                        
                        if trade_id in active_trades:
                            del active_trades[trade_id]
                        break
                        
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"Monitor WS closed for {trade_id}, reconnecting...")
                # Reconnect and resubscribe
                try:
                    ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
                    if DERIV_API_TOKEN:
                        await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                        await asyncio.wait_for(ws.recv(), timeout=5.0)
                    await ws.send(json.dumps(subscribe_msg))
                except Exception as e:
                    logger.error(f"Reconnect failed: {e}")
                    break
                
    except Exception as e:
        logger.error(f"Monitor error for {trade_id}: {e}")
    finally:
        if ws and not ws.closed:
            await ws.close()
        # Ensure trade count is decremented if something went wrong
        if trade_id in active_trades:
            del active_trades[trade_id]
        bot_status["concurrent_trades"] = max(0, bot_status["concurrent_trades"] - 1)

# =========================
# CORE ANALYSIS
# =========================
def calculate_trend(current, previous):
    if previous is None:
        return "➡️", 0
    change = current - previous
    return ("⬆️", change) if change > 0 else ("⬇️", change) if change < 0 else ("➡️", 0)

def calculate_general_trend():
    if len(general_trend_history) < 50:
        return {"direction": "INSUFFICIENT DATA ⏳", "strength": 0, "duration_ticks": len(general_trend_history)}
    
    prices_list = list(general_trend_history)
    start, current = prices_list[0], prices_list[-1]
    total_change = current - start
    pct_change = (total_change / start) * 100 if start != 0 else 0
    
    up = sum(1 for i in range(1, len(prices_list)) if prices_list[i] > prices_list[i-1])
    down = len(prices_list) - 1 - up
    
    direction = "BULLISH 📈" if pct_change > 1 else "BEARISH 📉" if pct_change < -1 else "NEUTRAL ➡️"
    consistency = max(up, down) / (up + down) if (up + down) > 0 else 0
    strength = min(abs(pct_change) * 10, 50) + (consistency * 50)
    
    return {
        "direction": direction,
        "strength": round(max(0, min(100, strength)), 1),
        "strength_emoji": "🔥" if strength >= 70 else "⚡" if strength >= 40 else "💤",
        "duration_ticks": len(prices_list),
        "total_change": round(total_change, 3),
        "percent_change": round(pct_change, 2),
        "consistency": round(consistency * 100, 1),
        "up_moves": up,
        "down_moves": down
    }

def analyze_signal(prices_list):
    if len(prices_list) < 30:
        return "WAIT", {}
    
    last_30 = prices_list[-30:]
    last_15 = prices_list[-15:]
    current = prices_list[-1]
    
    range_30 = max(last_30) - min(last_30)
    if range_30 > RANGE_LIMIT_30:
        return "WAIT - HIGH VOLATILITY", {"range_30": round(range_30, 3)}
    
    avg_10 = sum(prices_list[-10:]) / 10
    stretch = abs(current - avg_10)
    if stretch > STRETCH_LIMIT:
        return "WAIT - STRETCHED", {"stretch": round(stretch, 3)}
    
    moves = [last_15[i] - last_15[i-1] for i in range(1, len(last_15))]
    up = sum(1 for m in moves if m > 0)
    down = sum(1 for m in moves if m < 0)
    if up >= MOMENTUM_LIMIT or down >= MOMENTUM_LIMIT:
        return "WAIT - MOMENTUM", {"up": up, "down": down}
    
    recent_changes = [abs(prices_list[-6:][i]-prices_list[-6:][i-1]) for i in range(1, 6)]
    if max(recent_changes) > SPIKE_LIMIT:
        return "WAIT - SPIKE", {"max_spike": max(recent_changes)}
    
    return "ENTER", {
        "range_30": round(range_30, 3),
        "stretch": round(stretch, 3),
        "up_moves": up,
        "down_moves": down
    }

def should_print():
    global last_signal, last_signal_time
    now = time.time()
    if PRINT_EVERY_TICK:
        return True
    if bot_status["last_signal"] != last_signal:
        last_signal = bot_status["last_signal"]
        last_signal_time = now
        return True
    if now - last_signal_time >= SIGNAL_COOLDOWN:
        last_signal_time = now
        return True
    return False

def send_telegram(msg):
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg},
                timeout=5
            )
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# =========================
# MARKET DATA WEBSOCKET (Main Loop)
# =========================
async def stream_ticks():
    global prices, times, price_history, previous_price, general_trend_history, market_ws
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    bot_status["running"] = True
    bot_status["started_at"] = get_eat_timestamp()
    
    logger.info(f"🚀 Starting | Symbol: {SYMBOL} | Trading: {AUTO_TRADE_ENABLED}")

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                market_ws = ws
                logger.info("✅ Market WebSocket connected")
                
                # Only for balance display - trading WS handles actual trading
                if AUTO_TRADE_ENABLED and DERIV_API_TOKEN:
                    try:
                        await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                        response = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        # Don't process further, just for auth
                    except:
                        pass

                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))

                while not shutdown_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(msg)

                        if "error" in data:
                            logger.error(f"Deriv error: {data['error']}")
                            break

                        if "tick" in data:
                            tick = data["tick"]
                            quote = float(tick["quote"])
                            
                            trend, change = calculate_trend(quote, previous_price)
                            previous_price = quote
                            
                            general_trend_history.append(quote)
                            general_trend = calculate_general_trend()
                            bot_status["general_trend"] = general_trend
                            
                            prices.append(quote)
                            times.append(tick["epoch"])
                            price_history.append({
                                "price": quote,
                                "time": get_eat_clock(),
                                "trend": trend,
                                "change": round(change, 3)
                            })
                            if len(price_history) > 100:
                                price_history.pop(0)
                            if len(prices) > MAX_TICKS:
                                prices.pop(0)
                                times.pop(0)

                            signal, debug = analyze_signal(prices)
                            bot_status["last_price"] = quote
                            bot_status["last_signal"] = signal
                            bot_status["trend"] = trend
                            bot_status["price_change"] = round(change, 3)

                            # AUTO-TRADING
                            if signal == "ENTER" and AUTO_TRADE_ENABLED:
                                can_trade, reason = risk_manager.can_trade()
                                if can_trade:
                                    direction = "CALL" if "BULLISH" in general_trend.get("direction", "") else \
                                               "PUT" if "BEARISH" in general_trend.get("direction", "") else \
                                               "CALL" if trend == "⬆️" else "PUT"
                                    await place_trade(direction)
                                elif "Daily" in reason or "limit" in reason:
                                    logger.warning(f"Trading blocked: {reason}")

                            if should_print():
                                pnl = f" | P&L:${bot_status['daily_pnl']:+.2f}" if AUTO_TRADE_ENABLED else ""
                                logger.info(f"[{get_eat_clock()}] {quote:.3f} {trend} | {signal}{pnl}")

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"Message error: {e}")
                        break

        except Exception as e:
            logger.error(f"Connection error: {e}")
            market_ws = None
            if not shutdown_event.is_set():
                await asyncio.sleep(3)

    bot_status["running"] = False
    await close_trading_ws()
    logger.info("🛑 Bot stopped")

# =========================
# WEB SERVER
# =========================
async def health_check(request):
    return web.json_response({
        "status": "healthy",
        "running": bot_status["running"],
        "timestamp": get_eat_timestamp()
    })

async def api_status(request):
    total = bot_status["win_count"] + bot_status["loss_count"]
    win_rate = (bot_status["win_count"] / total * 100) if total > 0 else 0
    
    cooldown_remaining = 0
    if bot_status["cooldown_until"] and get_eat_time() < bot_status["cooldown_until"]:
        cooldown_remaining = int((bot_status["cooldown_until"] - get_eat_time()).total_seconds() / 60)
    
    return web.json_response({
        "status": "healthy",
        "running": bot_status["running"],
        "timestamp": get_eat_timestamp(),
        "symbol": SYMBOL,
        "trading_enabled": AUTO_TRADE_ENABLED,
        "last_price": bot_status.get("last_price"),
        "last_signal": bot_status.get("last_signal"),
        "general_trend": bot_status.get("general_trend", {}),
        "price_history": price_history[-50:],
        "balance": round(bot_status.get("balance", 0), 2),
        "daily_pnl": round(bot_status.get("daily_pnl", 0), 2),
        "total_trades": bot_status.get("total_trades_today", 0),
        "win_count": bot_status.get("win_count", 0),
        "loss_count": bot_status.get("loss_count", 0),
        "win_rate": round(win_rate, 1),
        "concurrent_trades": bot_status.get("concurrent_trades", 0),
        "active_trades": active_trades,
        "daily_limit_hit": bot_status.get("daily_limit_hit", False),
        "cooldown_remaining": cooldown_remaining,
        "next_stake": risk_manager.calculate_stake() if AUTO_TRADE_ENABLED else 0
    })

async def start_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_get('/api/status', api_status)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Server ready on port {PORT}")

# =========================
# MAIN
# =========================
def handle_signal(sig, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()
    sys.exit(0)

async def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    await asyncio.gather(
        start_server(),
        stream_ticks()
    )

if __name__ == "__main__":
    if AUTO_TRADE_ENABLED and not DERIV_API_TOKEN:
        logger.warning("⚠️ AUTO_TRADE_ENABLED but no DERIV_API_TOKEN")
        AUTO_TRADE_ENABLED = False
        bot_status["trading_enabled"] = False
    
    asyncio.run(main())
