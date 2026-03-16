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

# =========================
# RAILWAY CONFIG
# =========================
def get_env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default

def get_env_float(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default

def get_env_bool(name, default=False):
    val = os.getenv(name, str(default).lower())
    return val.lower() in ('true', '1', 'yes', 'on')

# Core settings
PORT = get_env_int("PORT", 8080)
AUTO_TRADE_ENABLED = get_env_bool("AUTO_TRADE_ENABLED", False)
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")
SYMBOL = os.getenv("SYMBOL", "R_25")

# Risk settings
STAKE_AMOUNT = get_env_float("STAKE_AMOUNT", 1.0)
MAX_DAILY_LOSS = get_env_float("MAX_DAILY_LOSS", 50.0)
MAX_DAILY_PROFIT = get_env_float("MAX_DAILY_PROFIT", 100.0)
MAX_TRADES_PER_DAY = get_env_int("MAX_TRADES_PER_DAY", 20)
MAX_CONCURRENT_TRADES = get_env_int("MAX_CONCURRENT_TRADES", 3)
MAX_STAKE_PERCENT = get_env_float("MAX_STAKE_PERCENT", 5.0)

# Martingale
MARTINGALE_ENABLED = get_env_bool("MARTINGALE_ENABLED", False)
MARTINGALE_MAX_STEPS = get_env_int("MARTINGALE_MAX_STEPS", 3)
MARTINGALE_MULTIPLIER = get_env_float("MARTINGALE_MULTIPLIER", 2.0)

# Trade settings
TRADE_DURATION = get_env_int("TRADE_DURATION", 5)
TRADE_DURATION_UNIT = os.getenv("TRADE_DURATION_UNIT", "m")
COOLDOWN_AFTER_LOSS = get_env_int("COOLDOWN_AFTER_LOSS", 3)
COOLDOWN_AFTER_WIN = get_env_int("COOLDOWN_AFTER_WIN", 1)

# Signal settings
MAX_TICKS = get_env_int("MAX_TICKS", 200)
MOMENTUM_THRESHOLD = get_env_int("MOMENTUM_THRESHOLD", 8)
MOMENTUM_LOOKBACK = get_env_int("MOMENTUM_LOOKBACK", 15)
STRETCH_THRESHOLD = get_env_float("STRETCH_THRESHOLD", 0.5)
STRETCH_LOOKBACK = get_env_int("STRETCH_LOOKBACK", 10)
VOLATILITY_THRESHOLD = get_env_float("VOLATILITY_THRESHOLD", 0.6)
VOLATILITY_LOOKBACK = get_env_int("VOLATILITY_LOOKBACK", 6)
RANGE_LOOKBACK = get_env_int("RANGE_LOOKBACK", 30)
MIN_CONFIDENCE_THRESHOLD = get_env_float("MIN_CONFIDENCE_THRESHOLD", 60.0)

# NEW: Trade monitoring timeout (seconds)
TRADE_MONITOR_TIMEOUT = get_env_int("TRADE_MONITOR_TIMEOUT", 600)  # 10 minutes max

# Notifications
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
PRINT_EVERY_TICK = get_env_bool("PRINT_EVERY_TICK", False)
SIGNAL_COOLDOWN = get_env_int("SIGNAL_COOLDOWN", 2)

# =========================
# GLOBAL STATE
# =========================
prices = []
times = []
last_signal = None
last_signal_time = 0
previous_price = None
price_history = []
general_trend_history = deque(maxlen=300)
shutdown_event = asyncio.Event()

market_ws = None
trading_ws = None
trading_ws_lock = asyncio.Lock()

bot_status = {
    "running": False,
    "last_price": None,
    "last_signal": None,
    "signal_type": None,
    "signal_direction": None,
    "signal_confidence": 0,
    "started_at": None,
    "trend": "➡️",
    "price_change": 0,
    "balance": 0.0,
    "daily_pnl": 0.0,
    "total_trades_today": 0,
    "win_count": 0,
    "loss_count": 0,
    "concurrent_trades": 0,
    "last_trade_time": None,
    "last_trade_result": None,
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

last_signal_trade_time = None
SIGNAL_EXPIRY_SECONDS = 30

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
            bot_status["last_trade_result"] = "WIN"
        else:
            daily_stats["losses"] += 1
            daily_stats["loss"] += abs(profit_loss)
            bot_status["loss_count"] = daily_stats["losses"]
            bot_status["daily_pnl"] += profit_loss
            martingale_state["consecutive_losses"] += 1
            bot_status["cooldown_until"] = get_eat_time() + timedelta(minutes=COOLDOWN_AFTER_LOSS)
            bot_status["last_trade_result"] = "LOSS"

risk_manager = RiskManager()

# =========================
# TRADING WEBSOCKET
# =========================
async def get_trading_ws():
    global trading_ws
    async with trading_ws_lock:
        if trading_ws is None or trading_ws.closed:
            try:
                url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
                trading_ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
                
                if DERIV_API_TOKEN:
                    await trading_ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                    response = await asyncio.wait_for(trading_ws.recv(), timeout=5.0)
                    
                    await trading_ws.send(json.dumps({"balance": 1}))
                    response = await asyncio.wait_for(trading_ws.recv(), timeout=5.0)
                    if "balance" in (data := json.loads(response)):
                        bot_status["balance"] = float(data["balance"]["balance"])
                
                logger.info("🔌 Trading WebSocket connected")
            except Exception as e:
                logger.error(f"Trading WS error: {e}")
                trading_ws = None
                return None
        return trading_ws

async def close_trading_ws():
    global trading_ws
    async with trading_ws_lock:
        if trading_ws and not trading_ws.closed:
            await trading_ws.close()
            trading_ws = None

async def place_trade(contract_type, signal_type="UNKNOWN", confidence=0):
    global last_signal_trade_time
    
    can_trade, reason = risk_manager.can_trade()
    if not can_trade:
        logger.info(f"⛔ Trade blocked: {reason}")
        return None
    
    if confidence < MIN_CONFIDENCE_THRESHOLD:
        logger.info(f"⛔ Confidence too low: {confidence:.0f}% < {MIN_CONFIDENCE_THRESHOLD:.0f}%")
        return None
    
    now = time.time()
    if last_signal_trade_time and (now - last_signal_trade_time) < SIGNAL_EXPIRY_SECONDS:
        logger.info(f"⛔ Signal too recent ({int(now - last_signal_trade_time)}s ago)")
        return None
    
    stake = risk_manager.calculate_stake()
    if stake <= 0:
        logger.error("⛔ Invalid stake amount")
        return None
    
    ws = await get_trading_ws()
    if not ws:
        logger.error("⛔ No trading connection")
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
                actual_stake = float(data["buy"]["buy_price"])
                
                # Get contract details
                contract_details = data["buy"]
                
                trade_info = {
                    "id": trade_id,
                    "contract_id": contract_id,
                    "type": contract_type,
                    "signal_type": signal_type,
                    "confidence": confidence,
                    "stake": actual_stake,
                    "entry_price": bot_status["last_price"],
                    "symbol": SYMBOL,
                    "start_time": get_eat_timestamp(),
                    "status": "OPEN",
                    "expected_duration": TRADE_DURATION,
                    "expected_duration_unit": TRADE_DURATION_UNIT
                }
                
                active_trades[trade_id] = trade_info
                bot_status["concurrent_trades"] += 1
                bot_status["last_trade_time"] = get_eat_timestamp()
                last_signal_trade_time = now
                
                # Start monitoring with timeout protection
                monitor_task = asyncio.create_task(
                    monitor_trade_with_timeout(trade_id, contract_id)
                )
                
                emoji = "📈" if contract_type == "CALL" else "📉"
                msg = (f"🚀 TRADE EXECUTED\n"
                       f"Signal: {signal_type} ({confidence:.0f}%)\n"
                       f"Type: {emoji} {contract_type}\n"
                       f"Stake: ${actual_stake:.2f}\n"
                       f"Entry: {bot_status['last_price']:.3f}\n"
                       f"ID: {trade_id}\n"
                       f"Contract: {contract_id}\n"
                       f"Duration: {TRADE_DURATION}{TRADE_DURATION_UNIT}\n"
                       f"Daily P&L: ${bot_status['daily_pnl']:+.2f}")
                logger.info(msg.replace('\n', ' | '))
                send_telegram(msg)
                return trade_info
            else:
                error = data.get("error", {}).get("message", "Unknown")
                logger.error(f"❌ Buy failed: {error}")
                return None
                
    except Exception as e:
        logger.error(f"❌ Place trade error: {e}")
        return None

async def monitor_trade_with_timeout(trade_id, contract_id):
    """
    Monitor trade with multiple fallback strategies:
    1. Try WebSocket subscription
    2. Fallback to polling if WebSocket fails
    3. Force close after timeout
    """
    start_time = time.time()
    result = None
    
    # Try WebSocket monitoring first
    try:
        result = await monitor_trade_websocket(trade_id, contract_id, start_time)
    except Exception as e:
        logger.error(f"❌ WebSocket monitor failed for {trade_id}: {e}")
    
    # If WebSocket failed or timed out, try polling
    if not result:
        logger.warning(f"⚠️ Falling back to polling for {trade_id}")
        try:
            result = await monitor_trade_polling(trade_id, contract_id, start_time)
        except Exception as e:
            logger.error(f"❌ Polling failed for {trade_id}: {e}")
    
    # If still no result, force close the trade tracking
    if not result:
        logger.error(f"❌ Could not determine result for {trade_id}, forcing close")
        await force_close_trade(trade_id, "UNKNOWN")

async def monitor_trade_websocket(trade_id, contract_id, start_time):
    """Monitor via WebSocket subscription"""
    ws = None
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    try:
        ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
        
        if DERIV_API_TOKEN:
            await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
            auth_response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            logger.debug(f"Monitor auth: {auth_response[:100]}...")
        
        # Subscribe to contract updates
        subscribe_msg = {
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"📡 Subscribed to contract {contract_id} for trade {trade_id}")
        
        while time.time() - start_time < TRADE_MONITOR_TIMEOUT:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                data = json.loads(msg)
                
                if "proposal_open_contract" in data:
                    contract = data["proposal_open_contract"]
                    
                    # Log contract status for debugging
                    is_sold = contract.get("is_sold", False)
                    is_expired = contract.get("is_expired", False)
                    status = contract.get("status", "unknown")
                    
                    logger.debug(f"Trade {trade_id} status: sold={is_sold}, expired={is_expired}, status={status}")
                    
                    # Update current profit
                    current_profit = float(contract.get("profit", 0))
                    current_spot = float(contract.get("current_spot", 0))
                    
                    if trade_id in active_trades:
                        active_trades[trade_id]["current_profit"] = current_profit
                        active_trades[trade_id]["current_price"] = current_spot
                    
                    # Check if trade is complete
                    if is_sold or is_expired or status == "sold":
                        profit = float(contract.get("profit", 0))
                        exit_tick = contract.get("exit_tick") or contract.get("current_spot")
                        
                        await finalize_trade(trade_id, profit, exit_tick, "websocket")
                        return True
                        
            except asyncio.TimeoutError:
                # Check if trade duration has passed
                elapsed = time.time() - start_time
                expected_seconds = TRADE_DURATION * 60 if TRADE_DURATION_UNIT == "m" else TRADE_DURATION
                
                if elapsed > expected_seconds + 30:  # Grace period
                    logger.warning(f"⏱️ Trade {trade_id} timeout after {elapsed}s")
                    return False
                continue
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"⚠️ Monitor WS closed for {trade_id}")
                return False
            except Exception as e:
                logger.error(f"❌ Monitor error for {trade_id}: {e}")
                return False
                
    except Exception as e:
        logger.error(f"❌ WebSocket setup error for {trade_id}: {e}")
        return False
    finally:
        if ws and not ws.closed:
            await ws.close()

async def monitor_trade_polling(trade_id, contract_id, start_time):
    """Fallback: Poll for contract status via API"""
    poll_interval = 5  # seconds
    max_attempts = 120  # 10 minutes max
    
    for attempt in range(max_attempts):
        if time.time() - start_time > TRADE_MONITOR_TIMEOUT:
            break
            
        try:
            # Create new connection for each poll
            url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
            async with websockets.connect(url, ping_timeout=10) as ws:
                if DERIV_API_TOKEN:
                    await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                    await asyncio.wait_for(ws.recv(), timeout=5.0)
                
                # Request contract status (not subscribe)
                status_msg = {
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 0  # One-time request
                }
                await ws.send(json.dumps(status_msg))
                
                response = await asyncio.wait_for(ws.recv(), timeout=10.0)
                data = json.loads(response)
                
                if "proposal_open_contract" in data:
                    contract = data["proposal_open_contract"]
                    
                    if contract.get("is_sold") or contract.get("is_expired"):
                        profit = float(contract.get("profit", 0))
                        exit_tick = contract.get("exit_tick") or contract.get("current_spot")
                        
                        await finalize_trade(trade_id, profit, exit_tick, "polling")
                        return True
                    
                    # Update running profit
                    if trade_id in active_trades:
                        active_trades[trade_id]["current_profit"] = float(contract.get("profit", 0))
                        
        except Exception as e:
            logger.debug(f"Poll attempt {attempt} failed: {e}")
        
        await asyncio.sleep(poll_interval)
    
    return False

async def force_close_trade(trade_id, result_status):
    """Force close a trade when monitoring fails"""
    if trade_id not in active_trades:
        return
    
    trade = active_trades[trade_id]
    
    # Estimate result based on current price vs entry
    entry = trade.get("entry_price", 0)
    current = bot_status.get("last_price", entry)
    
    if trade.get("type") == "CALL":
        estimated_profit = -trade.get("stake", 0) * 0.1  # Assume small loss
    else:
        estimated_profit = -trade.get("stake", 0) * 0.1
    
    logger.warning(f"⚠️ Force closing {trade_id} with estimated P&L: ${estimated_profit:.2f}")
    
    trade.update({
        "status": result_status,
        "profit": estimated_profit,
        "exit_price": current,
        "exit_time": get_eat_timestamp(),
        "forced_close": True
    })
    
    trade_history.append(trade.copy())
    risk_manager.update_after_trade(estimated_profit)
    
    if trade_id in active_trades:
        del active_trades[trade_id]

async def finalize_trade(trade_id, profit, exit_tick, method):
    """Finalize trade and update statistics"""
    if trade_id not in active_trades:
        logger.warning(f"⚠️ Trade {trade_id} not found in active_trades")
        return
    
    status = "WIN" if profit > 0 else "LOSS"
    trade = active_trades[trade_id]
    
    trade.update({
        "status": status,
        "profit": profit,
        "exit_price": float(exit_tick) if exit_tick else trade.get("entry_price"),
        "exit_time": get_eat_timestamp(),
        "monitor_method": method
    })
    
    trade_history.append(trade.copy())
    risk_manager.update_after_trade(profit)
    
    emoji = "✅" if profit > 0 else "❌"
    signal_type = trade.get("signal_type", "UNKNOWN")
    entry = trade.get("entry_price", 0)
    exit_p = trade.get("exit_price", 0)
    
    msg = (f"{emoji} TRADE CLOSED ({method})\n"
           f"Signal: {signal_type}\n"
           f"Result: {status}\n"
           f"P&L: ${profit:+.2f}\n"
           f"Entry: {entry:.3f} → Exit: {exit_p:.3f}\n"
           f"ID: {trade_id}\n"
           f"Daily P&L: ${bot_status['daily_pnl']:+.2f}\n"
           f"Win Rate: {bot_status['win_count']}/{bot_status['total_trades_today']}")
    logger.info(msg.replace('\n', ' | '))
    send_telegram(msg)
    
    del active_trades[trade_id]

# =========================
# SIGNAL ANALYSIS (unchanged from previous)
# =========================
def analyze_signals_directional(prices_list):
    if len(prices_list) < 30:
        return "NONE", None, 0, {"error": "Insufficient data"}
    
    current = prices_list[-1]
    signals_found = []
    
    momentum_signal = check_momentum_directional(prices_list)
    if momentum_signal:
        signals_found.append(momentum_signal)
    
    stretch_signal = check_stretch_directional(prices_list)
    if stretch_signal:
        signals_found.append(stretch_signal)
    
    volatility_signal = check_volatility_directional(prices_list)
    if volatility_signal:
        signals_found.append(volatility_signal)
    
    range_signal = check_range_breakout(prices_list)
    if range_signal:
        signals_found.append(range_signal)
    
    if not signals_found:
        return "NONE", None, 0, {"status": "No signals"}
    
    signals_found.sort(key=lambda x: x["confidence"], reverse=True)
    best_signal = signals_found[0]
    
    if len(signals_found) > 1:
        directions = [s["direction"] for s in signals_found]
        if len(set(directions)) == 1:
            best_signal["confidence"] = min(100, best_signal["confidence"] + 10)
            best_signal["debug"]["agreement"] = f"{len(signals_found)} signals agree"
    
    return best_signal["type"], best_signal["direction"], best_signal["confidence"], best_signal["debug"]

def check_momentum_directional(prices_list):
    if len(prices_list) < MOMENTUM_LOOKBACK + 1:
        return None
    
    recent = prices_list[-MOMENTUM_LOOKBACK:]
    moves = [recent[i] - recent[i-1] for i in range(1, len(recent))]
    
    up_moves = sum(1 for m in moves if m > 0)
    down_moves = sum(1 for m in moves if m < 0)
    total_moves = len(moves)
    
    if up_moves >= MOMENTUM_THRESHOLD:
        confidence = (up_moves / total_moves) * 100
        return {
            "type": "MOMENTUM",
            "direction": "CALL",
            "confidence": confidence,
            "debug": {"up": up_moves, "down": down_moves, "total": total_moves}
        }
    
    if down_moves >= MOMENTUM_THRESHOLD:
        confidence = (down_moves / total_moves) * 100
        return {
            "type": "MOMENTUM",
            "direction": "PUT",
            "confidence": confidence,
            "debug": {"up": up_moves, "down": down_moves, "total": total_moves}
        }
    
    return None

def check_stretch_directional(prices_list):
    if len(prices_list) < STRETCH_LOOKBACK:
        return None
    
    recent = prices_list[-STRETCH_LOOKBACK:]
    avg = statistics.mean(recent)
    current = prices_list[-1]
    
    stretch = current - avg
    stretch_pct = (abs(stretch) / avg) * 100 if avg != 0 else 0
    
    if stretch >= STRETCH_THRESHOLD:
        confidence = min(stretch_pct * 10, 100)
        return {
            "type": "STRETCH",
            "direction": "CALL",
            "confidence": confidence,
            "debug": {"stretch": round(stretch, 3), "avg": round(avg, 3), "pct": round(stretch_pct, 2)}
        }
    
    if stretch <= -STRETCH_THRESHOLD:
        confidence = min(stretch_pct * 10, 100)
        return {
            "type": "STRETCH",
            "direction": "PUT",
            "confidence": confidence,
            "debug": {"stretch": round(stretch, 3), "avg": round(avg, 3), "pct": round(stretch_pct, 2)}
        }
    
    return None

def check_volatility_directional(prices_list):
    if len(prices_list) < VOLATILITY_LOOKBACK + 1:
        return None
    
    recent = prices_list[-VOLATILITY_LOOKBACK:]
    changes = [abs(recent[i] - recent[i-1]) for i in range(1, len(recent))]
    max_change = max(changes) if changes else 0
    
    if max_change >= VOLATILITY_THRESHOLD:
        biggest_move_idx = changes.index(max_change)
        direction = "CALL" if (recent[biggest_move_idx + 1] > recent[biggest_move_idx]) else "PUT"
        
        confidence = min(max_change * 100, 100)
        return {
            "type": "VOLATILITY",
            "direction": direction,
            "confidence": confidence,
            "debug": {"spike": round(max_change, 3), "direction": direction}
        }
    
    return None

def check_range_breakout(prices_list):
    if len(prices_list) < RANGE_LOOKBACK:
        return None
    
    recent = prices_list[-RANGE_LOOKBACK:]
    current = prices_list[-1]
    
    range_high = max(recent)
    range_low = min(recent)
    range_size = range_high - range_low
    
    if range_size < 0.5:
        return None
    
    if current > range_high * 0.999:
        confidence = 70 + (current - range_high) * 100
        return {
            "type": "RANGE_BREAKOUT",
            "direction": "CALL",
            "confidence": min(confidence, 95),
            "debug": {"high": round(range_high, 3), "low": round(range_low, 3), "range": round(range_size, 3)}
        }
    
    if current < range_low * 1.001:
        confidence = 70 + (range_low - current) * 100
        return {
            "type": "RANGE_BREAKOUT",
            "direction": "PUT",
            "confidence": min(confidence, 95),
            "debug": {"high": round(range_high, 3), "low": round(range_low, 3), "range": round(range_size, 3)}
        }
    
    return None

# =========================
# MAIN LOOP
# =========================
async def stream_ticks():
    global prices, times, price_history, previous_price, general_trend_history, market_ws
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    bot_status["running"] = True
    bot_status["started_at"] = get_eat_timestamp()
    
    logger.info("=" * 60)
    logger.info(f"🚀 BOT STARTED | Symbol: {SYMBOL} | Trading: {'ON' if AUTO_TRADE_ENABLED else 'OFF'}")
    logger.info(f"Min Confidence: {MIN_CONFIDENCE_THRESHOLD}% | Timeout: {TRADE_MONITOR_TIMEOUT}s")
    logger.info("=" * 60)

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                market_ws = ws
                logger.info("✅ Market WebSocket connected")
                
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))

                while not shutdown_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(msg)

                        if "error" in data:
                            logger.error(f"❌ Deriv error: {data['error']}")
                            break

                        if "tick" in data:
                            tick = data["tick"]
                            quote = float(tick["quote"])
                            
                            trend, change = calculate_trend(quote, previous_price)
                            previous_price = quote
                            
                            general_trend_history.append(quote)
                            
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

                            signal_type, direction, confidence, debug = analyze_signals_directional(prices)
                            
                            bot_status["last_price"] = quote
                            bot_status["last_signal"] = f"{signal_type} {direction}" if direction else "NONE"
                            bot_status["signal_type"] = signal_type
                            bot_status["signal_direction"] = direction
                            bot_status["signal_confidence"] = confidence
                            bot_status["trend"] = trend
                            bot_status["price_change"] = round(change, 3)

                            if signal_type != "NONE" and direction and AUTO_TRADE_ENABLED:
                                can_trade, reason = risk_manager.can_trade()
                                if can_trade:
                                    await place_trade(direction, signal_type, confidence)
                                elif "Cooldown" not in reason:
                                    logger.info(f"⏸️ {reason}")

                            if should_log(signal_type, direction, confidence):
                                log_trade_status(signal_type, direction, confidence, debug, trend, change, quote)

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"❌ Message error: {e}")
                        break

        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            market_ws = None
            if not shutdown_event.is_set():
                logger.info("🔄 Reconnecting in 3s...")
                await asyncio.sleep(3)

    bot_status["running"] = False
    await close_trading_ws()
    logger.info("🛑 Bot stopped")

def calculate_trend(current, previous):
    if previous is None:
        return "➡️", 0
    change = current - previous
    return ("⬆️", change) if change > 0 else ("⬇️", change) if change < 0 else ("➡️", 0)

def should_log(signal_type, direction, confidence):
    global last_signal, last_signal_time
    now = time.time()
    
    if signal_type != "NONE" and confidence >= MIN_CONFIDENCE_THRESHOLD:
        return True
    if PRINT_EVERY_TICK:
        return True
    current_signal = f"{signal_type}_{direction}"
    if current_signal != last_signal:
        last_signal = current_signal
        last_signal_time = now
        return True
    if now - last_signal_time >= SIGNAL_COOLDOWN:
        last_signal_time = now
        return True
    return False

def log_trade_status(signal_type, direction, confidence, debug, trend, change, price):
    pnl_str = f"${bot_status['daily_pnl']:+.2f}" if AUTO_TRADE_ENABLED else "N/A"
    trades_str = f"{bot_status['total_trades_today']}/{MAX_TRADES_PER_DAY}"
    
    if signal_type != "NONE":
        emoji = "📈" if direction == "CALL" else "📉"
        signal_str = f"{emoji} {signal_type} {direction} ({confidence:.0f}%)"
        if "up" in debug and "down" in debug:
            signal_str += f" [↑{debug['up']}↓{debug['down']}]"
        elif "stretch" in debug:
            signal_str += f" [str:{debug['stretch']}]"
        elif "spike" in debug:
            signal_str += f" [spike:{debug['spike']}]"
        elif "range" in debug:
            signal_str += f" [rng:{debug['range']}]"
    else:
        signal_str = "WAITING..."
    
    cooldown = ""
    if bot_status["cooldown_until"] and get_eat_time() < bot_status["cooldown_until"]:
        remaining = int((bot_status["cooldown_until"] - get_eat_time()).total_seconds())
        cooldown = f" [COOLDOWN:{remaining}s]"
    
    concurrent = f" [OPEN:{bot_status['concurrent_trades']}]" if bot_status["concurrent_trades"] > 0 else ""
    
    logger.info(f"[{get_eat_clock()}] {price:.3f} {trend} | {signal_str} | P&L:{pnl_str} | Trades:{trades_str}{cooldown}{concurrent}")

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
    
    active_trades_list = []
    for tid, trade in active_trades.items():
        active_trades_list.append({
            "id": tid,
            "type": trade.get("type"),
            "signal": trade.get("signal_type"),
            "stake": trade.get("stake"),
            "entry": trade.get("entry_price"),
            "current_pnl": trade.get("current_profit", 0),
            "duration": trade.get("start_time")
        })
    
    return web.json_response({
        "status": "healthy",
        "running": bot_status["running"],
        "timestamp": get_eat_timestamp(),
        "symbol": SYMBOL,
        "trading_enabled": AUTO_TRADE_ENABLED,
        "last_price": bot_status.get("last_price"),
        "last_signal": bot_status.get("last_signal"),
        "signal_type": bot_status.get("signal_type"),
        "signal_direction": bot_status.get("signal_direction"),
        "signal_confidence": bot_status.get("signal_confidence"),
        "price_history": price_history[-50:],
        "balance": round(bot_status.get("balance", 0), 2),
        "daily_pnl": round(bot_status.get("daily_pnl", 0), 2),
        "total_trades": bot_status.get("total_trades_today", 0),
        "win_count": bot_status.get("win_count", 0),
        "loss_count": bot_status.get("loss_count", 0),
        "win_rate": round(win_rate, 1),
        "concurrent_trades": bot_status.get("concurrent_trades", 0),
        "active_trades": active_trades_list,
        "last_trade_result": bot_status.get("last_trade_result"),
        "daily_limit_hit": bot_status.get("daily_limit_hit", False),
        "cooldown_remaining": cooldown_remaining,
        "next_stake": risk_manager.calculate_stake() if AUTO_TRADE_ENABLED else 0,
        "martingale_step": martingale_state["consecutive_losses"] if MARTINGALE_ENABLED else 0
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
        logger.warning("⚠️ AUTO_TRADE_ENABLED but no DERIV_API_TOKEN - disabling trading")
        AUTO_TRADE_ENABLED = False
        bot_status["trading_enabled"] = False
    
    asyncio.run(main())
