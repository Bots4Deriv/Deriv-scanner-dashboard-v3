from typing import List, Dict, Any
from indicators import Indicators
from models import MarketData
from datetime import datetime

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
        score = 0
        if atr_val < self.thresholds["atr_medium"]:
            score += 30
        if atr_val < self.thresholds["atr_low"]:
            score += 20
        if self.thresholds["rsi_neutral_low"] < rsi_val < self.thresholds["rsi_neutral_high"]:
            score += 20
        if range_val < self.thresholds["range_threshold"]:
            score += 30
        return min(score, 100)

    def analyze(self, symbol: str, closes: List[float], highs: List[float], lows: List[float], market_state: Dict[str, MarketData], last_signals: Dict[str, str]) -> Dict[str, Any]:
        if len(closes) < 25:
            return {"signal": "INSUFFICIENT_DATA"}
        atr_val = Indicators.atr(highs, lows, closes) or 0
        rsi_val = Indicators.rsi(closes) or 50
        ema_fast = Indicators.ema(closes, 9) or closes[-1]
        ema_slow = Indicators.ema(closes, 21) or closes[-1]
        recent_high = max(highs[-5:])
        recent_low = min(lows[-5:])
        range_val = recent_high - recent_low
        price = closes[-1]
        score = self.calculate_score(atr_val, rsi_val, range_val)

        signal_type = "NEUTRAL"
        trend = "NEUTRAL"
        if score >= self.thresholds["score_threshold"]:
            signal_type = "RANGE"
            trend = "SIDEWAYS"
        elif atr_val < self.thresholds["compression_atr"] and range_val < self.thresholds["compression_range"]:
            signal_type = "COMPRESSION"
            trend = "CONSOLIDATION"
        elif ema_fast > ema_slow and rsi_val > self.thresholds["rsi_trend_high"]:
            signal_type = "UPTREND"
            trend = "BULLISH"
        elif ema_fast < ema_slow and rsi_val < self.thresholds["rsi_trend_low"]:
            signal_type = "DOWNTREND"
            trend = "BEARISH"

        # Print signal to console
        if last_signals.get(symbol) != signal_type:
            last_signals[symbol] = signal_type
            print(f"[{symbol}] Signal: {signal_type} | Price: {price} | ATR: {atr_val:.4f} | RSI: {rsi_val:.2f}")

        # Update market state
        md = market_state[symbol]
        md.price = price
        md.signal = signal_type
        md.trend = trend
        md.score = score
        md.atr = round(atr_val, 4)
        md.rsi = round(rsi_val, 2)
        md.ema_fast = round(ema_fast, 2)
        md.ema_slow = round(ema_slow, 2)
        md.last_update = datetime.now()

        return {"price": price, "signal": signal_type, "trend": trend, "score": score}
