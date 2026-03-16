from typing import List, Optional
import numpy as np

class Indicators:
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
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
        if len(data) < period:
            return None
        multiplier = 2.0 / (period + 1)
        ema_val = np.mean(data[:period])
        for price in data[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return float(ema_val)

    @staticmethod
    def sma(data: List[float], period: int) -> Optional[float]:
        if len(data) < period:
            return None
        return float(np.mean(data[-period:]))
