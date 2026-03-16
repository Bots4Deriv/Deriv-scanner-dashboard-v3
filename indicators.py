import numpy as np


class TechnicalIndicators:

    @staticmethod
    def rsi(prices, period=14):

        if len(prices) < period + 1:
            return 50

        deltas = np.diff(prices)

        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])

        if avg_loss == 0:
            return 100

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(prices, period):

        if len(prices) < period:
            return prices[-1]

        multiplier = 2 / (period + 1)

        ema = np.mean(prices[:period])

        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema

        return ema

    @staticmethod
    def tick_pressure(prices):

        if len(prices) < 10:
            return "NEUTRAL"

        up = 0
        down = 0

        for i in range(-10, -1):

            if prices[i] > prices[i - 1]:
                up += 1
            else:
                down += 1

        if up >= 7:
            return "BUY_PRESSURE"

        if down >= 7:
            return "SELL_PRESSURE"

        return "NEUTRAL"

    @staticmethod
    def signal(prices, ema_fast, ema_slow, rsi):

        if len(prices) < 21:
            return "WAIT", "NEUTRAL", 0

        momentum = prices[-1] - prices[-5]

        if ema_fast > ema_slow and rsi > 55 and momentum > 0:

            score = 70 + min(int(abs(momentum) * 10000), 30)
            return "CALL", "BULLISH", min(score, 100)

        elif ema_fast < ema_slow and rsi < 45 and momentum < 0:

            score = 70 + min(int(abs(momentum) * 10000), 30)
            return "PUT", "BEARISH", min(score, 100)

        else:
            return "WAIT", "NEUTRAL", 30
