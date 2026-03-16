from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketData:

    symbol: str = ""
    price: float = 0
    signal: str = "WAIT"
    trend: str = "NEUTRAL"
    score: int = 0

    rsi: float = 50
    ema_fast: float = 0
    ema_slow: float = 0

    timestamp: datetime = field(default_factory=datetime.now)

    price_history: list = field(default_factory=list)
