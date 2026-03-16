from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict

from config import Config

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

# Global state
market_state: Dict[str, MarketData] = {s: MarketData(symbol=s) for s in Config.SYMBOLS}
last_signals: Dict[str, str] = {}
connection_status: Dict[str, bool] = {s: False for s in Config.SYMBOLS}
