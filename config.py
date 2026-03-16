import os
import logging

class Config:
    API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
    APP_ID = os.getenv("DERIV_APP_ID", "1089")
    TIMEFRAME = int(os.getenv("TIMEFRAME", "60"))
    CANDLE_COUNT = int(os.getenv("CANDLE_COUNT", "80"))
    MAX_HISTORY = int(os.getenv("MAX_HISTORY", "150"))
    SCAN_INTERVAL = float(os.getenv("SCAN_INTERVAL", "1.0"))
    SYMBOLS = ["R_10", "R_25", "R_50", "R_75", "R_100"]

    @classmethod
    def validate(cls):
        if not cls.API_TOKEN:
            logging.warning("DERIV_API_TOKEN not set!")

Config.validate()
