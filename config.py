import os

class Config:

    DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")
    DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3"

    SYMBOLS = os.getenv(
        "SYMBOLS",
        "R_25,R_50,R_75,R_100"
    ).split(",")

    UPDATE_INTERVAL = float(os.getenv("UPDATE_INTERVAL", "0.1"))
    HISTORY_LENGTH = int(os.getenv("HISTORY_LENGTH", "100"))
