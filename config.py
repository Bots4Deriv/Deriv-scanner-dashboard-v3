from typing import List

class Config:
    # Trading symbols to monitor
    SYMBOLS: List[str] = [
        "R_10",      # Volatility 10 Index
        "R_25",      # Volatility 25 Index  
        "R_50",      # Volatility 50 Index
        "R_75",      # Volatility 75 Index
        "R_100",     # Volatility 100 Index
        "1HZ10V",    # HF Volatility 10
        "1HZ25V",    # HF Volatility 25
        "1HZ50V",    # HF Volatility 50
        "1HZ75V",    # HF Volatility 75
        "1HZ100V",   # HF Volatility 100
        "BOOM1000",  # Boom 1000 Index
        "CRASH1000", # Crash 1000 Index
        "STEPINDEX", # Step Index
        "JUMP10",    # Jump 10 Index
    ]
    
    # WebSocket settings
    WS_HEARTBEAT_INTERVAL = 30  # seconds
    
    # Update intervals
    MARKET_UPDATE_INTERVAL = 1.0  # seconds
    
    # Deriv API settings (if using Deriv)
    DERIV_APP_ID = "YOUR_APP_ID"
    DERIV_API_URL = "wss://ws.binaryws.com/websockets/v3"
