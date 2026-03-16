from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from config import Config
from models import market_state, connection_status

def add_routes(app: FastAPI):
    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        rows = ""
        for symbol in Config.SYMBOLS:
            data = market_state[symbol]
            colors = {
                "RANGE": "#ff9800",
                "COMPRESSION": "#9c27b0",
                "UPTREND": "#4caf50",
                "DOWNTREND": "#f44336",
                "NEUTRAL": "#757575",
                "SCANNING": "#2196f3",
                "INSUFFICIENT_DATA": "#607d8b"
            }
            color = colors.get(data.signal, "white")
            status = "🟢" if connection_status.get(symbol) else "🔴"
            rows += f"""
            <tr style="color:{color}">
                <td>{status} {symbol}</td>
                <td>{data.price}</td>
                <td>{data.signal}</td>
                <td>{data.trend}</td>
                <td>{data.score}</td>
                <td>{data.atr}</td>
                <td>{data.rsi}</td>
                <td>{data.ema_fast}/{data.ema_slow}</td>
            </tr>
            """
        html = f"""
        <html>
            <head><title>Deriv Elite Scanner V3</title></head>
            <body>
                <h2>Deriv Elite Scanner V3 Dashboard</h2>
                <table border="1" cellpadding="5">
                    <tr>
                        <th>Symbol</th><th>Price</th><th>Signal</th><th>Trend</th>
                        <th>Score</th><th>ATR</th><th>RSI</th><th>EMA9/21</th>
                    </tr>
                    {rows}
                </table>
            </body>
        </html>
        """
        return HTMLResponse(html)
