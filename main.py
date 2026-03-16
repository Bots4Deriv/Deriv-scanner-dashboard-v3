import asyncio

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

from models import MarketData
from config import Config
from deriv_api import DerivAPI
from broadcaster import connected_clients


app = FastAPI()


market_state = {
    symbol: MarketData(symbol=symbol)
    for symbol in Config.SYMBOLS
}


@app.get("/", response_class=HTMLResponse)
async def dashboard():

    return """
    <html>
    <body style="background:#111;color:white;font-family:sans-serif">

    <h2>Deriv Scanner</h2>

    <table border="1" cellpadding="10">
    <thead>
    <tr>
    <th>Symbol</th>
    <th>Price</th>
    <th>Signal</th>
    <th>Score</th>
    </tr>
    </thead>
    <tbody id="data"></tbody>
    </table>

    <script>

    let ws = new WebSocket(`ws://${location.host}/ws`);

    ws.onmessage = e => {

        let data = JSON.parse(e.data)

        let rows = ""

        for (let s in data.symbols){

            let d = data.symbols[s]

            rows += `<tr>
            <td>${s}</td>
            <td>${d.price}</td>
            <td>${d.signal}</td>
            <td>${d.score}</td>
            </tr>`
        }

        document.getElementById("data").innerHTML = rows
    }

    </script>

    </body>
    </html>
    """


@app.websocket("/ws")
async def websocket(ws: WebSocket):

    await ws.accept()

    connected_clients.append(ws)

    try:
        while True:
            await ws.receive_text()

    except:
        connected_clients.remove(ws)


@app.on_event("startup")
async def start():

    api = DerivAPI(market_state)

    asyncio.create_task(api.connect())
