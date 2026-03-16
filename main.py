import asyncio
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from deriv_api import start_deriv_stream

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_deriv_stream())


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """
    <html>
    <head>
        <title>Deriv Volatility Scanner</title>
        <style>
            body{
                background:#0f172a;
                color:white;
                font-family:Arial;
                text-align:center;
                padding-top:80px;
            }
            h1{
                color:#22c55e;
            }
        </style>
    </head>
    <body>

        <h1>Deriv Volatility Scanner Running ✅</h1>

        <p>Connected to Deriv tick stream</p>
        <p>Symbols: R_25 • R_50 • R_75 • R_100</p>

    </body>
    </html>
    """
