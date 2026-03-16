import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import Config
from analyzer import SignalAnalyzer
from websocket_client import DerivWebSocket
from dashboard import add_routes
from models import market_state, last_signals

app = FastAPI(title="Deriv Elite Scanner V3", version="3.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add dashboard route
add_routes(app)

analyzer = SignalAnalyzer()

@app.on_event("startup")
async def startup_event():
    tasks = []
    for symbol in Config.SYMBOLS:
        client = DerivWebSocket(symbol, market_state, last_signals, analyzer)
        tasks.append(asyncio.create_task(client.connect()))
    app.state.tasks = tasks
    print("🚀 Deriv Elite Scanner V3 started...")

@app.on_event("shutdown")
async def shutdown_event():
    for task in getattr(app.state, "tasks", []):
        task.cancel()
    print("⛔ Scanner stopped.")
