from datetime import datetime

connected_clients = []


async def broadcast_update(market_state):

    if not connected_clients:
        return

    data = {
        "timestamp": datetime.now().isoformat(),
        "symbols": {}
    }

    for symbol, state in market_state.items():

        data["symbols"][symbol] = {
            "price": round(state.price, 5),
            "signal": state.signal,
            "trend": state.trend,
            "score": state.score,
            "rsi": round(state.rsi, 2),
            "ema_fast": round(state.ema_fast, 2),
            "ema_slow": round(state.ema_slow, 2)
        }

    dead = []

    for client in connected_clients:

        try:
            await client.send_json(data)

        except:
            dead.append(client)

    for d in dead:
        connected_clients.remove(d)
