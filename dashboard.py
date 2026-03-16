from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import json
from datetime import datetime
from config import Config
from models import market_state, connection_status

# Store connected WebSocket clients
connected_clients = []

async def broadcast_update():
    """Broadcast updates to all connected WebSocket clients"""
    if not connected_clients:
        return
    
    # Prepare data payload
    data = {
        "timestamp": datetime.now().isoformat(),
        "symbols": {}
    }
    
    for symbol in Config.SYMBOLS:
        if symbol in market_state:
            state = market_state[symbol]
            data["symbols"][symbol] = {
                "price": float(state.price) if hasattr(state, 'price') else 0.0,
                "signal": state.signal,
                "trend": state.trend,
                "score": int(state.score) if hasattr(state, 'score') else 0,
                "atr": float(state.atr) if hasattr(state, 'atr') else 0.0,
                "rsi": float(state.rsi) if hasattr(state, 'rsi') else 0.0,
                "ema_fast": float(state.ema_fast) if hasattr(state, 'ema_fast') else 0.0,
                "ema_slow": float(state.ema_slow) if hasattr(state, 'ema_slow') else 0.0,
                "connected": connection_status.get(symbol, False)
            }
    
    # Broadcast to all clients
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except:
            disconnected.append(client)
    
    # Clean up disconnected clients
    for client in disconnected:
        if client in connected_clients:
            connected_clients.remove(client)

def add_routes(app: FastAPI):
    
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        connected_clients.append(websocket)
        print(f"Client connected. Total clients: {len(connected_clients)}")
        
        try:
            while True:
                # Keep connection alive and handle client pings
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        except:
            pass
        finally:
            if websocket in connected_clients:
                connected_clients.remove(websocket)
            print(f"Client disconnected. Total clients: {len(connected_clients)}")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Deriv Elite Scanner V3 | Real-Time Dashboard</title>
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }
                
                :root {
                    --bg-primary: #0a0e1a;
                    --bg-secondary: #151b2d;
                    --bg-tertiary: #1e2740;
                    --text-primary: #ffffff;
                    --text-secondary: #8b9dc3;
                    --accent-green: #00d084;
                    --accent-red: #ff4757;
                    --accent-blue: #3742fa;
                    --accent-yellow: #ffa502;
                    --border-color: #2d3748;
                    --success: #00d084;
                    --warning: #ffa502;
                    --danger: #ff4757;
                }
                
                body {
                    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
                    background: var(--bg-primary);
                    color: var(--text-primary);
                    min-height: 100vh;
                    overflow-x: hidden;
                }
                
                /* Animated background grid */
                .bg-grid {
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background-image: 
                        linear-gradient(rgba(55, 66, 250, 0.03) 1px, transparent 1px),
                        linear-gradient(90deg, rgba(55, 66, 250, 0.03) 1px, transparent 1px);
                    background-size: 50px 50px;
                    pointer-events: none;
                    z-index: 0;
                }
                
                .dashboard-container {
                    position: relative;
                    z-index: 1;
                    max-width: 1600px;
                    margin: 0 auto;
                    padding: 20px;
                }
                
                /* Header */
                header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 30px;
                    padding: 20px 30px;
                    background: linear-gradient(135deg, rgba(21, 27, 45, 0.9), rgba(30, 39, 64, 0.9));
                    backdrop-filter: blur(20px);
                    border-radius: 16px;
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
                }
                
                .brand {
                    display: flex;
                    align-items: center;
                    gap: 15px;
                }
                
                .logo {
                    width: 50px;
                    height: 50px;
                    background: linear-gradient(135deg, #3742fa, #2ed573);
                    border-radius: 12px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 24px;
                    box-shadow: 0 0 30px rgba(55, 66, 250, 0.5);
                    animation: glow 3s ease-in-out infinite alternate;
                }
                
                @keyframes glow {
                    from { box-shadow: 0 0 20px rgba(55, 66, 250, 0.5); }
                    to { box-shadow: 0 0 40px rgba(46, 213, 115, 0.5); }
                }
                
                .brand-text h1 {
                    font-size: 24px;
                    font-weight: 800;
                    background: linear-gradient(135deg, #fff, #8b9dc3);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    letter-spacing: -0.5px;
                }
                
                .brand-text span {
                    font-size: 12px;
                    color: var(--text-secondary);
                    text-transform: uppercase;
                    letter-spacing: 2px;
                }
                
                .connection-status {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    padding: 10px 20px;
                    background: rgba(0, 208, 132, 0.1);
                    border-radius: 30px;
                    border: 1px solid rgba(0, 208, 132, 0.3);
                }
                
                .connection-status.disconnected {
                    background: rgba(255, 71, 87, 0.1);
                    border-color: rgba(255, 71, 87, 0.3);
                }
                
                .pulse-dot {
                    width: 8px;
                    height: 8px;
                    background: var(--success);
                    border-radius: 50%;
                    position: relative;
                }
                
                .pulse-dot::after {
                    content: '';
                    position: absolute;
                    top: 50%;
                    left: 50%;
                    transform: translate(-50%, -50%);
                    width: 100%;
                    height: 100%;
                    background: var(--success);
                    border-radius: 50%;
                    animation: pulse-ring 2s cubic-bezier(0.215, 0.61, 0.355, 1) infinite;
                }
                
                .connection-status.disconnected .pulse-dot,
                .connection-status.disconnected .pulse-dot::after {
                    background: var(--danger);
                }
                
                @keyframes pulse-ring {
                    0% { transform: translate(-50%, -50%) scale(1); opacity: 1; }
                    100% { transform: translate(-50%, -50%) scale(4); opacity: 0; }
                }
                
                .status-text {
                    font-size: 13px;
                    font-weight: 600;
                    color: var(--success);
                }
                
                .connection-status.disconnected .status-text {
                    color: var(--danger);
                }
                
                /* Stats Grid */
                .stats-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }
                
                .stat-card {
                    background: linear-gradient(135deg, rgba(21, 27, 45, 0.8), rgba(30, 39, 64, 0.8));
                    border-radius: 16px;
                    padding: 24px;
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    position: relative;
                    overflow: hidden;
                    transition: transform 0.3s, box-shadow 0.3s;
                }
                
                .stat-card::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 3px;
                    background: linear-gradient(90deg, #3742fa, #2ed573);
                    transform: scaleX(0);
                    transition: transform 0.3s;
                }
                
                .stat-card:hover {
                    transform: translateY(-5px);
                    box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
                }
                
                .stat-card:hover::before {
                    transform: scaleX(1);
                }
                
                .stat-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 12px;
                }
                
                .stat-label {
                    font-size: 13px;
                    color: var(--text-secondary);
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }
                
                .stat-icon {
                    font-size: 20px;
                    opacity: 0.5;
                }
                
                .stat-value {
                    font-size: 32px;
                    font-weight: 800;
                    color: var(--text-primary);
                    font-family: 'Courier New', monospace;
                }
                
                .stat-change {
                    font-size: 12px;
                    margin-top: 8px;
                    color: var(--success);
                }
                
                /* Table Container */
                .table-wrapper {
                    background: linear-gradient(135deg, rgba(21, 27, 45, 0.8), rgba(30, 39, 64, 0.8));
                    border-radius: 20px;
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    overflow: hidden;
                    box-shadow: 0 25px 50px rgba(0, 0, 0, 0.5);
                }
                
                .table-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 20px 30px;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                }
                
                .table-title {
                    font-size: 18px;
                    font-weight: 700;
                }
                
                .live-indicator {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    font-size: 12px;
                    color: var(--text-secondary);
                }
                
                .live-dot {
                    width: 6px;
                    height: 6px;
                    background: var(--success);
                    border-radius: 50%;
                    animation: blink 1s infinite;
                }
                
                @keyframes blink {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.3; }
                }
                
                table {
                    width: 100%;
                    border-collapse: collapse;
                }
                
                thead {
                    background: rgba(10, 14, 26, 0.8);
                }
                
                th {
                    padding: 16px 24px;
                    text-align: left;
                    font-size: 11px;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    color: var(--text-secondary);
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                }
                
                td {
                    padding: 20px 24px;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.03);
                    font-size: 14px;
                    transition: background 0.2s;
                }
                
                tr:hover td {
                    background: rgba(255, 255, 255, 0.02);
                }
                
                /* Symbol Cell */
                .symbol-info {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }
                
                .conn-indicator {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    background: var(--success);
                    box-shadow: 0 0 10px var(--success);
                }
                
                .conn-indicator.offline {
                    background: var(--danger);
                    box-shadow: 0 0 10px var(--danger);
                }
                
                .symbol-name {
                    font-weight: 700;
                    font-size: 15px;
                    color: var(--text-primary);
                }
                
                /* Price Cell */
                .price-wrapper {
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                }
                
                .price-main {
                    font-family: 'Courier New', monospace;
                    font-size: 16px;
                    font-weight: 700;
                    color: var(--text-primary);
                }
                
                .price-change {
                    font-size: 11px;
                    color: var(--success);
                    font-weight: 600;
                }
                
                .price-change.down {
                    color: var(--danger);
                }
                
                /* Signal Badge */
                .signal-badge {
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                    padding: 8px 14px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    border: 1px solid;
                    transition: all 0.3s;
                }
                
                .signal-uptrend {
                    background: rgba(0, 208, 132, 0.1);
                    color: var(--success);
                    border-color: rgba(0, 208, 132, 0.3);
                    box-shadow: 0 0 20px rgba(0, 208, 132, 0.1);
                }
                
                .signal-downtrend {
                    background: rgba(255, 71, 87, 0.1);
                    color: var(--danger);
                    border-color: rgba(255, 71, 87, 0.3);
                    box-shadow: 0 0 20px rgba(255, 71, 87, 0.1);
                }
                
                .signal-range {
                    background: rgba(255, 165, 2, 0.1);
                    color: var(--warning);
                    border-color: rgba(255, 165, 2, 0.3);
                }
                
                .signal-compression {
                    background: rgba(139, 92, 246, 0.1);
                    color: #8b5cf6;
                    border-color: rgba(139, 92, 246, 0.3);
                }
                
                .signal-scanning {
                    background: rgba(55, 66, 250, 0.1);
                    color: var(--accent-blue);
                    border-color: rgba(55, 66, 250, 0.3);
                }
                
                /* Score Bar */
                .score-wrapper {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }
                
                .score-bar {
                    width: 80px;
                    height: 6px;
                    background: rgba(255, 255, 255, 0.1);
                    border-radius: 3px;
                    overflow: hidden;
                    position: relative;
                }
                
                .score-fill {
                    height: 100%;
                    border-radius: 3px;
                    transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
                    position: relative;
                    overflow: hidden;
                }
                
                .score-fill::after {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
                    animation: shimmer 2s infinite;
                }
                
                @keyframes shimmer {
                    0% { transform: translateX(-100%); }
                    100% { transform: translateX(100%); }
                }
                
                .score-text {
                    font-weight: 700;
                    font-size: 13px;
                    min-width: 30px;
                }
                
                /* Metrics */
                .metric {
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    color: var(--text-secondary);
                }
                
                .rsi-value {
                    display: inline-block;
                    padding: 4px 10px;
                    border-radius: 6px;
                    font-weight: 700;
                    font-size: 13px;
                }
                
                .rsi-high {
                    background: rgba(255, 71, 87, 0.2);
                    color: var(--danger);
                }
                
                .rsi-low {
                    background: rgba(0, 208, 132, 0.2);
                    color: var(--success);
                }
                
                .rsi-normal {
                    background: rgba(255, 255, 255, 0.1);
                    color: var(--text-secondary);
                }
                
                .ema-pair {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                }
                
                .ema-fast {
                    color: #60a5fa;
                    font-weight: 700;
                }
                
                .ema-slow {
                    color: #f472b6;
                    font-weight: 700;
                }
                
                .ema-arrow {
                    color: var(--text-secondary);
                    font-size: 11px;
                }
                
                /* Animations */
                @keyframes flash-green {
                    0% { background: rgba(0, 208, 132, 0.3); }
                    100% { background: transparent; }
                }
                
                @keyframes flash-red {
                    0% { background: rgba(255, 71, 87, 0.3); }
                    100% { background: transparent; }
                }
                
                .flash-up {
                    animation: flash-green 0.5s ease;
                }
                
                .flash-down {
                    animation: flash-red 0.5s ease;
                }
                
                /* Footer */
                .footer {
                    margin-top: 30px;
                    text-align: center;
                    color: var(--text-secondary);
                    font-size: 12px;
                    padding: 20px;
                }
                
                .update-time {
                    font-family: 'Courier New', monospace;
                    color: var(--accent-blue);
                }
                
                /* Responsive */
                @media (max-width: 1200px) {
                    .stats-grid {
                        grid-template-columns: repeat(2, 1fr);
                    }
                    
                    table {
                        font-size: 12px;
                    }
                    
                    th, td {
                        padding: 12px 16px;
                    }
                }
            </style>
        </head>
        <body>
            <div class="bg-grid"></div>
            
            <div class="dashboard-container">
                <header>
                    <div class="brand">
                        <div class="logo">⚡</div>
                        <div class="brand-text">
                            <h1>Deriv Elite Scanner</h1>
                            <span>Professional Trading Suite</span>
                        </div>
                    </div>
                    <div class="connection-status" id="conn-status">
                        <div class="pulse-dot"></div>
                        <span class="status-text">Live Connection</span>
                    </div>
                </header>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-header">
                            <span class="stat-label">Active Symbols</span>
                            <span class="stat-icon">📊</span>
                        </div>
                        <div class="stat-value" id="stat-symbols">--</div>
                        <div class="stat-change">Real-time tracking</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <span class="stat-label">Connected Feeds</span>
                            <span class="stat-icon">🔌</span>
                        </div>
                        <div class="stat-value" id="stat-connected" style="color: var(--success);">--</div>
                        <div class="stat-change">WebSocket active</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <span class="stat-label">Best Signal</span>
                            <span class="stat-icon">🎯</span>
                        </div>
                        <div class="stat-value" id="stat-best" style="font-size: 18px;">--</div>
                        <div class="stat-change">Highest score today</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <span class="stat-label">Market Volatility</span>
                            <span class="stat-icon">📈</span>
                        </div>
                        <div class="stat-value" id="stat-volatility">--</div>
                        <div class="stat-change">Average ATR</div>
                    </div>
                </div>
                
                <div class="table-wrapper">
                    <div class="table-header">
                        <h3 class="table-title">Market Overview</h3>
                        <div class="live-indicator">
                            <div class="live-dot"></div>
                            <span>LIVE DATA</span>
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Price</th>
                                <th>Signal</th>
                                <th>Trend</th>
                                <th>Score</th>
                                <th>ATR</th>
                                <th>RSI</th>
                                <th>EMA Cross</th>
                            </tr>
                        </thead>
                        <tbody id="market-data">
                            <!-- Data populated via WebSocket -->
                        </tbody>
                    </table>
                </div>
                
                <div class="footer">
                    <p>Last Update: <span class="update-time" id="last-update">Waiting for data...</span></p>
                    <p style="margin-top: 5px; opacity: 0.6;">Deriv Elite Scanner V3.0 | Real-Time Market Analysis</p>
                </div>
            </div>
            
            <script>
                let ws;
                let reconnectInterval;
                let previousPrices = {};
                
                const signalIcons = {
                    'UPTREND': '↗️',
                    'DOWNTREND': '↘️',
                    'RANGE': '↔️',
                    'COMPRESSION': '🌀',
                    'SCANNING': '🔍',
                    'NEUTRAL': '➡️',
                    'INSUFFICIENT_DATA': '⚠️'
                };
                
                const signalClasses = {
                    'UPTREND': 'signal-uptrend',
                    'DOWNTREND': 'signal-downtrend',
                    'RANGE': 'signal-range',
                    'COMPRESSION': 'signal-compression',
                    'SCANNING': 'signal-scanning',
                    'NEUTRAL': 'signal-scanning',
                    'INSUFFICIENT_DATA': 'signal-scanning'
                };
                
                function connectWebSocket() {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
                    
                    ws.onopen = () => {
                        console.log('WebSocket connected');
                        document.getElementById('conn-status').classList.remove('disconnected');
                        clearInterval(reconnectInterval);
                        
                        // Send ping every 30s to keep alive
                        setInterval(() => {
                            if (ws.readyState === WebSocket.OPEN) {
                                ws.send('ping');
                            }
                        }, 30000);
                    };
                    
                    ws.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        updateDashboard(data);
                    };
                    
                    ws.onclose = () => {
                        console.log('WebSocket disconnected');
                        document.getElementById('conn-status').classList.add('disconnected');
                        document.querySelector('.status-text').textContent = 'Reconnecting...';
                        
                        // Attempt reconnect every 3 seconds
                        reconnectInterval = setInterval(() => {
                            connectWebSocket();
                        }, 3000);
                    };
                    
                    ws.onerror = (error) => {
                        console.error('WebSocket error:', error);
                    };
                }
                
                function updateDashboard(data) {
                    const tbody = document.getElementById('market-data');
                    const symbols = Object.keys(data.symbols);
                    
                    // Update stats
                    document.getElementById('stat-symbols').textContent = symbols.length;
                    
                    const connectedCount = symbols.filter(s => data.symbols[s].connected).length;
                    document.getElementById('stat-connected').textContent = `${connectedCount}/${symbols.length}`;
                    
                    // Find best signal
                    let bestScore = -1;
                    let bestSymbol = '-';
                    let totalATR = 0;
                    
                    symbols.forEach(symbol => {
                        const s = data.symbols[symbol];
                        if (s.score > bestScore) {
                            bestScore = s.score;
                            bestSymbol = symbol;
                        }
                        totalATR += s.atr;
                    });
                    
                    document.getElementById('stat-best').textContent = bestScore > 0 ? `${bestSymbol} (${bestScore})` : '-';
                    document.getElementById('stat-volatility').textContent = symbols.length > 0 ? (totalATR / symbols.length).toFixed(5) : '-';
                    
                    // Update table rows
                    symbols.forEach(symbol => {
                        const s = data.symbols[symbol];
                        let row = document.getElementById(`row-${symbol}`);
                        
                        // Create row if doesn't exist
                        if (!row) {
                            row = document.createElement('tr');
                            row.id = `row-${symbol}`;
                            tbody.appendChild(row);
                        }
                        
                        // Detect price change for flash effect
                        let flashClass = '';
                        if (previousPrices[symbol] !== undefined) {
                            if (s.price > previousPrices[symbol]) flashClass = 'flash-up';
                            else if (s.price < previousPrices[symbol]) flashClass = 'flash-down';
                        }
                        previousPrices[symbol] = s.price;
                        
                        // RSI styling
                        let rsiClass = 'rsi-normal';
                        if (s.rsi > 70) rsiClass = 'rsi-high';
                        else if (s.rsi < 30) rsiClass = 'rsi-low';
                        
                        // Score color
                        let scoreColor = '#ef4444';
                        if (s.score > 70) scoreColor = '#10b981';
                        else if (s.score > 40) scoreColor = '#f59e0b';
                        
                        row.className = flashClass;
                        row.innerHTML = `
                            <td>
                                <div class="symbol-info">
                                    <div class="conn-indicator ${s.connected ? '' : 'offline'}"></div>
                                    <span class="symbol-name">${symbol}</span>
                                </div>
                            </td>
                            <td>
                                <div class="price-wrapper">
                                    <span class="price-main">${s.price.toFixed(5)}</span>
                                </div>
                            </td>
                            <td>
                                <span class="signal-badge ${signalClasses[s.signal] || 'signal-scanning'}">
                                    ${signalIcons[s.signal] || '⚪'} ${s.signal.replace('_', ' ')}
                                </span>
                            </td>
                            <td>${s.trend}</td>
                            <td>
                                <div class="score-wrapper">
                                    <div class="score-bar">
                                        <div class="score-fill" style="width: ${Math.min(s.score, 100)}%; background: ${scoreColor};"></div>
                                    </div>
                                    <span class="score-text" style="color: ${scoreColor};">${s.score}</span>
                                </div>
                            </td>
                            <td class="metric">${s.atr.toFixed(5)}</td>
                            <td>
                                <span class="rsi-value ${rsiClass}">${s.rsi.toFixed(1)}</span>
                            </td>
                            <td>
                                <div class="ema-pair">
                                    <span class="ema-fast">${s.ema_fast.toFixed(2)}</span>
                                    <span class="ema-arrow">→</span>
                                    <span class="ema-slow">${s.ema_slow.toFixed(2)}</span>
                                </div>
                            </td>
                        `;
                    });
                    
                    // Update timestamp
                    const date = new Date(data.timestamp);
                    document.getElementById('last-update').textContent = date.toLocaleTimeString();
                }
                
                // Initialize connection
                connectWebSocket();
                
                // Cleanup on page unload
                window.addEventListener('beforeunload', () => {
                    if (ws) ws.close();
                });
            </script>
        </body>
        </html>
        """
        return HTMLResponse(html)
