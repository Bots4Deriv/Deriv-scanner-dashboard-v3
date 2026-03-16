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
            
            # Enhanced color scheme with modern trading aesthetics
            signal_colors = {
                "RANGE": "#f59e0b",           # Amber
                "COMPRESSION": "#8b5cf6",     # Violet
                "UPTREND": "#10b981",         # Emerald
                "DOWNTREND": "#ef4444",       # Red
                "NEUTRAL": "#6b7280",         # Gray
                "SCANNING": "#3b82f6",        # Blue
                "INSUFFICIENT_DATA": "#64748b" # Slate
            }
            
            trend_icons = {
                "UPTREND": "↗️",
                "DOWNTREND": "↘️",
                "RANGE": "↔️",
                "NEUTRAL": "➡️",
                "COMPRESSION": "🌀",
                "SCANNING": "🔍",
                "INSUFFICIENT_DATA": "⚠️"
            }
            
            signal_color = signal_colors.get(data.signal, "#e5e7eb")
            trend_icon = trend_icons.get(data.signal, "➡️")
            connection_status_icon = "🟢" if connection_status.get(symbol) else "🔴"
            pulse_class = "pulse-live" if connection_status.get(symbol) else "pulse-dead"
            
            # Signal strength visualization
            score_width = min(max(data.score, 0), 100)
            score_color = "#10b981" if data.score > 70 else "#f59e0b" if data.score > 40 else "#ef4444"
            
            rows += f"""
            <tr class="data-row" data-symbol="{symbol}">
                <td>
                    <div class="symbol-cell">
                        <span class="connection-status {pulse_class}">{connection_status_icon}</span>
                        <span class="symbol-name">{symbol}</span>
                    </div>
                </td>
                <td class="price-cell">
                    <span class="price-value">{data.price:.5f}</span>
                    <span class="price-flash" id="flash-{symbol}"></span>
                </td>
                <td>
                    <span class="signal-badge" style="background: {signal_color}20; color: {signal_color}; border: 1px solid {signal_color}40;">
                        {trend_icon} {data.signal.replace('_', ' ')}
                    </span>
                </td>
                <td class="trend-cell">{data.trend}</td>
                <td>
                    <div class="score-container">
                        <div class="score-bar-bg">
                            <div class="score-bar-fill" style="width: {score_width}%; background: {score_color};"></div>
                        </div>
                        <span class="score-value">{data.score}</span>
                    </div>
                </td>
                <td class="metric-cell">{data.atr:.5f}</td>
                <td class="metric-cell">
                    <span class="rsi-value {"rsi-overbought" if data.rsi > 70 else "rsi-oversold" if data.rsi < 30 else ""}">
                        {data.rsi:.1f}
                    </span>
                </td>
                <td class="ema-cell">
                    <span class="ema-fast">{data.ema_fast:.5f}</span>
                    <span class="ema-separator">/</span>
                    <span class="ema-slow">{data.ema_slow:.5f}</span>
                </td>
            </tr>
            """
        
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Deriv Elite Scanner V3 | Pro Dashboard</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                :root {{
                    --bg-primary: #0f172a;
                    --bg-secondary: #1e293b;
                    --bg-tertiary: #334155;
                    --text-primary: #f8fafc;
                    --text-secondary: #94a3b8;
                    --accent-blue: #3b82f6;
                    --accent-green: #10b981;
                    --accent-red: #ef4444;
                    --border-color: #334155;
                    --hover-bg: #283548;
                }}
                
                body {{
                    font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
                    color: var(--text-primary);
                    min-height: 100vh;
                    padding: 20px;
                }}
                
                .dashboard-container {{
                    max-width: 1400px;
                    margin: 0 auto;
                }}
                
                header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 30px;
                    padding: 20px;
                    background: rgba(30, 41, 59, 0.8);
                    backdrop-filter: blur(10px);
                    border-radius: 16px;
                    border: 1px solid var(--border-color);
                    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
                }}
                
                .logo-section {{
                    display: flex;
                    align-items: center;
                    gap: 15px;
                }}
                
                .logo-icon {{
                    width: 50px;
                    height: 50px;
                    background: linear-gradient(135deg, #3b82f6, #8b5cf6);
                    border-radius: 12px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 24px;
                    box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4);
                }}
                
                h1 {{
                    font-size: 28px;
                    font-weight: 700;
                    background: linear-gradient(135deg, #f8fafc, #94a3b8);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                }}
                
                .version-badge {{
                    background: rgba(59, 130, 246, 0.2);
                    color: #60a5fa;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                    border: 1px solid rgba(59, 130, 246, 0.3);
                }}
                
                .stats-overview {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 15px;
                    margin-bottom: 25px;
                }}
                
                .stat-card {{
                    background: rgba(30, 41, 59, 0.6);
                    padding: 20px;
                    border-radius: 12px;
                    border: 1px solid var(--border-color);
                    transition: transform 0.2s, box-shadow 0.2s;
                }}
                
                .stat-card:hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.2);
                }}
                
                .stat-label {{
                    font-size: 12px;
                    color: var(--text-secondary);
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    margin-bottom: 8px;
                }}
                
                .stat-value {{
                    font-size: 24px;
                    font-weight: 700;
                    color: var(--text-primary);
                }}
                
                .table-container {{
                    background: rgba(30, 41, 59, 0.6);
                    border-radius: 16px;
                    border: 1px solid var(--border-color);
                    overflow: hidden;
                    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
                }}
                
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                
                thead {{
                    background: rgba(15, 23, 42, 0.8);
                    position: sticky;
                    top: 0;
                }}
                
                th {{
                    padding: 16px;
                    text-align: left;
                    font-size: 12px;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    color: var(--text-secondary);
                    border-bottom: 1px solid var(--border-color);
                }}
                
                td {{
                    padding: 16px;
                    border-bottom: 1px solid rgba(51, 65, 85, 0.5);
                    font-size: 14px;
                }}
                
                tr.data-row {{
                    transition: background 0.2s;
                }}
                
                tr.data-row:hover {{
                    background: var(--hover-bg);
                }}
                
                .symbol-cell {{
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}
                
                .connection-status {{
                    font-size: 10px;
                }}
                
                .pulse-live {{
                    animation: pulse 2s infinite;
                }}
                
                @keyframes pulse {{
                    0%, 100% {{ opacity: 1; }}
                    50% {{ opacity: 0.5; }}
                }}
                
                .symbol-name {{
                    font-weight: 600;
                    font-size: 14px;
                    color: var(--text-primary);
                }}
                
                .price-cell {{
                    position: relative;
                    font-family: 'Courier New', monospace;
                    font-weight: 700;
                    font-size: 15px;
                }}
                
                .price-value {{
                    color: var(--accent-green);
                }}
                
                .signal-badge {{
                    padding: 6px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                }}
                
                .trend-cell {{
                    font-weight: 500;
                    color: var(--text-secondary);
                }}
                
                .score-container {{
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}
                
                .score-bar-bg {{
                    width: 60px;
                    height: 6px;
                    background: rgba(255, 255, 255, 0.1);
                    border-radius: 3px;
                    overflow: hidden;
                }}
                
                .score-bar-fill {{
                    height: 100%;
                    border-radius: 3px;
                    transition: width 0.3s ease;
                }}
                
                .score-value {{
                    font-weight: 600;
                    font-size: 13px;
                    color: var(--text-secondary);
                }}
                
                .metric-cell {{
                    font-family: 'Courier New', monospace;
                    color: var(--text-secondary);
                }}
                
                .rsi-value {{
                    padding: 4px 8px;
                    border-radius: 6px;
                    font-weight: 600;
                }}
                
                .rsi-overbought {{
                    background: rgba(239, 68, 68, 0.2);
                    color: #f87171;
                }}
                
                .rsi-oversold {{
                    background: rgba(16, 185, 129, 0.2);
                    color: #34d399;
                }}
                
                .ema-cell {{
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                }}
                
                .ema-fast {{
                    color: #60a5fa;
                    font-weight: 600;
                }}
                
                .ema-slow {{
                    color: #f472b6;
                    font-weight: 600;
                }}
                
                .ema-separator {{
                    color: var(--text-secondary);
                    margin: 0 4px;
                }}
                
                .last-update {{
                    text-align: center;
                    margin-top: 20px;
                    color: var(--text-secondary);
                    font-size: 12px;
                }}
                
                /* Scrollbar styling */
                ::-webkit-scrollbar {{
                    width: 8px;
                    height: 8px;
                }}
                
                ::-webkit-scrollbar-track {{
                    background: var(--bg-primary);
                }}
                
                ::-webkit-scrollbar-thumb {{
                    background: var(--bg-tertiary);
                    border-radius: 4px;
                }}
                
                ::-webkit-scrollbar-thumb:hover {{
                    background: #475569;
                }}
                
                @media (max-width: 768px) {{
                    header {{
                        flex-direction: column;
                        gap: 15px;
                        text-align: center;
                    }}
                    
                    table {{
                        font-size: 12px;
                    }}
                    
                    th, td {{
                        padding: 12px 8px;
                    }}
                    
                    .score-bar-bg {{
                        width: 40px;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="dashboard-container">
                <header>
                    <div class="logo-section">
                        <div class="logo-icon">📊</div>
                        <div>
                            <h1>Deriv Elite Scanner</h1>
                            <span class="version-badge">V3 Professional</span>
                        </div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 12px; color: var(--text-secondary);">Market Status</div>
                        <div style="font-size: 14px; font-weight: 600; color: var(--accent-green);">● Live Trading</div>
                    </div>
                </header>
                
                <div class="stats-overview">
                    <div class="stat-card">
                        <div class="stat-label">Active Symbols</div>
                        <div class="stat-value">{len(Config.SYMBOLS)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Connected</div>
                        <div class="stat-value" style="color: var(--accent-green);">
                            {sum(1 for s in Config.SYMBOLS if connection_status.get(s))}/{len(Config.SYMBOLS)}
                        </div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Signals Today</div>
                        <div class="stat-value" style="color: var(--accent-blue);">Active</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">System Status</div>
                        <div class="stat-value" style="color: var(--accent-green);">Online</div>
                    </div>
                </div>
                
                <div class="table-container">
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
                                <th>EMA 9/21</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows}
                        </tbody>
                    </table>
                </div>
                
                <div class="last-update">
                    Last updated: <span id="timestamp">{__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span> UTC
                </div>
            </div>
            
            <script>
                // Auto-refresh every 5 seconds
                setInterval(() => {{
                    window.location.reload();
                }}, 5000);
                
                // Add flash effect to price changes (simulated)
                document.querySelectorAll('.price-cell').forEach(cell => {{
                    cell.addEventListener('DOMContentLoaded', () => {{
                        cell.style.animation = 'flash 0.5s ease';
                    }});
                }});
            </script>
        </body>
        </html>
        """
        return HTMLResponse(html)
