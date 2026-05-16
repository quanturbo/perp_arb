# perp_arb_v2

Async perpetual futures arbitrage bot for crypto exchanges. Monitors spread between multiple exchanges in real time and places hedged long/short pairs when spread thresholds are met.

## Features

- Real-time WebSocket price streams (Binance, Bybit, OKX, Gate.io, Bitget, MEXC, KuCoin, BingX, Aster)
- Direct-order WS execution for Binance, Bybit, OKX, Gate.io (lower latency than REST)
- Position lifecycle management: open, monitor, close, reconcile
- Symbol quarantine to auto-disable problem pairs
- Funding-rate poller and volume poller
- Web dashboard with live stats and runtime config editor
- Telegram alert notifications
- Persistent SQLite deal storage
- Deploy tools for remote server management

## Requirements

- Python 3.11+
- Dependencies: `pip install -r requirements.txt`

## Setup

```bash
cp .env.example .env
# Fill in your exchange API keys and Telegram credentials in .env
python main.py
```

## Deploy tools

Scripts in `tools/` use environment variables for server connection:

```powershell
$env:DEPLOY_HOST     = "ubuntu@YOUR_SERVER_IP"
$env:DEPLOY_REMOTE_DIR = "/home/ubuntu/bot"
$env:SSH_KEY_PATH    = "C:/path/to/your-key.pem"

.\tools\deploy_changed.ps1 -Restart -Check
.\tools\check_remote.ps1
```

## Structure

```
src/
  adapters/    # Exchange connections, WebSocket streams, HTTP client, Telegram
  domain/      # Trading logic, position model, spread calculation
  scanners/    # Opportunity discovery and alerting
  web/         # aiohttp dashboard API and HTML template
main.py        # Entry point
tools/         # Deploy and diagnostic scripts
```

## License

Private — all rights reserved.
