import ccxt.pro as ccxtpro
from loguru import logger

def compute_order_amount_contracts(
    exchange: ccxtpro.Exchange,
    symbol: str,
    base_qty: float,
) -> float:
    market = exchange.market(symbol)
    contract_size = float(market.get('contractSize', 1) or 1)

    raw_amount = base_qty / contract_size
    amount = float(exchange.amount_to_precision(symbol, raw_amount))
    return amount

def get_contract_size(exchange: ccxtpro.Exchange, symbol: str) -> float:
    return float(exchange.market(symbol).get('contractSize', 1) or 1)

def setup_trading(exchange_id: str, is_testnet: bool = False) -> ccxtpro.Exchange:
    from src.config import SETTINGS
    options = {'defaultType': 'swap', 'createMarketBuyOrderRequiresPrice': False}
    config = {
        'enableRateLimit': True,
        'options': options,
        'timeout': 10000,
    }

    if hasattr(SETTINGS, exchange_id.upper()):
        creds = getattr(SETTINGS, exchange_id.upper())
        config['apiKey'] = creds['API_KEY']
        config['secret'] = creds['API_SECRET']
        if 'API_PASSWORD' in creds:
            config['password'] = creds['API_PASSWORD']

    ex_class = getattr(ccxtpro, exchange_id.lower())
    exchange = ex_class(config)
    if is_testnet:
        exchange.set_sandbox_mode(True)
    return exchange
