from __future__ import annotations

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS spread_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    exchange_long TEXT NOT NULL,
    exchange_short TEXT NOT NULL,
    price_spread_pct REAL NOT NULL,
    funding_long REAL NOT NULL,
    funding_short REAL NOT NULL,
    funding_spread_pct REAL NOT NULL,
    real_spread_pct REAL NOT NULL,
    direction TEXT NOT NULL,
    prices_json TEXT NOT NULL,
    tick_age_long_ms REAL DEFAULT 0,
    tick_age_short_ms REAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_snap_symbol_ts ON spread_snapshots(symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS funding_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    funding_rate REAL NOT NULL,
    next_funding_time REAL,
    interval_hours REAL NOT NULL,
    timestamp REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fund_symbol_ts ON funding_log(symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    exchange_long TEXT NOT NULL,
    exchange_short TEXT NOT NULL,
    opened_at REAL NOT NULL,
    closed_at REAL DEFAULT 0,
    entry_spread_pct REAL NOT NULL,
    close_spread_pct REAL DEFAULT 0,
    amount_usdt REAL NOT NULL,
    open_long_quoted REAL DEFAULT 0,
    open_long_fill REAL DEFAULT 0,
    open_long_filled_qty REAL DEFAULT 0,
    open_long_slip_pct REAL DEFAULT 0,
    open_long_latency_ms REAL DEFAULT 0,
    open_long_ack_latency_ms REAL DEFAULT 0,
    open_long_decision_to_order_ms REAL DEFAULT 0,
    open_long_tick_age_ms REAL DEFAULT 0,
    open_long_quote_age_ms REAL DEFAULT 0,
    open_short_quoted REAL DEFAULT 0,
    open_short_fill REAL DEFAULT 0,
    open_short_filled_qty REAL DEFAULT 0,
    open_short_slip_pct REAL DEFAULT 0,
    open_short_latency_ms REAL DEFAULT 0,
    open_short_ack_latency_ms REAL DEFAULT 0,
    open_short_decision_to_order_ms REAL DEFAULT 0,
    open_short_tick_age_ms REAL DEFAULT 0,
    open_short_quote_age_ms REAL DEFAULT 0,
    close_long_quoted REAL DEFAULT 0,
    close_long_fill REAL DEFAULT 0,
    close_long_filled_qty REAL DEFAULT 0,
    close_long_slip_pct REAL DEFAULT 0,
    close_long_latency_ms REAL DEFAULT 0,
    close_long_ack_latency_ms REAL DEFAULT 0,
    close_long_decision_to_order_ms REAL DEFAULT 0,
    close_long_tick_age_ms REAL DEFAULT 0,
    close_long_quote_age_ms REAL DEFAULT 0,
    close_short_quoted REAL DEFAULT 0,
    close_short_fill REAL DEFAULT 0,
    close_short_filled_qty REAL DEFAULT 0,
    close_short_slip_pct REAL DEFAULT 0,
    close_short_latency_ms REAL DEFAULT 0,
    close_short_ack_latency_ms REAL DEFAULT 0,
    close_short_decision_to_order_ms REAL DEFAULT 0,
    close_short_tick_age_ms REAL DEFAULT 0,
    close_short_quote_age_ms REAL DEFAULT 0,
    close_latency_ms REAL DEFAULT 0,
    total_latency_ms REAL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS idx_deals_symbol ON deals(symbol, opened_at DESC);
"""

DEAL_MIGRATION_COLUMNS = (
    "close_latency_ms",
    "open_long_filled_qty",
    "open_short_filled_qty",
    "close_long_filled_qty",
    "close_short_filled_qty",
    "open_long_tick_age_ms",
    "open_long_ack_latency_ms",
    "open_long_decision_to_order_ms",
    "open_long_quote_age_ms",
    "open_short_tick_age_ms",
    "open_short_ack_latency_ms",
    "open_short_decision_to_order_ms",
    "open_short_quote_age_ms",
    "close_long_tick_age_ms",
    "close_long_ack_latency_ms",
    "close_long_decision_to_order_ms",
    "close_long_quote_age_ms",
    "close_short_tick_age_ms",
    "close_short_ack_latency_ms",
    "close_short_decision_to_order_ms",
    "close_short_quote_age_ms",
)

DEAL_COLUMNS = [
    "id",
    "symbol",
    "exchange_long",
    "exchange_short",
    "opened_at",
    "closed_at",
    "entry_spread_pct",
    "close_spread_pct",
    "amount_usdt",
    "status",
    "total_latency_ms",
    "open_long_quoted",
    "open_long_fill",
    "open_long_filled_qty",
    "open_long_slip_pct",
    "open_long_latency_ms",
    "open_long_ack_latency_ms",
    "open_long_decision_to_order_ms",
    "open_long_tick_age_ms",
    "open_long_quote_age_ms",
    "open_short_quoted",
    "open_short_fill",
    "open_short_filled_qty",
    "open_short_slip_pct",
    "open_short_latency_ms",
    "open_short_ack_latency_ms",
    "open_short_decision_to_order_ms",
    "open_short_tick_age_ms",
    "open_short_quote_age_ms",
    "close_long_quoted",
    "close_long_fill",
    "close_long_filled_qty",
    "close_long_slip_pct",
    "close_long_latency_ms",
    "close_long_ack_latency_ms",
    "close_long_decision_to_order_ms",
    "close_long_tick_age_ms",
    "close_long_quote_age_ms",
    "close_short_quoted",
    "close_short_fill",
    "close_short_filled_qty",
    "close_short_slip_pct",
    "close_short_latency_ms",
    "close_short_ack_latency_ms",
    "close_short_decision_to_order_ms",
    "close_short_tick_age_ms",
    "close_short_quote_age_ms",
    "close_latency_ms",
]


async def initialize_storage_schema(db: aiosqlite.Connection) -> None:
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("PRAGMA auto_vacuum=INCREMENTAL")
    await db.executescript(SCHEMA)
    await _migrate_deals(db)
    await db.commit()


async def _migrate_deals(db: aiosqlite.Connection) -> None:
    for column in DEAL_MIGRATION_COLUMNS:
        try:
            await db.execute(f"ALTER TABLE deals ADD COLUMN {column} REAL DEFAULT 0")
        except Exception:
            pass
