"""SQLite storage adapter — throttled writes (interval OR spread improvement)."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Optional

import aiosqlite
from loguru import logger

from src.adapters import storage_deals
from src.adapters.storage_schema import initialize_storage_schema
from src.domain.models import SpreadSnapshot

class SpreadStorage:
    def __init__(
        self,
        db_path: str = "spreads.db",
        interval_sec: float = 5.0,
        save_on_improvement: bool = True,
        spread_retention_hours: float = 24.0,
        funding_retention_days: float = 7.0,
        max_size_mb: float = 1024.0,
    ):
        self._db_path = db_path
        self._interval_sec = interval_sec
        self._save_on_improvement = save_on_improvement
        self._spread_retention_hours = spread_retention_hours
        self._funding_retention_days = funding_retention_days
        # 0 disables the cap; useful for tests and tiny deployments.
        self._max_size_bytes = int(max_size_mb * 1024 * 1024) if max_size_mb > 0 else 0
        self._db: Optional[aiosqlite.Connection] = None

        # Throttle state per symbol
        self._last_saved_time: dict[str, float] = {}
        self._last_saved_spread: dict[str, float] = {}

    async def init_db(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        await initialize_storage_schema(self._db)
        logger.info("Database initialized: {}", self._db_path)

    async def cleanup_old_data(self) -> int:
        """Delete spread_snapshots older than retention period and funding older than retention.

        Uses batched deletes to avoid long-running transactions.
        Returns total rows deleted.
        """
        if not self._db:
            return 0

        now = time.time()
        spread_cutoff = now - self._spread_retention_hours * 3600
        funding_cutoff = now - self._funding_retention_days * 86400
        total = 0
        batch_size = 10000

        # Batch delete spread_snapshots
        while True:
            cursor = await self._db.execute(
                "DELETE FROM spread_snapshots WHERE rowid IN "
                "(SELECT rowid FROM spread_snapshots WHERE timestamp < ? LIMIT ?)",
                (spread_cutoff, batch_size),
            )
            await self._db.commit()
            deleted = cursor.rowcount
            total += deleted
            if deleted < batch_size:
                break

        # Batch delete funding_log
        while True:
            cursor = await self._db.execute(
                "DELETE FROM funding_log WHERE rowid IN "
                "(SELECT rowid FROM funding_log WHERE timestamp < ? LIMIT ?)",
                (funding_cutoff, batch_size),
            )
            await self._db.commit()
            deleted = cursor.rowcount
            total += deleted
            if deleted < batch_size:
                break

        if total > 0:
            # Reclaim disk space from deleted pages (non-blocking, frees up to 1000 pages per run)
            await self._db.execute("PRAGMA incremental_vacuum(1000)")
            await self._db.commit()
            logger.info(
                "Cleanup: {} rows deleted (spreads < {}h, funding < {}d)",
                total,
                self._spread_retention_hours,
                self._funding_retention_days,
            )

        # Then enforce the absolute size cap (independent of time-based rules).
        # Time-based rules drop rows older than X; the size cap is a backstop for
        # bursty write rates where 24h of data still exceeds the disk budget.
        total += await self.enforce_size_limit()
        return total

    def _db_size_bytes(self) -> int:
        """Wall size on disk including the WAL sidecar, which can grow large
        between checkpoints. Returns 0 if the file is missing (pre-init)."""
        try:
            size = os.path.getsize(self._db_path)
        except OSError:
            return 0
        for suffix in ("-wal", "-shm"):
            try:
                size += os.path.getsize(self._db_path + suffix)
            except OSError:
                pass
        return size

    async def enforce_size_limit(self) -> int:
        """Drop oldest spread_snapshots in batches until the file is under
        ``max_size_mb``. Returns rows deleted. No-op when the cap is 0 or the
        file is already under the threshold.

        Spread snapshots dominate the byte budget (large prices_json blob);
        funding_log and deals are tiny by comparison and intentionally untouched.
        """
        if not self._db or self._max_size_bytes <= 0:
            return 0
        size = self._db_size_bytes()
        if size <= self._max_size_bytes:
            return 0

        logger.warning(
            "DB size cap exceeded: {:.1f} MB > {:.1f} MB cap, trimming oldest snapshots",
            size / 1024 / 1024,
            self._max_size_bytes / 1024 / 1024,
        )
        batch_size = 10000
        total = 0
        # Bounded loop: each iteration either frees space and shrinks the file
        # or exits when no more snapshot rows remain.
        for _ in range(200):
            cursor = await self._db.execute(
                "DELETE FROM spread_snapshots WHERE rowid IN "
                "(SELECT rowid FROM spread_snapshots ORDER BY timestamp ASC LIMIT ?)",
                (batch_size,),
            )
            await self._db.commit()
            deleted = cursor.rowcount
            total += deleted
            if deleted == 0:
                break
            # Reclaim freed pages so the file actually shrinks.
            await self._db.execute("PRAGMA wal_checkpoint(PASSIVE)")
            await self._db.execute("PRAGMA incremental_vacuum(2000)")
            await self._db.commit()
            if self._db_size_bytes() <= self._max_size_bytes:
                break

        if total > 0:
            logger.info(
                "Size cap enforced: {} oldest snapshots dropped, db now {:.1f} MB",
                total,
                self._db_size_bytes() / 1024 / 1024,
            )
        return total

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    def should_save(self, snapshot: SpreadSnapshot) -> bool:
        key = snapshot.symbol
        now = time.time()

        last_time = self._last_saved_time.get(key, 0.0)
        time_ok = (now - last_time) >= self._interval_sec

        improvement_ok = False
        if self._save_on_improvement:
            last_spread = self._last_saved_spread.get(key)
            if last_spread is None:
                improvement_ok = True
            else:
                change = abs(snapshot.real_spread_pct) - abs(last_spread)
                # Only save if spread improved by at least 0.1% absolute
                if change >= 0.1:
                    improvement_ok = True

        return time_ok or improvement_ok

    async def save_spread(self, snapshot: SpreadSnapshot) -> bool:
        if not self.should_save(snapshot):
            return False
        if not self._db:
            return False

        # Extract tick age from prices metadata
        long_prices = snapshot.prices.get(snapshot.exchange_long, {})
        short_prices = snapshot.prices.get(snapshot.exchange_short, {})
        tick_age_long_ms = float(long_prices.get("tick_age_ms", 0) or 0)
        tick_age_short_ms = float(short_prices.get("tick_age_ms", 0) or 0)

        await self._db.execute(
            """INSERT INTO spread_snapshots
               (symbol, timestamp, exchange_long, exchange_short,
                price_spread_pct, funding_long, funding_short,
                funding_spread_pct, real_spread_pct, direction, prices_json,
                tick_age_long_ms, tick_age_short_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                snapshot.symbol,
                snapshot.timestamp,
                snapshot.exchange_long,
                snapshot.exchange_short,
                snapshot.price_spread_pct,
                snapshot.funding_long,
                snapshot.funding_short,
                snapshot.funding_spread_pct,
                snapshot.real_spread_pct,
                snapshot.direction,
                json.dumps(snapshot.prices),
                tick_age_long_ms,
                tick_age_short_ms,
            ),
        )
        await self._db.commit()

        self._last_saved_time[snapshot.symbol] = time.time()
        self._last_saved_spread[snapshot.symbol] = snapshot.real_spread_pct
        return True

    async def _query_rows(
        self,
        query: str,
        params: tuple,
        keys: list[str],
    ) -> list[dict]:
        """Execute a SELECT via a short-lived sqlite3 connection in a thread.

        This bypasses the aiosqlite write connection entirely, so reads
        never queue behind continuous writes.
        """
        if not self._db:
            return []
        db_path = self._db_path

        def _run():
            conn = sqlite3.connect(db_path, timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            try:
                cursor = conn.execute(query, params)
                rows = cursor.fetchall()
                return [{k: r[i] for i, k in enumerate(keys)} for r in rows]
            finally:
                conn.close()

        return await asyncio.to_thread(_run)

    _HISTORY_COLS = [
        "timestamp",
        "exchange_long",
        "exchange_short",
        "price_spread_pct",
        "funding_long",
        "funding_short",
        "funding_spread_pct",
        "real_spread_pct",
        "direction",
        "prices_json",
        "tick_age_long_ms",
        "tick_age_short_ms",
    ]

    _HISTORY_SELECT = """timestamp, exchange_long, exchange_short,
                         price_spread_pct, funding_long, funding_short,
                         funding_spread_pct, real_spread_pct, direction, prices_json,
                         tick_age_long_ms, tick_age_short_ms"""

    async def get_history(
        self,
        symbol: str,
        limit: int = 200,
        since: Optional[float] = None,
        bucket_sec: int = 0,
        slim: bool = False,
    ) -> list[dict]:
        if bucket_sec > 0:
            return await self._get_history_downsampled(
                symbol, limit, since, bucket_sec, slim
            )

        if since is not None:
            query = f"""SELECT {self._HISTORY_SELECT}
                       FROM spread_snapshots
                       WHERE symbol = ? AND timestamp >= ?
                       ORDER BY timestamp DESC
                       LIMIT ?"""
            params = (symbol, since, limit)
        else:
            query = f"""SELECT {self._HISTORY_SELECT}
                       FROM spread_snapshots
                       WHERE symbol = ?
                       ORDER BY timestamp DESC
                       LIMIT ?"""
            params = (symbol, limit)
        rows = await self._query_rows(query, params, self._HISTORY_COLS)
        self._post_process_rows(rows, slim)
        return rows

    @staticmethod
    def _post_process_rows(rows: list[dict], slim: bool) -> None:
        """Parse prices_json; optionally strip to {exId: last} for chart mode."""
        for row in rows:
            prices = json.loads(row.pop("prices_json"))
            if slim:
                row["prices"] = {
                    k: round(v.get("last") or v.get("ask") or v.get("bid") or 0, 6)
                    for k, v in prices.items()
                }
                row.pop("direction", None)
            else:
                row["prices"] = prices

    async def _get_history_downsampled(
        self,
        symbol: str,
        limit: int,
        since: Optional[float],
        bucket_sec: int,
        slim: bool = False,
    ) -> list[dict]:
        """Return one row per time bucket, picking the peak abs(spread)."""
        import time as _time

        where = "WHERE symbol = ?"
        params: list = [symbol]
        # Cap scan range: if no since provided, default to 3 days max
        effective_since = since
        if effective_since is None:
            effective_since = _time.time() - 3 * 86400
        where += " AND timestamp >= ?"
        params.append(effective_since)

        query = f"""
            SELECT {self._HISTORY_SELECT},
                   MAX(ABS(real_spread_pct)) AS _peak
            FROM spread_snapshots
            {where}
            GROUP BY CAST(timestamp / ? AS INTEGER)
            ORDER BY timestamp DESC
            LIMIT ?"""
        params.extend([bucket_sec, limit])

        rows = await self._query_rows(query, tuple(params), self._HISTORY_COLS)
        self._post_process_rows(rows, slim)
        return rows

    async def save_funding(
        self,
        exchange_id: str,
        symbol: str,
        funding_rate: float,
        next_funding_time: Optional[float],
        interval_hours: float,
        timestamp: float,
    ) -> None:
        if not self._db:
            return
        await self._db.execute(
            """INSERT INTO funding_log
               (exchange_id, symbol, funding_rate, next_funding_time, interval_hours, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                exchange_id,
                symbol,
                funding_rate,
                next_funding_time,
                interval_hours,
                timestamp,
            ),
        )
        await self._db.commit()

    async def get_funding_history(
        self,
        symbol: str,
        limit: int = 500,
        since: Optional[float] = None,
    ) -> list[dict]:
        if since is not None:
            query = """SELECT exchange_id, funding_rate, next_funding_time, interval_hours, timestamp
                       FROM funding_log
                       WHERE symbol = ? AND timestamp >= ?
                       ORDER BY timestamp DESC
                       LIMIT ?"""
            params = (symbol, since, limit)
        else:
            query = """SELECT exchange_id, funding_rate, next_funding_time, interval_hours, timestamp
                       FROM funding_log
                       WHERE symbol = ?
                       ORDER BY timestamp DESC
                       LIMIT ?"""
            params = (symbol, limit)
        return await self._query_rows(
            query,
            params,
            [
                "exchange_id",
                "funding_rate",
                "next_funding_time",
                "interval_hours",
                "timestamp",
            ],
        )

    # Tiny in-process cache for /api/max_spread. The dashboard polls this
    # frequently; a 15 s cache is invisible to users and eliminates duplicate
    # full-range scans.
    _MAX_SPREAD_CACHE_TTL_SEC: float = 15.0

    async def get_max_spread(
        self,
        symbol: str,
        hours: float = 8.0,
    ) -> dict | None:
        """Return the row with max |real_spread_pct| in the last N hours.

        Performance notes:
          * Uses the class's existing aiosqlite connection (no fresh-connect
            + per-call PRAGMA spam). Previously this ran ~2-3 s per call.
          * Splits `ORDER BY ABS(expr)` into two separately-indexed queries
            (max positive, min negative) so `idx_snap_symbol_ts` is usable
            by each. SQLite picks whichever has a larger absolute value.
          * 15 s in-memory TTL cache to absorb dashboard polling bursts.
        """
        if not self._db:
            return None

        # Cache lookup (per-symbol, per-hours key)
        now = time.time()
        if not hasattr(self, "_max_spread_cache"):
            self._max_spread_cache: dict[tuple[str, float], tuple[float, dict | None]] = {}
        cache_key = (symbol, hours)
        cached = self._max_spread_cache.get(cache_key)
        if cached and (now - cached[0]) < self._MAX_SPREAD_CACHE_TTL_SEC:
            return cached[1]

        since = now - hours * 3600
        keys = (
            "timestamp",
            "exchange_long",
            "exchange_short",
            "price_spread_pct",
            "real_spread_pct",
            "direction",
            "tick_age_long_ms",
            "tick_age_short_ms",
        )
        base = (
            "SELECT timestamp, exchange_long, exchange_short, "
            "price_spread_pct, real_spread_pct, direction, "
            "tick_age_long_ms, tick_age_short_ms "
            "FROM spread_snapshots WHERE symbol = ? AND timestamp >= ? "
        )
        # Two tiny indexed fetches; SQLite can use idx_snap_symbol_ts for both.
        params = (symbol, since)
        cursor_pos = await self._db.execute(
            base + "ORDER BY real_spread_pct DESC LIMIT 1", params,
        )
        row_pos = await cursor_pos.fetchone()
        cursor_neg = await self._db.execute(
            base + "ORDER BY real_spread_pct ASC LIMIT 1", params,
        )
        row_neg = await cursor_neg.fetchone()

        candidates = [r for r in (row_pos, row_neg) if r is not None]
        if not candidates:
            self._max_spread_cache[cache_key] = (now, None)
            return None

        # Pick the row with the larger absolute spread.
        # real_spread_pct is at index 4 in the SELECT list above.
        best = max(candidates, key=lambda r: abs(r[4]))
        result = {k: best[i] for i, k in enumerate(keys)}
        self._max_spread_cache[cache_key] = (now, result)
        return result

    async def save_deal(self, deal: dict) -> int | None:
        """Save a new deal (position opened). Returns the deal row id."""
        return await storage_deals.save_deal(self._db, deal)

    async def close_deal(self, deal_id: int, close_data: dict) -> None:
        """Update a deal with close info."""
        await storage_deals.close_deal(self._db, deal_id, close_data)

    async def force_close_deal(self, deal_id: int) -> None:
        """Mark a deal as manually closed (positions closed outside bot)."""
        await storage_deals.force_close_deal(self._db, deal_id)

    async def cancel_deal(self, deal_id: int, reason: str = "orphaned") -> None:
        """Mark a deal as cancelled (e.g. positions vanished from exchange)."""
        await storage_deals.cancel_deal(self._db, deal_id, reason)

    async def get_deals(
        self, symbol: str, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        """Get recent deals with full leg details, supporting pagination."""
        return await storage_deals.get_deals(self._query_rows, symbol, limit, offset)

    async def get_deals_count(self, symbol: str) -> int:
        """Return total deal count for a symbol."""
        return await storage_deals.get_deals_count(self._query_rows, symbol)

    async def get_deals_cumulative(
        self,
        symbol: str,
        deal_ids: set[int] | None = None,
    ) -> dict:
        return await storage_deals.get_deals_cumulative(
            self._query_rows,
            symbol,
            deal_ids,
        )

    async def get_open_deals(self) -> list[dict]:
        """Return all open deals."""
        return await storage_deals.get_open_deals(self._query_rows)
