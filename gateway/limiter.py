import asyncio
import math
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path


class QuotaExceeded(Exception):
    def __init__(self, retry_after):
        self.retry_after = retry_after


class TokenLimiter:
    def __init__(self, path: str, limit=50_000, window=60, clock=time.time):
        if path == ":memory:":
            raise ValueError("Use an on disk SQLite database")
        self.path, self.limit, self.window, self.clock = path, limit, window, clock

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    async def initialize(self):
        await asyncio.to_thread(self._initialize)

    def _initialize(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS token_usage (
                id TEXT PRIMARY KEY, tenant TEXT NOT NULL, tokens INTEGER NOT NULL,
                expires REAL NOT NULL, settled INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS usage_expiry ON token_usage(expires)")
            db.execute("CREATE INDEX IF NOT EXISTS usage_tenant ON token_usage(tenant, expires)")

    async def reserve(self, tenant, tokens, request_deadline=15):
        if type(tokens) is not int or tokens <= 0:
            raise ValueError("Token reservation must be a positive integer")
        return await asyncio.to_thread(self._reserve, tenant, tokens, request_deadline)

    def _reserve(self, tenant, tokens, request_deadline):
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                now = self.clock()
                db.execute("DELETE FROM token_usage WHERE expires <= ?", (now,))
                rows = db.execute(
                    "SELECT tokens, expires FROM token_usage WHERE tenant=? ORDER BY expires",
                    (tenant,),
                ).fetchall()
                used = sum(row[0] for row in rows)
                if used + tokens > self.limit:
                    retry = self.window
                    for charge, expiry in rows:
                        used -= charge
                        retry = max(1, math.ceil(expiry - now))
                        if used + tokens <= self.limit:
                            break
                    raise QuotaExceeded(retry)
                reservation = uuid.uuid4().hex
                db.execute(
                    "INSERT INTO token_usage VALUES (?, ?, ?, ?, 0)",
                    (
                        reservation,
                        tenant,
                        tokens,
                        now + request_deadline + self.window,
                    ),
                )
                db.commit()
                return reservation
            except BaseException:
                db.rollback()
                raise

    async def settle(self, reservation, actual=None):
        if actual is not None and (type(actual) is not int or actual < 0):
            raise ValueError("Invalid usage")
        await asyncio.to_thread(self._settle, reservation, actual)

    def _settle(self, reservation, actual):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE token_usage
                SET tokens=COALESCE(?, tokens), expires=?, settled=1
                WHERE id=? AND settled=0""",
                (actual, self.clock() + self.window, reservation),
            )
            db.commit()
