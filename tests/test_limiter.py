import asyncio
import sqlite3

import pytest

from gateway.limiter import QuotaExceeded, TokenLimiter


async def test_sliding_expiry_and_restart(tmp_path):
    now = [100.0]
    path = str(tmp_path / "quota.sqlite3")
    limiter = TokenLimiter(path, limit=100, clock=lambda: now[0])
    await limiter.initialize()
    first = await limiter.reserve("a", 70)
    await limiter.settle(first, 60)
    now[0] = 120
    second = await limiter.reserve("a", 40)
    await limiter.settle(second)
    restarted = TokenLimiter(path, limit=100, clock=lambda: now[0])
    with pytest.raises(QuotaExceeded) as exc:
        await restarted.reserve("a", 1)
    assert exc.value.retry_after == 40
    now[0] = 160
    await restarted.reserve("a", 60)
    with pytest.raises(QuotaExceeded):
        await restarted.reserve("a", 1)
    await restarted.reserve("b", 100)
    with sqlite3.connect(path) as db:
        assert (
            db.execute("SELECT count(*) FROM token_usage WHERE id=?", (first,)).fetchone()[0] == 0
        )


async def test_concurrent_independent_connections_cannot_overspend(tmp_path):
    path = str(tmp_path / "quota.sqlite3")
    limiters = [TokenLimiter(path, limit=100) for _ in range(25)]
    await limiters[0].initialize()
    results = await asyncio.gather(
        *(lim.reserve("same", 10) for lim in limiters), return_exceptions=True
    )
    assert sum(isinstance(result, str) for result in results) == 10
    assert sum(isinstance(result, QuotaExceeded) for result in results) == 15


async def test_active_reservations_and_crash_expiry(tmp_path):
    now = [100.0]
    limiter = TokenLimiter(str(tmp_path / "quota.sqlite3"), limit=100, clock=lambda: now[0])
    await limiter.initialize()
    reservation = await limiter.reserve("a", 100, request_deadline=65)
    now[0] = 161
    with pytest.raises(QuotaExceeded):
        await limiter.reserve("a", 1)
    await limiter.settle(reservation, 30)
    # A duplicated settlement cannot refund more quota or extend its lifetime.
    await limiter.settle(reservation, 0)
    with pytest.raises(QuotaExceeded):
        await limiter.reserve("a", 71)
    now[0] = 221
    await limiter.reserve("a", 100, request_deadline=15)
    now[0] = 296
    await limiter.reserve("a", 100)


async def test_retry_after_waits_for_enough_capacity(tmp_path):
    now = [0.0]
    limiter = TokenLimiter(str(tmp_path / "quota.sqlite3"), limit=100, clock=lambda: now[0])
    await limiter.initialize()
    await limiter.settle(await limiter.reserve("t", 10))
    now[0] = 10
    await limiter.settle(await limiter.reserve("t", 90))
    with pytest.raises(QuotaExceeded) as exc:
        await limiter.reserve("t", 50)
    assert exc.value.retry_after == 60
