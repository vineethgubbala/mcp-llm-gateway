import time
from contextlib import asynccontextmanager

import httpx
import jwt

from gateway.app import Settings, create_app
from gateway.limiter import TokenLimiter

SECRET = "only-for-tests-" * 4


def auth(role="viewer", tenant="tenant-a", secret=SECRET, expiry=3600):
    now = int(time.time())
    token = jwt.encode(
        {
            "role": role,
            "sub": tenant,
            "iat": now,
            "exp": now + expiry,
            "iss": "gateway-demo",
            "aud": "gateway",
        },
        secret,
        algorithm="HS256",
    )
    return {"Authorization": "Bearer " + token}


@asynccontextmanager
async def gateway(tmp_path, handler, limit=50_000):
    settings = Settings(
        jwt_secret=SECRET, db_path=str(tmp_path / "quota.sqlite3"), allow_local_http=True
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as upstream:
        app = create_app(settings, upstream, TokenLimiter(settings.db_path, limit=limit))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client, app
