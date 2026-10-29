import asyncio
import json
import sqlite3

import httpx
import pytest

from gateway.provider import Completion, Endpoint, ModelRouter
from tests.conftest import auth, gateway

PAYLOAD = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 20}


def success():
    return httpx.Response(
        200, json={"choices": [{"message": {"content": "hello"}}], "usage": {"total_tokens": 10}}
    )


@pytest.mark.parametrize("failure", ["429", "timeout", "connection"])
async def test_fallback_and_primary_cancellation(tmp_path, failure):
    calls = []
    cancelled = asyncio.Event()

    async def upstream(request):
        calls.append(request.url.path)
        if request.url.path == "/primary":
            if failure == "429":
                return httpx.Response(429, text="secret internal trace")
            if failure == "connection":
                raise httpx.ConnectError("private host and credentials")
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return success()

    async with gateway(tmp_path, upstream) as (client, app):
        assert app.state.router.primary_timeout == 3.0
        app.state.router.primary_timeout = 0.02
        response = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert response.status_code == 200
        assert response.json()["fallback"] is True
        assert calls == ["/primary", "/secondary"]
        if failure == "timeout":
            assert cancelled.is_set()
    with sqlite3.connect(tmp_path / "quota.sqlite3") as db:
        assert (
            db.execute("SELECT tokens FROM token_usage").fetchone()[0]
            == Completion(**PAYLOAD).reservation()
        )


async def test_success_settles_actual_usage_and_shares_quota_by_tenant(tmp_path):
    calls = []

    async def upstream(request):
        calls.append(request.url.path)
        return success()

    async with gateway(tmp_path, upstream, limit=160) as (client, _):
        first = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert first.status_code == 200
        assert first.json()["fallback"] is False
        denied = await client.post("/v1/completions", headers=auth("admin"), json=PAYLOAD)
        assert denied.status_code == 429
        assert int(denied.headers["Retry-After"]) > 0
        assert (
            await client.post("/v1/completions", headers=auth(tenant="other"), json=PAYLOAD)
        ).status_code == 200
        assert calls == ["/primary", "/primary"]
    with sqlite3.connect(tmp_path / "quota.sqlite3") as db:
        assert db.execute("SELECT sum(tokens) FROM token_usage").fetchone()[0] == 20


@pytest.mark.parametrize("status,expected_calls", [(429, 2), (500, 1), (401, 1)])
async def test_upstream_failures_are_sanitized(tmp_path, status, expected_calls):
    calls = []

    async def upstream(request):
        calls.append(request.url.path)
        return httpx.Response(status, text="SECRET_TOKEN stack /private/path")

    async with gateway(tmp_path, upstream) as (client, _):
        response = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert response.status_code in (502, 503)
        assert set(response.json()["error"]) == {"code", "message", "request_id"}
        assert "SECRET" not in response.text
        assert len(calls) == expected_calls


async def test_backup_timeout_is_sanitized(tmp_path):
    async def upstream(request):
        if request.url.path == "/primary":
            return httpx.Response(429)
        await asyncio.Event().wait()

    async with gateway(tmp_path, upstream) as (client, app):
        app.state.router.backup_timeout = 0.02
        response = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "providers_unavailable"


async def test_client_cancellation_does_not_start_backup():
    started, cancelled = asyncio.Event(), asyncio.Event()
    paths = []

    async def upstream(request):
        paths.append(request.url.path)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        router = ModelRouter(
            client, Endpoint("http://test/primary", "a"), Endpoint("http://test/secondary", "b")
        )
        task = asyncio.create_task(router.complete(Completion(**PAYLOAD)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
        assert paths == ["/primary"]


async def test_malformed_provider_response(tmp_path):
    async def upstream(request):
        return httpx.Response(200, json={"choices": "private error"})

    async with gateway(tmp_path, upstream) as (client, _):
        response = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "invalid_upstream"


async def test_usage_above_reservation_is_charged(tmp_path):
    async def upstream(request):
        body = json.loads(success().content)
        body["usage"]["total_tokens"] = 1000
        return httpx.Response(200, json=body)

    async with gateway(tmp_path, upstream) as (client, _):
        response = await client.post("/v1/completions", headers=auth(), json=PAYLOAD)
        assert response.status_code == 502
    with sqlite3.connect(tmp_path / "quota.sqlite3") as db:
        assert db.execute("SELECT tokens FROM token_usage").fetchone()[0] == 1000


def test_endpoint_configuration_rejects_untrusted_transport():
    from gateway.app import Settings
    from tests.conftest import SECRET

    with pytest.raises(ValueError):
        Settings(jwt_secret=SECRET)
    with pytest.raises(ValueError):
        Settings(jwt_secret=SECRET, allow_local_http=True, mcp_url="http://remote.invalid/mcp")
