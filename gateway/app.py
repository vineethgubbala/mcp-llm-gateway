import asyncio
import logging
import os
import sqlite3
import ssl
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import ValidationError

from gateway.auth import AuthError, authenticate
from gateway.limiter import QuotaExceeded, TokenLimiter
from gateway.provider import (
    Completion,
    Endpoint,
    GatewayError,
    ModelRouter,
    bounded_json,
    event,
    text_deltas,
)
from gateway.redaction import StreamRedactor
from gateway.wire import loads, rpc_error, valid_request

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    jwt_secret: str
    db_path: str = "data/quota.sqlite3"
    mcp_url: str = "http://127.0.0.1:9001/mcp"
    mcp_token: str = ""
    primary: Endpoint = Endpoint("http://127.0.0.1:9001/primary", "demo-primary")
    secondary: Endpoint = Endpoint("http://127.0.0.1:9001/secondary", "demo-backup")
    allow_local_http: bool = False

    def __post_init__(self):
        if len(self.jwt_secret.encode()) < 32:
            raise ValueError("JWT_SECRET must contain at least 32 bytes")
        for url in (self.mcp_url, self.primary.url, self.secondary.url):
            parsed = urlparse(url)
            local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
            if (
                not parsed.hostname
                or parsed.username
                or parsed.password
                or (
                    parsed.scheme != "https"
                    and not (self.allow_local_http and local and parsed.scheme == "http")
                )
            ):
                raise ValueError("Upstreams require HTTPS; local HTTP must be explicitly enabled")

    @classmethod
    def from_env(cls):
        return cls(
            jwt_secret=os.environ["JWT_SECRET"],
            db_path=os.getenv("DB_PATH", "data/quota.sqlite3"),
            mcp_url=os.getenv("MCP_URL", "http://127.0.0.1:9001/mcp"),
            mcp_token=os.getenv("MCP_SERVICE_TOKEN", ""),
            primary=Endpoint(
                os.getenv("PRIMARY_URL", "http://127.0.0.1:9001/primary"),
                os.getenv("PRIMARY_MODEL", "demo-primary"),
                os.getenv("PRIMARY_API_KEY", ""),
            ),
            secondary=Endpoint(
                os.getenv("SECONDARY_URL", "http://127.0.0.1:9001/secondary"),
                os.getenv("SECONDARY_MODEL", "demo-backup"),
                os.getenv("SECONDARY_API_KEY", ""),
            ),
            allow_local_http=os.getenv("ALLOW_LOCAL_HTTP") == "1",
        )


async def read_body(request, maximum=65_536):
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > maximum:
            raise GatewayError("request_too_large", "Request body is too large", 413)
    return bytes(body)


def error_payload(code, message, request_id):
    return {"error": {"code": code, "message": message, "request_id": request_id}}


def create_app(settings=None, client=None, limiter=None):
    settings = settings or Settings.from_env()
    limiter = limiter or TokenLimiter(settings.db_path)
    owned_client = client is None
    tls = ssl.create_default_context(cafile=os.getenv("UPSTREAM_CA_FILE"))
    cert, key = os.getenv("UPSTREAM_CLIENT_CERT"), os.getenv("UPSTREAM_CLIENT_KEY")
    if bool(cert) != bool(key):
        raise ValueError("Set both UPSTREAM_CLIENT_CERT and UPSTREAM_CLIENT_KEY")
    if cert:
        tls.load_cert_chain(cert, key)
    client = client or httpx.AsyncClient(
        verify=tls,
        timeout=httpx.Timeout(10, connect=3),
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    router = ModelRouter(client, settings.primary, settings.secondary)

    @asynccontextmanager
    async def lifespan(app):
        await limiter.initialize()
        try:
            yield
        finally:
            if owned_client:
                await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.router = router

    @app.middleware("http")
    async def request_context(request, call_next):
        request.state.request_id = uuid.uuid4().hex
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    @app.exception_handler(GatewayError)
    async def gateway_error(request, exc):
        return JSONResponse(
            error_payload(exc.code, exc.message, request.state.request_id), status_code=exc.status
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return JSONResponse(
            error_payload("invalid_request", "Invalid request", request.state.request_id),
            status_code=400,
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request, exc):
        # Never log tokens, prompts, upstream response bodies, or exception text.
        log.error("Request failed id=%s type=%s", request.state.request_id, type(exc).__name__)
        return JSONResponse(
            error_payload(
                "internal_error", "Request could not be completed", request.state.request_id
            ),
            status_code=500,
        )

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.post("/mcp")
    async def mcp_proxy(request: Request):
        try:
            identity = authenticate(request.headers.get("authorization"), settings.jwt_secret)
        except AuthError:
            return JSONResponse(
                rpc_error(None, -32001, "Unauthorized"),
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        try:
            raw = await read_body(request)
        except GatewayError:
            return JSONResponse(
                rpc_error(None, -32600, "Request body is too large"), status_code=413
            )
        try:
            body = loads(raw)
        except (ValueError, UnicodeError):
            return JSONResponse(rpc_error(None, -32700, "Parse error"), status_code=400)
        if not valid_request(body):
            return JSONResponse(rpc_error(None, -32600, "Invalid Request"), status_code=400)
        request_id = body.get("id")
        method = body["method"]
        params = body.get("params", {})
        if "id" not in body and not isinstance(params, dict):
            return Response(status_code=202)
        if not isinstance(params, dict):
            return JSONResponse(rpc_error(request_id, -32602, "Invalid params"), status_code=400)
        if "id" not in body:
            if method not in {"notifications/initialized", "notifications/cancelled"}:
                return Response(status_code=202)
        elif method not in {"initialize", "ping", "tools/list", "tools/call"}:
            return JSONResponse(rpc_error(request_id, -32601, "Method not found"))
        if method == "tools/call":
            name = params.get("name")
            if not isinstance(name, str) or not name:
                return JSONResponse(
                    rpc_error(request_id, -32602, "Invalid params"), status_code=400
                )
            if not isinstance(params.get("arguments", {}), dict):
                return JSONResponse(
                    rpc_error(request_id, -32602, "Invalid params"), status_code=400
                )
            if name.startswith("admin_") and identity.role != "admin":
                return JSONResponse(
                    rpc_error(request_id, -32001, "Unauthorized Tool Call"), status_code=403
                )
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        # Caller credentials are only for this gateway. Downstream has its own identity.
        if settings.mcp_token:
            headers["Authorization"] = f"Bearer {settings.mcp_token}"
        if "mcp-protocol-version" in request.headers:
            headers["MCP-Protocol-Version"] = request.headers["mcp-protocol-version"]
        try:
            async with asyncio.timeout(5):
                async with client.stream(
                    "POST", settings.mcp_url, content=raw, headers=headers
                ) as response:
                    if "id" not in body:
                        if response.status_code not in (200, 202, 204):
                            raise ValueError()
                        return Response(status_code=202)
                    result = await bounded_json(response)
                    if (
                        not isinstance(result, dict)
                        or result.get("jsonrpc") != "2.0"
                        or result.get("id") != request_id
                        or ("result" in result) == ("error" in result)
                    ):
                        raise ValueError()
                    return JSONResponse(result, status_code=response.status_code)
        except (TimeoutError, httpx.HTTPError, GatewayError, ValueError):
            return JSONResponse(
                rpc_error(request_id, -32603, "Downstream unavailable"), status_code=502
            )

    async def prepare(request, deadline):
        try:
            identity = authenticate(request.headers.get("authorization"), settings.jwt_secret)
        except AuthError:
            raise GatewayError("unauthorized", "Valid bearer token required", 401) from None
        raw = await read_body(request)
        try:
            completion = Completion.model_validate(loads(raw))
        except (ValueError, UnicodeError, ValidationError):
            raise GatewayError("invalid_request", "Invalid completion request", 400) from None
        estimate = completion.reservation()
        if estimate > limiter.limit:
            raise GatewayError("request_too_large", "Request exceeds the token budget", 400)
        try:
            reservation = await limiter.reserve(identity.tenant, estimate, deadline)
        except QuotaExceeded as exc:
            return JSONResponse(
                error_payload(
                    "rate_limited", "Tenant token limit exceeded", request.state.request_id
                ),
                status_code=429,
                headers={"Retry-After": str(exc.retry_after)},
            )
        except sqlite3.Error:
            raise GatewayError("quota_unavailable", "Token quota is unavailable", 503) from None
        return completion, reservation

    async def settle(reservation, usage=None):
        try:
            await limiter.settle(reservation, usage)
        except sqlite3.Error:
            # The original durable reservation remains charged until its expiry.
            log.error("Quota settlement failed")

    @app.post("/v1/completions")
    async def complete(request: Request):
        prepared = await prepare(request, deadline=15)
        if isinstance(prepared, Response):
            return prepared
        completion, reservation = prepared
        try:
            result, fallback = await router.complete(completion)
        except GatewayError:
            await settle(reservation)
            raise
        # Unknown consumption on a failed primary must not free quota.
        actual = result["usage"] if not fallback else None
        if actual is not None and actual > completion.reservation():
            await settle(reservation, actual)
            raise GatewayError("invalid_usage", "Provider exceeded the reserved token budget")
        await settle(reservation, actual)
        return {**result, "fallback": fallback, "request_id": request.state.request_id}

    @app.post("/v1/stream")
    async def stream(request: Request):
        prepared = await prepare(request, deadline=65)
        if isinstance(prepared, Response):
            return prepared
        completion, reservation = prepared
        upstream = client.build_request(
            "POST",
            settings.primary.url,
            headers=settings.primary.headers(),
            json={**completion.model_dump(), "model": settings.primary.model, "stream": True},
        )
        try:
            async with asyncio.timeout(3):
                response = await client.send(upstream, stream=True)
        except (TimeoutError, httpx.HTTPError):
            await settle(reservation)
            raise GatewayError("upstream_unavailable", "Provider is unavailable", 503) from None
        if response.status_code != 200 or "text/event-stream" not in response.headers.get(
            "content-type", ""
        ):
            await response.aclose()
            await settle(reservation)
            raise GatewayError("upstream_error", "Provider could not start the stream")

        async def generate():
            redactor = StreamRedactor()
            try:
                async with asyncio.timeout(60):
                    async for delta in text_deltas(response):
                        cleaned = redactor.feed(delta)
                        if cleaned:
                            yield event({"delta": cleaned})
                    tail = redactor.finish()
                    if tail:
                        yield event({"delta": tail})
                    yield event({"done": True})
            except (GatewayError, TimeoutError, httpx.HTTPError):
                # Do not flush an unfinished candidate when a stream fails.
                yield event(
                    error_payload(
                        "stream_failed", "Provider stream interrupted", request.state.request_id
                    )
                )
            finally:
                # Shield cleanup from ASGI's disconnect cancellation scope.
                import anyio

                with anyio.CancelScope(shield=True):
                    await response.aclose()
                    await settle(reservation)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
