import asyncio
import json
from dataclasses import dataclass

import httpx
from pydantic import BaseModel, ConfigDict, Field

from gateway.wire import loads


class Message(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    role: str = Field(pattern=r"^(system|user|assistant)$")
    content: str = Field(min_length=1, max_length=32_000)


class Completion(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    messages: list[Message] = Field(min_length=1, max_length=64)
    max_tokens: int = Field(default=256, ge=1, le=8192)

    def reservation(self):
        # Conservative input bound for byte BPE text models, plus framing overhead.
        # Never accept a client supplied token count.
        return sum(len(m.content.encode("utf-8")) + 128 for m in self.messages) + self.max_tokens


class GatewayError(Exception):
    def __init__(self, code, message, status=502):
        self.code, self.message, self.status = code, message, status


class RetryableProviderError(Exception):
    pass


@dataclass(frozen=True)
class Endpoint:
    url: str
    model: str
    api_key: str = ""

    def headers(self):
        return {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}


async def bounded_json(response, maximum=1_048_576):
    data = bytearray()
    async for chunk in response.aiter_bytes():
        data.extend(chunk)
        if len(data) > maximum:
            raise GatewayError("invalid_upstream", "Provider response was too large")
    try:
        return loads(data)
    except (ValueError, UnicodeError):
        raise GatewayError("invalid_upstream", "Provider returned an invalid response") from None


class ModelRouter:
    def __init__(self, client, primary, secondary, primary_timeout=3.0, backup_timeout=10.0):
        self.client, self.primary, self.secondary = client, primary, secondary
        self.primary_timeout, self.backup_timeout = primary_timeout, backup_timeout

    async def _attempt(self, endpoint, completion, deadline):
        try:
            async with asyncio.timeout(deadline):
                async with self.client.stream(
                    "POST",
                    endpoint.url,
                    headers=endpoint.headers(),
                    json={**completion.model_dump(), "model": endpoint.model, "stream": False},
                ) as response:
                    if response.status_code == 429:
                        raise RetryableProviderError()
                    if response.status_code != 200:
                        raise GatewayError(
                            "upstream_error", "Provider could not complete the request"
                        )
                    body = await bounded_json(response)
                    try:
                        text = body["choices"][0]["message"]["content"]
                        usage = body.get("usage", {}).get("total_tokens")
                        if not isinstance(text, str):
                            raise ValueError()
                        if usage is not None and (type(usage) is not int or usage < 0):
                            raise ValueError()
                    except (KeyError, IndexError, TypeError, AttributeError, ValueError):
                        raise GatewayError(
                            "invalid_upstream", "Provider returned an invalid response"
                        ) from None
                    return {"text": text, "model": endpoint.model, "usage": usage}
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError):
            raise RetryableProviderError() from None

    async def complete(self, completion):
        try:
            return await self._attempt(self.primary, completion, self.primary_timeout), False
        except RetryableProviderError:
            try:
                return await self._attempt(self.secondary, completion, self.backup_timeout), True
            except RetryableProviderError:
                raise GatewayError(
                    "providers_unavailable", "No model provider is available", 503
                ) from None


async def sse_data(response, maximum=65_536):
    """Parse fragmented UTF 8 SSE without unbounded aiter_lines buffers."""
    pending = bytearray()
    fields = []
    event_size = 0
    async for chunk in response.aiter_bytes():
        # Consume a large transport chunk in bounded slices without buffering it twice.
        for offset in range(0, len(chunk), 4096):
            pending.extend(chunk[offset : offset + 4096])
            while b"\n" in pending:
                raw, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                line = raw.rstrip(b"\r")
                event_size += len(line)
                if event_size > maximum:
                    raise GatewayError("invalid_stream", "Provider stream event was too large")
                if not line:
                    if fields:
                        try:
                            yield b"\n".join(fields).decode("utf-8")
                        except UnicodeError:
                            raise GatewayError(
                                "invalid_stream", "Invalid provider stream"
                            ) from None
                    fields, event_size = [], 0
                elif line.startswith(b"data:"):
                    fields.append(line[5:].removeprefix(b" "))
            if len(pending) + event_size > maximum:
                raise GatewayError("invalid_stream", "Provider stream event was too large")
    if pending or fields:
        raise GatewayError("invalid_stream", "Provider stream ended unexpectedly")


async def text_deltas(response):
    async for data in sse_data(response):
        if data == "[DONE]":
            return
        try:
            body = loads(data)
            choices = body["choices"]
            if not isinstance(choices, list) or len(choices) > 1:
                raise ValueError()
            if not choices:  # Usage event.
                continue
            delta = choices[0]["delta"]
            if "tool_calls" in delta or "function_call" in delta:
                raise ValueError()
            content = delta.get("content")
            if content is not None and not isinstance(content, str):
                raise ValueError()
        except (ValueError, UnicodeError, KeyError, IndexError, TypeError, AttributeError):
            raise GatewayError("invalid_stream", "Invalid provider stream") from None
        if content:
            yield content
    raise GatewayError("invalid_stream", "Provider stream ended unexpectedly")


def event(payload):
    return "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
