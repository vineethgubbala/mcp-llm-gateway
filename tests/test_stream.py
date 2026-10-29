import asyncio
import json

import httpx
import pytest

from gateway.provider import GatewayError, sse_data, text_deltas
from tests.conftest import auth, gateway
from tests.test_router import PAYLOAD


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


def frame(text):
    return (
        "data: "
        + json.dumps({"choices": [{"delta": {"content": text}}]}, ensure_ascii=False)
        + "\r\n\r\n"
    ).encode()


async def test_sse_handles_every_byte_boundary():
    wire = b": keepalive\r\n\r\n" + frame("hello 🌱") + b"data: [DONE]\r\n\r\n"
    response = httpx.Response(200, stream=ByteStream([bytes([byte]) for byte in wire]))
    assert [delta async for delta in text_deltas(response)] == ["hello 🌱"]


async def test_stream_endpoint_redacts_split_pii_and_closes(tmp_path):
    text = "Contact alice@example.com. SSN 123-45-6789; card 4111 1111 1111 1111. Done."
    chunks = [frame(char) for char in text] + [b"data: [DONE]\n\n"]
    stream = ByteStream(chunks)

    async def upstream(request):
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)

    async with gateway(tmp_path, upstream) as (client, _):
        response = await client.post("/v1/stream", headers=auth(), json=PAYLOAD)
        events = [
            json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")
        ]
        assert "".join(event.get("delta", "") for event in events) == (
            "Contact [REDACTED]. SSN [REDACTED]; card [REDACTED]. Done."
        )
        assert events[-1] == {"done": True}
        assert stream.closed


@pytest.mark.parametrize(
    "chunks",
    [
        [b"data: " + b"x" * 70_000],
        [b"data: invalid-json\n\n"],
        [frame("partial@example")],
        [b'data: {"error":"secret stack trace"}\n\n'],
    ],
)
async def test_failed_stream_does_not_flush_pending_pii(tmp_path, chunks):
    stream = ByteStream(chunks)

    async def upstream(request):
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)

    async with gateway(tmp_path, upstream) as (client, _):
        response = await client.post("/v1/stream", headers=auth(), json=PAYLOAD)
        assert "stream_failed" in response.text
        assert "secret" not in response.text
        assert "partial@example" not in response.text
        assert stream.closed


async def test_delta_available_before_provider_finishes():
    waiting = asyncio.Event()

    class SlowStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield frame("First ")
            await waiting.wait()
            yield b"data: [DONE]\n\n"

    response = httpx.Response(200, stream=SlowStream())
    deltas = text_deltas(response)
    assert await asyncio.wait_for(anext(deltas), 0.2) == "First "
    waiting.set()
    assert [delta async for delta in deltas] == []


async def test_sse_event_size_counts_multiple_data_lines():
    response = httpx.Response(200, stream=ByteStream([b"data: " + b"x" * 1000 + b"\n"] * 70))
    with pytest.raises(GatewayError):
        _ = [item async for item in sse_data(response)]
