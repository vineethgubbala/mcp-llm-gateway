"""Exercise both running local services without printing credentials."""

import asyncio
import json
import os
import time

import httpx


async def main():
    headers = {"Authorization": "Bearer " + os.environ["VIEWER_TOKEN"]}
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=15, trust_env=False
    ) as client:
        response = await client.post(
            "/mcp",
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
            },
        )
        assert response.status_code == 200 and response.json()["result"]["tools"]
        response = await client.post(
            "/mcp",
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "admin_reset_key", "arguments": {}},
            },
        )
        assert response.status_code == 403 and response.json()["error"]["code"] == -32001
        for prompt, expected_fallback in [
            ("hello", False),
            ("demo:429", True),
            ("demo:timeout", True),
        ]:
            start = time.monotonic()
            response = await client.post(
                "/v1/completions",
                headers=headers,
                json={
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 100,
                },
            )
            elapsed = time.monotonic() - start
            assert response.status_code == 200, response.text
            assert response.json()["fallback"] is expected_fallback
            if prompt == "demo:timeout":
                assert 2.9 <= elapsed < 5.0, elapsed
            print(f"Completion {prompt}: passed ({elapsed:.3f}s)")
        start, first, done = time.monotonic(), None, False
        pieces = []
        async with client.stream(
            "POST",
            "/v1/stream",
            headers=headers,
            json={
                "messages": [{"role": "user", "content": "stream"}],
                "max_tokens": 100,
            },
        ) as response:
            assert response.status_code == 200
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    item = json.loads(line[6:])
                    assert "error" not in item
                    if "delta" in item:
                        first = first if first is not None else time.monotonic() - start
                        pieces.append(item["delta"])
                    done = done or item.get("done", False)
        total = time.monotonic() - start
        assert "".join(pieces) == "Contact [REDACTED]. SSN [REDACTED]. Card [REDACTED]. Done."
        assert done and first is not None and first < total
        print(f"Redacted stream: passed (first text {first:.3f}s, complete {total:.3f}s)")


asyncio.run(main())
