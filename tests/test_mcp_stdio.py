import asyncio
import json
import sys

import pytest


@pytest.fixture
async def server():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "gateway.mcp_server",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    yield process
    process.stdin.close()
    await asyncio.wait_for(process.wait(), 5)
    stderr = await process.stderr.read()
    assert b"Customer MCP server ready" in stderr
    assert process.returncode == 0, stderr.decode()


async def send(process, body):
    data = body if isinstance(body, str) else json.dumps(body)
    process.stdin.write((data + "\n").encode())
    await process.stdin.drain()


async def receive(process):
    line = await asyncio.wait_for(process.stdout.readline(), 5)
    body = json.loads(line)
    assert body["jsonrpc"] == "2.0"
    return body


async def initialize(process):
    await send(
        process,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
        },
    )
    assert "result" in await receive(process)
    await send(process, {"jsonrpc": "2.0", "method": "notifications/initialized"})


async def test_protocol_and_validation_over_real_stdio(server):
    await initialize(server)
    await send(server, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = (await receive(server))["result"]["tools"]
    assert {tool["name"] for tool in tools} == {"get_customer_record", "trigger_refund"}
    assert all(tool["inputSchema"]["additionalProperties"] is False for tool in tools)
    good = {"customer_id": "CUST-12345", "amount": 12.5, "reason": "Duplicate transaction"}
    invalid = [
        {**good, "customer_id": "CUST-1234"},
        {**good, "customer_id": "CUST-12345\n"},
        {**good, "customer_id": "CUST-１２３４５"},
        {**good, "customer_id": 12345},
        {**good, "amount": 0},
        {**good, "amount": -1},
        {**good, "amount": True},
        {**good, "amount": "1.5"},
        {**good, "reason": "short"},
        {**good, "reason": " " * 20},
        {**good, "extra": "ignored?"},
        {"customer_id": "CUST-12345"},
    ]
    for i, arguments in enumerate(invalid, 10):
        await send(
            server,
            {
                "jsonrpc": "2.0",
                "id": i,
                "method": "tools/call",
                "params": {
                    "name": "trigger_refund",
                    "arguments": arguments,
                },
            },
        )
        response = await receive(server)
        assert response["id"] == i
        assert response["error"]["code"] == -32602
        assert "result" not in response
    for name, arguments in [
        ("trigger_refund", good),
        ("get_customer_record", {"customer_id": "CUST-12345"}),
    ]:
        await send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": arguments,
                },
            },
        )
        assert (await receive(server))["result"]["isError"] is False
    await send(
        server,
        {
            "jsonrpc": "2.0",
            "id": 31,
            "method": "tools/call",
            "params": {
                "name": "get_customer_record",
                "arguments": {"customer_id": "CUST-99999"},
            },
        },
    )
    assert (await receive(server))["result"]["isError"] is True
    # Every stdout line, including errors, must parse as a JSON RPC message.
    for raw, code in [
        ("{", -32700),
        ("[]", -32600),
        ('{"jsonrpc":"2.0","id":90,"method":"unknown"}', -32601),
        ('{"jsonrpc":"2.0","id":91,"method":"tools/call","params":{}}', -32602),
        (
            '{"jsonrpc":"2.0","id":92,"method":"tools/call","params":{"name":"x","arguments":{"amount":NaN}}}',
            -32700,
        ),
    ]:
        await send(server, raw)
        assert (await receive(server))["error"]["code"] == code
