import json

import httpx
import pytest

from tests.conftest import auth, gateway


def call(name="admin_reset_key", request_id=7):
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": {}},
    }


async def test_authorization_blocks_before_forwarding(tmp_path):
    received = []

    async def downstream(request):
        received.append(request)
        body = json.loads(request.content)
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": body["id"], "result": {"ok": True}}
        )

    async with gateway(tmp_path, downstream) as (client, _):
        denied = await client.post("/mcp", headers=auth(), json=call())
        assert denied.status_code == 403
        assert denied.json()["error"] == {"code": -32001, "message": "Unauthorized Tool Call"}
        assert denied.json()["id"] == 7
        assert received == []
        for headers in ({}, auth(secret="forged" * 8), auth(expiry=-1), auth(role="owner")):
            assert (await client.post("/mcp", headers=headers, json=call())).status_code == 401
        assert received == []
        for headers, payload in [
            (auth("admin"), call()),
            (auth(), call("get_customer_record")),
            (auth(), {"jsonrpc": "2.0", "id": "list", "method": "tools/list"}),
        ]:
            response = await client.post("/mcp", headers=headers, json=payload)
            assert response.status_code == 200
            assert json.loads(received[-1].content) == payload
            assert "authorization" not in received[-1].headers


@pytest.mark.parametrize(
    "body,code",
    [
        ("{", -32700),
        ("[]", -32600),
        ('{"jsonrpc":"1.0"}', -32600),
        ('{"jsonrpc":"2.0","id":true,"method":"tools/list"}', -32600),
        ('{"jsonrpc":"2.0","id":1,"method":"tools/call","params":[]}', -32602),
        ('{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":1}}', -32602),
        (
            '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"safe","name":"admin_reset_key"}}',
            -32700,
        ),
    ],
)
async def test_invalid_requests_never_reach_downstream(tmp_path, body, code):
    async def downstream(request):
        pytest.fail("Invalid request was forwarded")

    async with gateway(tmp_path, downstream) as (client, _):
        response = await client.post("/mcp", headers=auth(), content=body)
        assert response.json()["error"]["code"] == code


async def test_notification_cannot_bypass_authorization(tmp_path):
    async def downstream(request):
        pytest.fail("Call notification was forwarded")

    body = call()
    del body["id"]
    async with gateway(tmp_path, downstream) as (client, _):
        response = await client.post("/mcp", headers=auth(), json=body)
        assert response.status_code == 202
        assert response.content == b""


async def test_tools_list_response_is_transparent(tmp_path):
    result = {"jsonrpc": "2.0", "id": 12, "result": {"tools": [{"name": "admin_reset_key"}]}}

    async def downstream(request):
        return httpx.Response(200, json=result)

    async with gateway(tmp_path, downstream) as (client, _):
        response = await client.post(
            "/mcp", headers=auth(), json={"jsonrpc": "2.0", "id": 12, "method": "tools/list"}
        )
        assert response.json() == result
