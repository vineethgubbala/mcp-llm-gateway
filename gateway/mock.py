"""Local fixtures only. Never expose this process to an untrusted network."""

import asyncio
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

app = FastAPI()


@app.post("/mcp")
async def mcp(request: Request):
    body = await request.json()
    if "id" not in body:
        return Response(status_code=202)
    if body["method"] == "tools/list":
        result = {
            "tools": [
                {"name": name, "description": "Local demo tool", "inputSchema": {"type": "object"}}
                for name in ["get_customer_record", "admin_reset_key"]
            ]
        }
    elif body["method"] == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock", "version": "1.0"},
        }
    elif body["method"] == "ping":
        result = {}
    else:
        result = {"content": [{"type": "text", "text": "Mock tool completed"}]}
    return {"jsonrpc": "2.0", "id": body["id"], "result": result}


async def completion(request, primary):
    body = await request.json()
    prompt = body["messages"][-1]["content"]
    if primary and prompt == "demo:429":
        return JSONResponse({"private_error": "Do not forward this"}, status_code=429)
    if primary and prompt == "demo:timeout":
        await asyncio.sleep(3.5)
    text = "Contact demo@example.com. SSN 123-45-6789. Card 4111 1111 1111 1111. Done."
    if body.get("stream"):

        async def chunks():
            for offset in range(0, len(text), 3):
                frame = {"choices": [{"delta": {"content": text[offset : offset + 3]}}]}
                yield "data: " + json.dumps(frame) + "\n\n"
                await asyncio.sleep(0.02)
            yield "data: [DONE]\n\n"

        return StreamingResponse(chunks(), media_type="text/event-stream")
    return {"choices": [{"message": {"content": "Mock completion"}}], "usage": {"total_tokens": 25}}


@app.post("/primary")
async def primary(request: Request):
    return await completion(request, True)


@app.post("/secondary")
async def secondary(request: Request):
    return await completion(request, False)
