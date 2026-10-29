import asyncio
import json
import logging
import sys
from typing import Annotated

import anyio
from mcp import McpError, types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from gateway.wire import loads, rpc_error, valid_request

log = logging.getLogger(__name__)
server = Server("customer-tools")
CustomerId = Annotated[str, Field(pattern=r"^CUST-[0-9]{5}$", min_length=10, max_length=10)]


class CustomerInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    customer_id: CustomerId


class RefundInput(CustomerInput):
    amount: Annotated[float, Field(gt=0, allow_inf_nan=False)]
    reason: Annotated[str, Field(min_length=10, max_length=2000)]

    @field_validator("reason")
    @classmethod
    def meaningful_reason(cls, value):
        if len(value.strip()) < 10:
            raise ValueError("Reason must contain at least 10 characters after trimming")
        return value.strip()


MODELS = {"get_customer_record": CustomerInput, "trigger_refund": RefundInput}
CUSTOMERS = {"CUST-12345": {"customer_id": "CUST-12345", "name": "Demo Customer"}}


@server.list_tools()
async def list_tools():
    return [
        types.Tool(
            name=name,
            description=(
                "Read a demo customer"
                if name == "get_customer_record"
                else "Simulate a refund; no money is moved"
            ),
            inputSchema=model.model_json_schema(),
        )
        for name, model in MODELS.items()
    ]


async def call_tool(request: types.CallToolRequest):
    try:
        model = MODELS.get(request.params.name)
        if model is None:
            raise McpError(types.ErrorData(code=-32602, message="Unknown tool"))
        try:
            arguments = model.model_validate(request.params.arguments or {})
        except ValidationError as exc:
            fields = sorted({str(e["loc"][0]) for e in exc.errors() if e["loc"]})
            raise McpError(
                types.ErrorData(code=-32602, message="Invalid tool arguments: " + ", ".join(fields))
            ) from None
        customer = CUSTOMERS.get(arguments.customer_id)
        if customer is None:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text="Customer not found")],
                    isError=True,
                )
            )
        data = (
            customer
            if isinstance(arguments, CustomerInput) and not isinstance(arguments, RefundInput)
            else {"status": "simulated", **arguments.model_dump()}
        )
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps(data))],
                structuredContent=data,
            )
        )
    except McpError:
        raise
    except Exception:
        log.error("Tool execution failed")
        raise McpError(types.ErrorData(code=-32603, message="Internal error")) from None


# SDK 1.x's call_tool decorator catches McpError and returns isError instead.
# Register directly so invalid arguments reach the dispatcher as JSON RPC errors.
server.request_handlers[types.CallToolRequest] = call_tool


class LockedOutput:
    def __init__(self, output):
        self.output = output
        self.lock = anyio.Lock()

    async def write(self, line):
        async with self.lock:
            await self.output.write(line)
            await self.output.flush()

    async def flush(self):
        pass


class CheckedInput:
    """Check framing before SDK 1.x loses the original malformed message."""

    def __init__(self, source, output):
        self.source = source
        self.output = output

    async def __aiter__(self):
        async for line in self.source:
            error = None
            try:
                body = loads(line)
            except (ValueError, UnicodeError):
                error = rpc_error(None, -32700, "Parse error")
            else:
                if not valid_request(body):
                    error = rpc_error(None, -32600, "Invalid Request")
                elif "id" not in body:
                    # Invalid notifications never receive a response.
                    try:
                        types.ClientNotification.model_validate(body)
                    except ValidationError:
                        continue
                elif body["method"] not in {"initialize", "ping", "tools/list", "tools/call"}:
                    error = rpc_error(body["id"], -32601, "Method not found")
                else:
                    try:
                        types.ClientRequest.model_validate(body)
                    except ValidationError:
                        error = rpc_error(body["id"], -32602, "Invalid params")
            if error:
                await self.output.write(json.dumps(error) + "\n")
            else:
                yield line


async def run():
    output = LockedOutput(anyio.wrap_file(sys.stdout))
    source = CheckedInput(anyio.wrap_file(sys.stdin), output)
    log.info("Customer MCP server ready")
    async with stdio_server(stdin=source, stdout=output) as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    asyncio.run(run())


if __name__ == "__main__":
    main()
