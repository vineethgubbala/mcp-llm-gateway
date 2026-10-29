# MCP and LLM Gateway

I used Python 3.12, the official MCP SDK, FastAPI and SQLite. The four tasks share one small package, but the stdio MCP server runs separately from the HTTP gateway. All examples work with local mock services and do not require a paid model account.

| Task | Implementation | Tests |
| --- | --- | --- |
| 1. Customer tools over stdio | `gateway/mcp_server.py` | `tests/test_mcp_stdio.py` |
| 2. MCP authorization proxy | `/mcp` in `gateway/app.py`, `gateway/auth.py` | `tests/test_proxy.py` |
| 3. Streaming redaction | `/v1/stream`, `gateway/redaction.py`, SSE parser in `gateway/provider.py` | `tests/test_redaction.py`, `tests/test_stream.py` |
| 4. Quota and fallback | `/v1/completions`, `gateway/limiter.py`, `gateway/provider.py` | `tests/test_limiter.py`, `tests/test_router.py` |

## Run locally

From the project directory:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
pytest -q
ruff check .
ruff format --check .
```

The lock file records the dependency versions used for this submission. `pyproject.toml` records the direct dependencies. The MCP SDK is deliberately pinned to version 1.26.0 because the validation adapter uses its request handler registration interface.

For Task 1:

```bash
python -m gateway.mcp_server
```

This process waits for MCP messages on stdin. Connect using an MCP client and complete the initialize handshake before calling tools. The test starts a real subprocess and checks every stdout response as JSON. Startup and SDK logs go to stderr.

For Tasks 2 through 4, generate local credentials once:

```bash
python scripts/dev_env.py
source .env.local
```

The script creates a random signing secret and two JWTs valid for 24 hours. It refuses to overwrite an existing file. Keep `.env.local` out of source control. To renew demo credentials, remove that local file and run the script again.

In the first terminal:

```bash
source .venv/bin/activate
python -m uvicorn gateway.mock:app --host 127.0.0.1 --port 9001
```

In the second terminal:

```bash
source .venv/bin/activate
source .env.local
python -m uvicorn gateway.app:create_app --factory --host 127.0.0.1 --port 8000
```

In a third terminal, run the smoke check:

```bash
source .venv/bin/activate
source .env.local
python scripts/smoke.py
```

It checks tool listing, a denied admin call, a normal completion, fallback on 429, fallback at the actual three second deadline, and redaction over a real HTTP stream. It prints first text latency and total stream time for the local fixture, not a provider benchmark.

## Requests to try

List tools as a viewer:

```bash
curl -s http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

The viewer can see the downstream tool listing, including admin tools, as required. Calling an admin tool returns HTTP 403 and the requested JSON RPC error code:

```bash
curl -s http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
```

```json
{"jsonrpc":"2.0","id":2,"error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

Use `$ADMIN_TOKEN` for an allowed call. The proxy forwards the original request body after validation. It does not forward the caller's bearer token to the downstream service.

Stream redacted text:

```bash
curl -N http://127.0.0.1:8000/v1/stream \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"max_tokens":100}'
```

The output uses a small SSE contract: `{"delta":"..."}` events followed by `{"done":true}`. A failure after headers have been sent becomes a sanitized `{"error":...}` event. Only text content is forwarded. Provider metadata and unsupported tool call deltas are not passed through.

Trigger a fallback:

```bash
curl -s http://127.0.0.1:8000/v1/completions \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"demo:429"}],"max_tokens":100}'
```

The mock also accepts `demo:timeout`. Both return a completion with `"fallback":true` from the backup. A normal prompt returns `"fallback":false`.

## Task 1 decisions

I interpreted `CUST-XXXXX` as `CUST-` followed by exactly five ASCII digits. Input models reject extra fields and type coercion. Refund amounts must be finite and greater than zero. JSON numbers such as `1` and `1.0` are both accepted; strings and booleans are rejected. Reasons must contain at least ten characters after trimming.

The SDK 1.x tool decorator converts exceptions into tool results with `isError`. That does not satisfy the requested JSON RPC validation behavior, so I registered the call handler directly. Invalid arguments use `-32602`; malformed JSON uses `-32700`; invalid envelopes use `-32600`; unsupported methods use `-32601`. An unexpected tool failure uses `-32603` without exception text. A valid call for a missing customer returns a tool execution error instead of a protocol error.

Customer data is an in memory fixture. Refunds are explicitly simulated and do not contact a payment service. I did not add a payment workflow or imply that retrying a real refund would be safe without an idempotency key.

## Task 2 decisions

The role comes from a verified JWT, not from decoding an untrusted claim. Verification pins HS256 and checks the signature, expiry, issued time, issuer and audience. The signed subject identifies the tenant. Changing a role in a token without signing it again fails authentication.

Authorization runs before the downstream request. A viewer's `admin_` call cannot reach the mock. Duplicate JSON keys and batch bodies are rejected so the proxy and downstream cannot interpret the same request differently. Tool calls without an ID are discarded as invalid notifications. Initialized and cancellation notifications receive no JSON RPC response.

This is a stateless HTTP JSON RPC proxy for the assessment mock. It supports initialization, ping, tool listing and tool calls. It is not a complete implementation of stateful MCP Streamable HTTP: session ownership, GET event streams and session deletion are outside this example. Those would need explicit tenant binding before being added.

## Task 3 decisions

I avoided running a regex separately on each delta because `alice@exam` and `ple.com` would escape that check. The redactor first holds an incomplete email atom, then checks numeric runs that can contain spaces or hyphens. Ordinary text is released at word boundaries, without waiting for the full completion.

The retained redaction state is at most 256 characters for an email atom and 64 characters for a numeric run. Oversized candidates are replaced and discarded until a boundary. The SSE parser caps each event at 64 KiB and handles JSON frames and UTF 8 characters split across transport chunks. There is no queue that accumulates the full completion, and downstream backpressure naturally pauses upstream reads.

Supported patterns are common ASCII emails, SSNs formatted as `123-45-6789`, and card shaped numeric runs with spaces or hyphens. I conservatively redact runs with at least 13 digits, including longer ambiguous runs, without a Luhn requirement. This can also redact long order numbers. A very long ordinary word can be redacted because its eventual ending cannot safely be predicted.

This is a pattern guardrail, not a general PII classifier. Internationalized or quoted email forms, obfuscated addresses, names, addresses and card digits split by arbitrary prose are outside its coverage. There is an unavoidable latency tradeoff: releasing a possible email prefix immediately would make later redaction impossible. The final candidate is flushed only after a clean provider completion; interrupted streams discard it.

The stream route does not switch providers after text has started. Combining two independent generations would create an incoherent answer. Task 4's fallback behavior is implemented on the completion route.

## Task 4 decisions

The quota is 50,000 tokens per rolling 60 seconds for each signed tenant subject. Token rotations with the same subject share that quota. I used an on disk SQLite database with WAL and `BEGIN IMMEDIATE` so separate workers cannot both read the same available balance and reserve it. Database work runs in threads rather than blocking the event loop.

Before dispatch, a request reserves an upper estimate of input tokens plus its maximum output tokens. The input estimate uses UTF 8 byte length plus 128 framing tokens per message. This is intentionally conservative for text models with byte based tokenizers. It is not an exact tokenizer and should be replaced by a provider specific counter when integrating a model with different tokenization or message framing. Clients cannot supply their own token counts.

A successful primary response replaces the reservation with reported usage. If usage exceeds the reservation, the full reported amount is charged and the response is rejected. Missing usage, fallback, errors and interrupted requests retain the reserved amount because consumption is uncertain. The quota is per logical gateway request; it is not a billing ledger for both provider attempts.

Completed charges expire 60 seconds after settlement. Pending reservations also count against quota and remain valid for the maximum request duration plus the window. This avoids losing an active request at a window boundary. A crashed process leaves a temporary reservation that expires automatically. Expired records are evicted inside the next admission transaction. A rejected request receives `Retry-After` based on enough expiring charges to admit it, not just the earliest record.

The primary has a total three second deadline, including connection and body reading. On 429, timeout or a transport failure, the router cancels that attempt and makes one backup call with a ten second deadline. Client cancellation propagates and does not start a backup. Other HTTP failures, including authentication errors, are sanitized without automatic retry.

Errors have this shape:

```json
{"error":{"code":"providers_unavailable","message":"No model provider is available","request_id":"..."}}
```

I do not return upstream error bodies, credentials or stack traces. The request ID can be used to correlate safe server logs.

## Deployment and troubleshooting

| Setting | Purpose |
| --- | --- |
| `JWT_SECRET` | Required signing secret, at least 32 bytes |
| `DB_PATH` | Persistent local SQLite path, default `data/quota.sqlite3` |
| `MCP_URL`, `MCP_SERVICE_TOKEN` | Fixed downstream address and separate service identity |
| `PRIMARY_URL`, `PRIMARY_MODEL`, `PRIMARY_API_KEY` | Primary chat completion compatible endpoint |
| `SECONDARY_URL`, `SECONDARY_MODEL`, `SECONDARY_API_KEY` | Backup endpoint |
| `ALLOW_LOCAL_HTTP=1` | Allow HTTP only for loopback demo addresses |
| `UPSTREAM_CA_FILE` | Optional PEM trust bundle for private upstream certificates |
| `UPSTREAM_CLIENT_CERT`, `UPSTREAM_CLIENT_KEY` | Optional client certificate and key for upstream mTLS |

Production upstream URLs must use HTTPS. Certificate validation stays enabled and redirects are disabled. Upstream addresses come from server configuration, never from request bodies. For the local demo, the generated environment explicitly allows loopback HTTP. The gateway expects chat completion compatible provider APIs; other provider formats need an adapter.

For a deployment behind an identity aware network, I would check the following in order:

1. Verify the request reaches `/healthz`. A DNS or connection failure occurs before JWT or tool authorization. Confirm service DNS, port and allowed egress from the gateway process.
2. Check TLS trust and hostname matching. For a private CA, set `UPSTREAM_CA_FILE`. For mTLS, confirm the certificate identity, expiry and paired private key. Do not resolve trust errors by disabling verification.
3. Separate HTTP 401 from 403. A 401 indicates token verification failed. A 403 with `Unauthorized Tool Call` means identity was accepted but that role cannot call the tool.
4. Verify the downstream service credential separately. The caller's token is intentionally not passed through. HTTP proxy environment variables are also ignored, so configure any required enterprise proxy explicitly in the HTTP client.
5. If a stream arrives all at once, check response buffering and idle timeouts on the ingress proxy. The application sends `X-Accel-Buffering: no` and `Cache-Control: no-cache`, but the ingress must respect them.
6. If quota storage is unavailable, check write access to the database directory, including SQLite WAL and shared memory files. Multiple local workers can share it. Do not place WAL files on a shared network filesystem or assume this implements a distributed quota across hosts.

The mock service has no authentication and should remain bound to loopback. A production deployment also needs ingress TLS, managed signing keys, audit retention and resource limits. Those are deployment concerns rather than features implemented by this local assessment.

## References

Official SDK: https://github.com/modelcontextprotocol/python-sdk

MCP transport specification: https://modelcontextprotocol.io/specification/2025-11-25/basic/transports

SQLite transaction isolation: https://www.sqlite.org/isolation.html

HTTPX async streaming: https://www.python-httpx.org/async/
