"""Generate local credentials. Existing files are never overwritten."""

import secrets
import time
from pathlib import Path

import jwt

secret = secrets.token_urlsafe(48)
now = int(time.time())
settings = {"JWT_SECRET": secret, "ALLOW_LOCAL_HTTP": "1"}
for role in ("viewer", "admin"):
    settings[role.upper() + "_TOKEN"] = jwt.encode(
        {
            "sub": "demo-tenant",
            "role": role,
            "iat": now,
            "exp": now + 86400,
            "aud": "gateway",
            "iss": "gateway-demo",
        },
        secret,
        algorithm="HS256",
    )
path = Path(".env.local")
with path.open("x") as output:
    path.chmod(0o600)
    for key, value in settings.items():
        output.write(f"export {key}='{value}'\n")
print("Created .env.local with demo tokens valid for 24 hours.")
