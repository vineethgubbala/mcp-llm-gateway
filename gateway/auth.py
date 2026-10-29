from dataclasses import dataclass

import jwt


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class Identity:
    tenant: str
    role: str


def authenticate(header: str | None, secret: str) -> Identity:
    parts = (header or "").split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise AuthError()
    try:
        claims = jwt.decode(
            parts[1],
            secret,
            algorithms=["HS256"],
            audience="gateway",
            issuer="gateway-demo",
            options={"require": ["exp", "iat", "sub", "role"]},
        )
        if claims["role"] not in ("admin", "viewer"):
            raise AuthError()
        if not isinstance(claims["sub"], str) or not 1 <= len(claims["sub"]) <= 128:
            raise AuthError()
        return Identity(tenant=claims["sub"], role=claims["role"])
    except (jwt.PyJWTError, TypeError, KeyError):
        raise AuthError() from None
