import json


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Nonfinite JSON number")


def loads(data):
    return json.loads(data, object_pairs_hook=_pairs, parse_constant=_constant)


def rpc_error(request_id, code, message):
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def valid_id(value):
    return isinstance(value, str) or type(value) is int


def valid_request(body):
    return (
        isinstance(body, dict)
        and body.get("jsonrpc") == "2.0"
        and isinstance(body.get("method"), str)
        and ("id" not in body or valid_id(body["id"]))
        and not ({"result", "error"} & body.keys())
    )
