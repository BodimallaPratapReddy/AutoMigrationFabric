"""Validate user answers at both HTTP and direct Temporal signal boundaries."""

import math


def validate_reply(request: dict, payload: dict) -> None:
    if payload.get("request_id") != request["id"] or not payload.get("actor"):
        raise ValueError("Reply must identify the current request and the reviewer")
    if request["kind"] == "PERMISSION":
        if payload.get("decision") not in {"once", "reject"}:
            raise ValueError("Choose allow once or reject")
        return
    answer = payload.get("answer")
    if not isinstance(answer, dict):
        raise ValueError("Form answer must be an object")
    fields = {field["key"]: field for field in request.get("fields", [])}
    if set(answer) - set(fields):
        raise ValueError("Answer contains unknown fields")
    for key, field in fields.items():
        if key not in answer:
            if field.get("required", True):
                raise ValueError(f"Answer required: {key}")
            continue
        value = answer[key]
        kind = field.get("type")
        valid = ((kind == "string" and isinstance(value, str))
                 or (kind == "boolean" and type(value) is bool)
                 or (kind in {"number", "integer"} and type(value) in {int, float}
                     and math.isfinite(value) and (kind != "integer" or int(value) == value))
                 or (kind == "multiselect" and isinstance(value, list)
                     and all(isinstance(x, str) for x in value)))
        if not valid:
            raise ValueError(f"Invalid answer type: {key}")
        if isinstance(value, (str, list)):
            lower = field.get("minLength" if isinstance(value, str) else "minItems", 0)
            upper = field.get("maxLength" if isinstance(value, str) else "maxItems", 16000)
            if not lower <= len(value) <= upper:
                raise ValueError(f"Answer length outside allowed range: {key}")
        if type(value) in {int, float}:
            if value < field.get("minimum", -math.inf) or value > field.get("maximum", math.inf):
                raise ValueError(f"Answer outside allowed range: {key}")
        options = {option["value"] for option in field.get("options", [])}
        if options and not field.get("custom", False):
            if any(x not in options for x in (value if isinstance(value, list) else [value])):
                raise ValueError(f"Select an offered option: {key}")
