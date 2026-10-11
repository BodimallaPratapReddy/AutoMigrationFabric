"""SQL-only repair boundaries; deterministic and safe to import in workflows."""
import re

SQL_ERRORS = {"PARSE_SYNTAX_ERROR", "INVALID_VIEW_TEXT", "UNRESOLVED_COLUMN",
              "UNRESOLVED_ROUTINE", "DATATYPE_MISMATCH", "WRONG_NUM_ARGS"}


def repairable_views(report: dict) -> list[str]:
    objects = report.get("objects") or []
    if not objects or any(o["object_type"] == "TABLE" and o["status"] != "SUCCESS" for o in objects):
        return []
    failed = [o for o in objects if o["status"] == "FAILED"]
    for item in failed:
        code = item.get("error_code") or ""
        if not code:
            match = re.match(r"\[([A-Z_]+)(?:\.[A-Z_]+)?\]", item.get("message") or "")
            code = match.group(1) if match else ""
        if item["object_type"] != "VIEW" or code.split(".")[0] not in SQL_ERRORS:
            return []
    return [o["name"] for o in objects if o["object_type"] == "VIEW" and o["status"] != "SUCCESS"] if failed else []


def validate_correction(previous: dict, proposed: dict, eligible: list[str]) -> list[dict]:
    if previous["tables"] != proposed["tables"]:
        raise ValueError("SQL repair cannot change base tables, mappings, extraction or replication")
    if len(previous["views"]) != len(proposed["views"]):
        raise ValueError("SQL repair cannot change the view graph")
    changes = []
    for old, new in zip(previous["views"], proposed["views"]):
        before, after = old["view"], new["view"]
        if old["dependencies"] != new["dependencies"] or {
            k: v for k, v in before.items() if k not in {"view_sql", "logic_description"}
        } != {k: v for k, v in after.items() if k not in {"view_sql", "logic_description"}}:
            raise ValueError("SQL repair cannot change destinations or dependencies")
        if before["view_sql"] != after["view_sql"]:
            name = f"{before['fabric_schema_name']}.{before['view_name']}"
            if name.casefold() not in {n.casefold() for n in eligible}:
                raise ValueError("SQL repair cannot change successfully provisioned views")
            changes.append({"name": name, "before": before["view_sql"], "after": after["view_sql"],
                            "logic_description": after.get("logic_description")})
        elif before.get("logic_description") != after.get("logic_description"):
            raise ValueError("Translation notes may change only alongside a SQL correction")
    if not changes:
        raise ValueError("Repair must change at least one failed view SQL statement")
    return changes
