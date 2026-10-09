"""Compare approved target definitions with saved Config DB definitions."""

from __future__ import annotations

import re


def compare_columns(proposed: list[dict], existing: list[dict], *, detailed: bool = False) -> dict:
    """Return a stable, UI-ready structural diff using target column names."""
    def shape(columns: list[dict]) -> dict[str, dict]:
        result = {}
        for column in columns:
            if detailed and not column.get("is_selected", True):
                continue
            name = (column.get("target_column_name") or column["column_name"]).upper()
            result[name] = {
                "name": name,
                "fabric_type": column["fabric_data_type"].upper(),
                "nullable": column.get("is_nullable"),
            }
            if detailed:
                item = result[name]
                if re.fullmatch(r"(?:VARCHAR|NVARCHAR|CHAR|NCHAR|STRING)(?:\((?:\d+|MAX)\))?", item["fabric_type"]):
                    item["fabric_type"] = "STRING"
                item.update(primary_key=bool(column.get("is_primary_key")),
                            source_type=(column.get("source_data_type") or "").upper(),
                            description=column.get("description"))
        return result

    before, after = shape(existing), shape(proposed)
    added = [after[name] for name in sorted(after.keys() - before.keys())]
    removed = [before[name] for name in sorted(before.keys() - after.keys())]
    def differs(name: str) -> bool:
        old, new = before[name], after[name]
        if not detailed:
            return old != new
        return (old["fabric_type"] != new["fabric_type"] or old["primary_key"] != new["primary_key"]
                or (old["nullable"] is not None and new["nullable"] is not None
                    and old["nullable"] != new["nullable"])
                or (old["source_type"] and new["source_type"] and old["source_type"] != new["source_type"])
                or (new["description"] is not None and old["description"] != new["description"]))
    changed = [{"before": before[name], "after": after[name]}
               for name in sorted(before.keys() & after.keys()) if differs(name)]
    return {"same": not (added or removed or changed), "added": added,
            "removed": removed, "changed": changed}


REPLICATION_REVIEW_FIELDS = (
    "incremental_method", "write_strategy", "primary_key_columns", "merge_key_columns",
    "watermark_column", "watermark_column_data_type", "watermark_index_name",
    "effective_from_column", "effective_to_column", "current_flag_column", "max_row_fetch",
    "pipeline_workspace_id", "pipeline_item_id",
    "delete_policy",
)


def compare_replication(proposed: dict, existing: dict | None) -> dict:
    def shape(config: dict) -> dict:
        result = {}
        for name in REPLICATION_REVIEW_FIELDS:
            value = config.get(name)
            if name == "delete_policy":
                value = value or {"mode": "NONE"}
            elif name in {"primary_key_columns", "merge_key_columns"}:
                value = sorted(str(item).upper() for item in value or [])
            elif isinstance(value, str):
                value = value.upper()
            result[name] = value
        return result
    before, after = shape(existing or {}), shape(proposed)
    changed = [{"name": name, "before": before[name], "after": after[name]}
               for name in REPLICATION_REVIEW_FIELDS if before[name] != after[name]]
    return {"same": existing is not None and not changed, "changed": changed,
            "before": before, "after": after}


def allowed_target_decisions(review: dict) -> set[str]:
    if not review.get("is_provisioned_record"):
        return {"REVISE_PLANNED"}
    allowed = {"ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"}
    definition = next((item for item in review.get("definitions", [])
                       if item["source_table_guid"] == review.get("selected_existing_guid")), {})
    changes = {item["name"] for item in definition.get("replication_comparison", {}).get("changed", [])}
    if changes & {"incremental_method", "watermark_column", "watermark_column_data_type"}:
        allowed.discard("ALTER_FUTURE")
    if "delete_policy" in changes:
        # Historical markers and previously missed deletions need a new baseline.
        allowed.discard("ALTER_FUTURE")
    key_change = any(item["before"].get("primary_key") != item["after"].get("primary_key")
                     for item in definition.get("comparison", {}).get("changed", []))
    if key_change or changes & {"primary_key_columns", "merge_key_columns"}:
        return {"REPLACE_FULL"}
    return allowed
