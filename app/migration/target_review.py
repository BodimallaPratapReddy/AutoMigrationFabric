"""Compare approved target definitions with saved Config DB definitions."""

from __future__ import annotations


def compare_columns(proposed: list[dict], existing: list[dict]) -> dict:
    """Return a stable, UI-ready structural diff using target column names."""
    def shape(columns: list[dict]) -> dict[str, dict]:
        result = {}
        for column in columns:
            name = (column.get("target_column_name") or column["column_name"]).upper()
            result[name] = {
                "name": name,
                "fabric_type": column["fabric_data_type"].upper(),
                "nullable": column.get("is_nullable"),
            }
        return result

    before, after = shape(existing), shape(proposed)
    added = [after[name] for name in sorted(after.keys() - before.keys())]
    removed = [before[name] for name in sorted(before.keys() - after.keys())]
    changed = [{"before": before[name], "after": after[name]}
               for name in sorted(before.keys() & after.keys()) if before[name] != after[name]]
    return {"same": not (added or removed or changed), "added": added,
            "removed": removed, "changed": changed}
