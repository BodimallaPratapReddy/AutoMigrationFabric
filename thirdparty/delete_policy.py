"""Deterministic delete policy validation shared by approvals and ingestion."""

MODES = {"NONE", "SOFT_DELETE", "RECONCILE", "SOFT_DELETE_AND_RECONCILE"}
DELETE_COLUMNS = {"_is_deleted", "_delete_detected_at", "_delete_detection_method"}


def validate_delete_policy(policy: dict, columns: list[str], keys: list[str],
                           watermark: str | None) -> dict:
    if not isinstance(policy, dict) or policy.get("mode") not in MODES:
        raise ValueError("Choose a delete detection method")
    mode = policy["mode"]
    if mode == "NONE":
        return {"mode": "NONE"}
    if not keys:
        raise ValueError("Delete propagation requires validated key fields")
    if any(name.lower() in DELETE_COLUMNS for name in columns):
        raise ValueError("Source columns conflict with reserved deletion metadata fields")
    behavior = policy.get("behavior")
    if behavior not in {"MARK", "DELETE"}:
        raise ValueError("Choose mark deleted or remove from current-state Bronze")
    result = {"mode": mode, "behavior": behavior}
    if mode in {"SOFT_DELETE", "SOFT_DELETE_AND_RECONCILE"}:
        field = policy.get("soft_delete_column")
        if field not in columns:
            raise ValueError("Select a discovered soft-delete field")
        predicate = policy.get("soft_delete_predicate", "VALUES")
        if predicate not in {"VALUES", "NOT_NULL"}:
            raise ValueError("Choose deletion values or a non-null deletion timestamp")
        values = policy.get("soft_delete_values", [])
        if (predicate == "VALUES" and (not isinstance(values, list) or not values
                or any(not isinstance(value, str) or not value for value in values))):
            raise ValueError("Supply at least one exact source deletion value")
        if watermark and policy.get("watermark_tracks_soft_delete") is not True:
            raise ValueError("Confirm that deletion and restoration changes advance the watermark")
        result.update(soft_delete_column=field, soft_delete_predicate=predicate,
                      soft_delete_values=list(dict.fromkeys(values)) if predicate == "VALUES" else [],
                      watermark_tracks_soft_delete=bool(watermark))
    if mode in {"RECONCILE", "SOFT_DELETE_AND_RECONCILE"}:
        interval = policy.get("reconcile_interval_minutes")
        if isinstance(interval, bool) or not isinstance(interval, int) or not 15 <= interval <= 525600:
            raise ValueError("Reconciliation interval must be between 15 and 525600 minutes")
        result.update(reconcile_interval_minutes=interval,
                      reconcile_require_complete_snapshot=True,
                      reconcile_require_consistent_snapshot=True)
    return result
