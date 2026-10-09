# Oracle deletion configuration for dynamic Fabric pipelines

This app performs source discovery, key profiling, user approval, and Config DB
persistence. It never extracts table data for loading, applies source deletes,
or schedules ingestion. Dynamic Fabric pipelines own those operations.

Apply `thirdparty/configdb/migrations/004_oracle_delete_policy.sql` to an existing
configuration database before deploying the updated API and migration worker.
New databases include these fields in `config_db_new_database.sql`.

## Saved configuration

`bronze_replication.ReplicationConfig.DeletePolicy` is JSON. It is saved in the same
approved runtime-plan transaction as the key and watermark configuration.
`KeyValidation` contains the exact full-table key-check result, selected source
columns, and observation time. It does not guarantee future rows are valid.

Example for both supported detection mechanisms:

```json
{
  "mode": "SOFT_DELETE_AND_RECONCILE",
  "behavior": "MARK",
  "soft_delete_column": "IS_DELETED",
  "soft_delete_predicate": "VALUES",
  "soft_delete_values": ["Y", "1"],
  "watermark_tracks_soft_delete": true,
  "reconcile_interval_minutes": 1440,
  "reconcile_require_complete_snapshot": true,
  "reconcile_require_consistent_snapshot": true
}
```

| Queue column | Meaning |
| --- | --- |
| `DeleteDetectionMethod` | `NONE`, `SOFT_DELETE`, `RECONCILE`, or `SOFT_DELETE_AND_RECONCILE` |
| `DeleteAction` | `MARK` retains the row with deletion metadata; `DELETE` removes it from the current-state table |
| `SoftDeleteColumn` | Discovered Oracle source field |
| `SoftDeletePredicate` | `VALUES` or `NOT_NULL` |
| `SoftDeleteValues` | JSON array of exact, case-sensitive source-value strings; no SQL expressions |
| `SoftDeleteWatermarkConfirmed` | User confirmed both deletion and restoration advance the chosen watermark |
| `ReconcileIntervalMinutes` | Minimum time between successful reconciliations; pipeline scheduler must enforce it |
| `ReconcileRequireCompleteSnapshot` | Must be true for reconciliation |
| `ReconcileRequireConsistentSnapshot` | Must be true for reconciliation |
| `LastReconciledTimestamp` | UTC time of the last successful reconciliation, written by the pipeline |
| `LastReconciledSCN` | Oracle snapshot SCN from the last successful reconciliation, written by the pipeline |
| `DeletePolicy`, `KeyValidation` | Original JSON policy and key-validation evidence |

Existing rows with NULL `DeletePolicy` expose `DeleteDetectionMethod=NONE`.
`NONE` disables incremental delete propagation; a complete `FULL`/`REPLACE` load
still removes rows absent from the source snapshot.

## Required pipeline behavior

1. Read policy and key/watermark configuration from `vw_ReplicationQueue`.
   Fail closed on an unknown non-NONE policy or unsupported action. Do not enable
   a new policy until the dynamic pipeline implements its contract.
2. Serialize loading, reconciliation, and structure changes for the same target
   using the existing `ReplicationState.Status='RUNNING'` guard. Keep the guard
   until all target changes and checkpoint updates complete. Never clear a
   RUNNING guard automatically merely because a run is slow.
3. Validate incoming keys too. An approval-time key check is a point-in-time
   observation, not a source constraint. Reject null keys and ambiguous duplicate
   keys before applying changes. Never invent a key from Oracle ROWID.
4. For soft deletes, include the deletion field in extraction even if it is not
   needed by downstream consumers. For `VALUES`, compare the source field's
   agreed string representation with the exact values; null means active. Reject
   unsupported conversions rather than silently changing case, trimming values,
   or guessing numeric/timestamp formats. `NOT_NULL` means any non-null value is
   deleted. Do not filter deleted rows out before processing their keys.
5. For `MARK`, the pipeline owns `_is_deleted` (BOOLEAN), `_delete_detected_at`
   (TIMESTAMP), and `_delete_detection_method` (STRING). These are target metadata,
   not columns to SELECT from Oracle. Active reinserts/restorations clear the flag
   and deletion metadata. Retrying an unchanged tombstone must not change its
   original detection time. `DELETE` must remove only the identified key and
   must not insert a missing soft-deleted row. Newer active rows can reinsert it.
6. For reconciliation, extract the entire key set at one consistent source point
   (for example, a supported `AS OF SCN` snapshot), with the same table scope as
   normal ingestion. Use a unique staging snapshot per run. Verify all chunks
   succeeded and staged counts match the expected count from that same snapshot.
   All keys must pass null/duplicate checks. A partial snapshot, count mismatch,
   timeout, privilege failure, or lost connection means **no reconciliation
   deletes and no reconciliation checkpoint advancement**. An empty snapshot is
   valid only after the same completeness checks prove the source is empty.
7. Compare keys from that complete snapshot with active target keys, under the
   same serialization guard. Missing target keys become deletions according to
   `DeleteAction`. Never use a watermark batch for this comparison. Never run
   ingestion against a newer source point while applying an older reconciliation
   snapshot. For combined mode, reconciliation detects physically removed rows;
   soft-delete processing handles rows still present with a deletion flag.
8. Set `LastReconciledTimestamp`/`LastReconciledSCN` only after the target commit.
   Advance the extraction watermark only after its target writes succeed. The
   pipeline must make a retry idempotent when a Delta commit succeeds but its
   Config DB checkpoint update fails. Emit deletion counts and audit results.

Delta change data feed can expose target changes to Silver, but is not an Oracle
delete detector. Detection time is not necessarily source deletion time. MARK
retains only the current row/tombstone, not a complete event history. Physical
data reclamation and history retention remain separate maintenance policies.
