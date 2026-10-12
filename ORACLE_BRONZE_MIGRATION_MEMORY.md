# Oracle to Fabric Bronze migration memory

Last reviewed: 2026-10-09.

This document records the migration decisions, current configuration behavior, and best practices agreed for this project. Bronze represents the **current Oracle state**. The application discovers source metadata, validates configuration, obtains approvals, and saves configuration to Config DB. Dynamic Fabric pipelines perform data loading and execute the configured policies.

## Ownership and implementation status

| Area | Responsibility and status |
| --- | --- |
| Metadata discovery and type mapping | Application; implemented |
| Oracle database and ETRM descriptions | New table/base-table plans store original database comments in `Description` and separate detailed ETRM descriptions in `EtrmDescription` at table and column level. Provisioning uses only database descriptions for table/column comments; missing database comments are not filled from ETRM. Existing records are not retroactively relabeled. |
| Approved Oracle extraction SQL | New table plans and view base-table plans persist `SourceTables.ExtractSql`, exposed by `vw_ReplicationQueue`; scheduled pipeline metadata drift checks remain an external integration responsibility |
| Key suggestions, user-selected keys, source null/duplicate profiling | Application; implemented |
| Soft-delete and scheduled key-reconciliation configuration | Application and Config DB; implemented |
| View dependency table replication review | New view workflows review each base table's agent mappings, key validation, watermark/full refresh and delete policy before final bundle approval; existing workflow histories retain their prior behavior |
| Extraction, staging, merge, deletion, retries, scheduling, checkpoints | Fabric pipelines; execution must follow the configuration contract |
| Consistency-test selection and optimization review in the setup workflow | Discussed; not implemented |
| Validation history, staging manifests, cost estimates | Proposed pipeline/configuration extensions; not implemented by this document |

Do not add data-loading logic to this application as part of these plans. Saving a delete policy does not establish that an external pipeline implements it. The execution requirements are in [the delete pipeline contract](thirdparty/fabric/ORACLE_DELETE_PIPELINE_CONTRACT.md).

## Current-state contract

At an aligned Oracle snapshot and committed Bronze version, the active keys and mapped values should agree. Incremental updates must replace the previous state for the same replication key. Hard deletes require reconciliation or a source change/deletion feed; a normal watermark query cannot discover rows that no longer exist.

When delete action is `MARK`, active-state comparisons exclude `_is_deleted = true`. Retained tombstones support deletion tracking but do not constitute complete change history. A restored source row must become active again. When action is `DELETE`, the pipeline removes the target row.

## Source discovery and analysis

Capture the source owner/table, ordered column names, Oracle types, precision, scale, nullability, comments, primary and unique constraints, all indexes, partition metadata, and available table statistics. Preserve the original definitions with the approved mapping.

Analyze metadata for distinct purposes:

- Constraints and unique indexes propose replication keys; ordinary indexes reveal access paths, not uniqueness.
- Timestamp columns propose watermarks; business behavior determines whether every change updates them.
- Row count, average row length, blocks, and statistics age inform sizing. Statistics are estimates and may be stale.
- Partitioning and indexes guide extraction experiments. Do not copy Oracle index definitions into Lakehouse table properties.
- Large objects, unsupported types, timezone semantics, and numeric bounds require explicit mapping review.

Additional discovery worth considering includes constraint validation state, index-column expressions, column distributions, change rate, source retention, snapshot privileges, and extraction query plans. These are requirements to evaluate, not claims that every field is currently collected.

## Type mapping: use the JSON and mapper together

The source policy is [oracle_type_mapping.json](thirdparty/oracle/oracle_type_mapping.json); [datatype_mapping.py](thirdparty/datatype_mapping.py) supplies rule ordering and fallback behavior. Oracle `NUMBER` has several mappings based on precision and scale.

| Source definition | Current target | Reason |
| --- | --- | --- |
| `NUMBER` without declared precision | `STRING` | Avoid assuming numeric range or scale |
| `NUMBER(9,0)` | `INT` | Integer precision up to 9 |
| `NUMBER(18,0)` | `BIGINT` | Integer precision from 10 through 18 |
| `NUMBER(25,0)` | `DECIMAL(25,0)` | Integer precision from 19 through 38 |
| `NUMBER(10,2)` | `DECIMAL(10,2)` | Supported positive scale within precision |
| `NUMBER(10,-2)` | `DECIMAL(12,0)` | Required integer precision is `p + abs(s)` |
| `NUMBER(38,-1)` | `STRING` | Adjusted precision exceeds 38 |
| `NUMBER(38,39)` | `DOUBLE` | Configured rule for scale at least 39 |
| `NUMBER(5,6)` | `STRING`, review required | Mapper fallback for positive scale greater than precision, below overflow threshold |

The mapper checks declared precision outside 1–38 before other numeric rules and uses the review fallback. If precision is known but scale is missing, it assumes scale zero. Missing precision uses the unbounded rule.

**Open testing item:** the scale-overflow JSON rule currently labels `DOUBLE` as `lossy: false`. Binary floating point does not guarantee exact preservation of decimal values. Leave this policy unchanged for now, as agreed; test representative and boundary values before deciding the correction. For exact identifiers or financial values, consider an approved exact representation such as canonical strings when decimal limits are exceeded. Test key uniqueness again after conversion because distinct source values can collapse to one target value.

Other current rules: text and CLOB types map to `STRING`; RAW/BLOB types to `BINARY`; Oracle `DATE` to `TIMESTAMP` so time is retained. Timezone timestamp variants are flagged as lossy. Timestamp precision metadata does not itself guarantee target precision preservation. Intervals require representation review; BFILE, XMLTYPE, spatial, and unknown types require explicit handling. `ROWID` mapping to string does not make it a stable business replication key.

## Replication keys before watermarks

Discover the declared primary key first. Current inspection reads enabled primary/unique constraints but does not check their `VALIDATED` state. Treat this distinction explicitly during future discovery improvements.

If no usable primary key exists, show unique-index candidates and allow the user to select **any discovered columns**, including a composite key outside those suggestions. Read all indexes for context, but propose uniqueness only from appropriate unique indexes. Function-based or unusable indexes are not ordinary column-key candidates.

A unique index does not prove all key components are non-null. Run read-only source queries checking null components and duplicates across the entire selected composite key. Record scope, check time, and outcome; failure or timeout means unverified, not valid. Current checks use a read-only transaction and bounded call timeout. Full-table checks can be expensive.

Passing setup checks proves observed data validity at that time, not future enforcement. Define handling for later duplicates, nulls, and mutable keys. If the user confirms no key, restrict the current setup to full refresh. An ordinary incremental upsert or key reconciliation cannot safely infer row identity without a valid key.

## Watermark requirements

Use a column that changes on every relevant insert/update, including soft deletion and restoration. Being timestamp-typed or indexed does not establish this behavior. Confirm it with the source owner and representative tests.

The pipeline needs a fixed extraction boundary, handling for equal watermark values, late commits and replay, and an idempotent merge. A timestamp cursor alone can miss changes committed after a prior extraction with older timestamps. Choose overlap, tie-breaking, version guards, or an appropriate source change feed based on demonstrated source behavior.

Advance the checkpoint only after the target commit succeeds. A failed run must replay safely without advancing past uncommitted data.

## Delete-handling configuration

Current modes are `NONE`, `SOFT_DELETE`, `RECONCILE`, and `SOFT_DELETE_AND_RECONCILE`; actions are `MARK` or `DELETE`.

- Soft deletion uses a discovered source column and either `VALUES` (configured deletion values; null remains active) or `NOT_NULL` (for example, a deletion timestamp).
- Watermark ingestion requires confirmation that soft-delete changes update the watermark.
- Reconciliation stores an interval in minutes and requires both a complete and a consistent source-key snapshot.
- Reserved marker names are `_is_deleted`, `_delete_detected_at`, and `_delete_detection_method`. Pipelines own their target implementation.

`ReplicationConfig` stores `DeletePolicy` and `KeyValidation` JSON alongside key and ingestion configuration. `vw_ReplicationQueue` exposes policy fields for pipelines. `ReplicationState.LastReconciledTimestamp` and `LastReconciledSCN` must advance only after successful reconciliation. Existing absent policies mean `NONE`.

Protect extraction, target writes, reconciliation, and schema changes with coordinated run ownership, including the `RUNNING` state. Do not let an older snapshot remove rows from a newer load. A slow run alone is not evidence that its lock may be cleared.

## Temporary staging in Lakehouse Files

Temporary batch staging can use Parquet files under Lakehouse `Files`, with isolated source/batch/attempt paths. These files can be read by Spark; placing files there does not automatically register a managed table. See [Microsoft's Lakehouse file-loading documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data).

Staging makes a batch inspectable and replayable. A proposed manifest should record extraction boundaries or SCN, expected/completed chunks, row counts, mapping version, and completion status. A batch should progress through extraction, validation, target commit, and checkpoint persistence before cleanup. Keep failed attempts long enough for retry and diagnosis; use a separate retention policy for eventual cleanup.

Persist checkpoints and validation results after deleting temporary files. Historical stage files are not required to compare current source and target state.

**Incremental staging verifies the incoming batch, not the whole table.** Loading 101, then 102, then 103 through temporary stages does not prove that 101 still exists or remains correct in Bronze. End-to-end verification needs a fresh source comparison.

## Complete key snapshots for reconciliation

Extract all keys for the agreed source scope at one consistent boundary. Oracle Flashback Query with `AS OF SCN` is an option where supported and authorized; it depends on retained undo and applicable restrictions. Do not assume it works for every source. See [Oracle Flashback documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html).

Before interpreting missing keys as deletions, verify every extraction chunk completed, key validity, source scope, snapshot consistency, and alignment with the target state. Matching counts alone do not prove completeness.

| Aligned Oracle keys | Bronze keys | Interpretation |
| --- | --- | --- |
| 101, 103 | 101, 102, 103 | 102 is a deletion candidate after successful complete reconciliation |
| 101, 102, 103 | 102, 103 | 101 is missing in Bronze; investigate or repair under an approved policy |

Never use an incremental watermark batch as the complete key set. Never perform deletion after partial extraction or failed snapshot validation. Key comparison detects presence differences; detecting wrong non-key values requires value comparison as well.

## Proposed consistency tests

These checks are design guidance for future configuration and pipeline work.

| Frequency | Checks | What they establish |
| --- | --- | --- |
| Every load | Schema/mapping compatibility, incoming null/duplicate keys before and after mapping, rejected rows, processing metrics, freshness | Batch integrity and operational health |
| Scheduled | Active-row counts, missing/extra keys, target key validity, selected or full value comparisons, delete/restore behavior | Source/target consistency for the tested scope |
| After configuration changes | Mapping boundaries, schema evolution, full-refresh behavior, restart/replay and failure scenarios | Approved behavior under change and recovery |

Processing metrics should distinguish staged, rejected, inserted, updated, deleted, and unchanged rows with explicit definitions. Freshness should consider last successful run and source backlog; an old maximum timestamp can be normal for an unchanged table.

Record source snapshot/SCN, target Delta version, scope, mapping and policy versions, deletion filtering, execution time, and result. If source and target cannot be aligned, report that limitation rather than claiming exact consistency. Clearly distinguish sampled checks from complete checks. Counts, aggregates, and hashes are useful diagnostics; hashes need canonical representations and still have collision limitations.

A failed validation should alert or pause publication according to policy. It must not automatically authorize destructive repair.

## Large tables and cost

A full key-only comparison reads fewer bytes than all columns but still examines every key in its scope. Measure source execution plans, logical/physical reads, CPU, elapsed time, network bytes, staged bytes, and Fabric consumption. A suitable covering index may help; validate its actual plan and completeness rather than assuming it will be used.

Use distributed comparisons, bounded extraction parallelism, and explicit schedules. Extra parallelism can increase source pressure without reducing total work. Complete partition/range reconciliation is possible only when source and target scope match and row/key movement is handled. Rolling checks do not provide one simultaneous whole-table snapshot; recent-window or sampled checks cannot prove absence everywhere.

If frequent full scans are unaffordable, reconsider reconciliation frequency or a source change/deletion feed. Estimate from statistics and pilot runs, then compare with actual measurements. There is no universal cost per Oracle record. Attribute work to shared Fabric capacity and distinguish utilization from incremental billing; OneLake storage has separate consumption considerations. See [OneLake consumption](https://learn.microsoft.com/en-us/fabric/onelake/onelake-consumption).

## Optimization review

Identify the actual writer and readers before choosing properties. Start with a simple layout, then measure write size, update rate, query predicates, and maintenance overhead.

- Evaluate partitioning or clustering against measured reads and merges; Oracle indexes are not Lakehouse indexes.
- Confirm runtime-specific adaptive file sizing and compaction support. Copy/Dataflow execution does not automatically inherit Spark write settings.
- Schedule `OPTIMIZE` when necessary for file layout; account for its capacity and latency cost.
- Evaluate V-Order for consumption patterns such as Direct Lake rather than enabling it universally for write-heavy ingestion.
- Treat `VACUUM` as retention cleanup; choose retention compatible with readers, retries, recovery, and downstream consumers.

These choices depend on the execution engine and supported table features. See [Microsoft's table maintenance and optimization guidance](https://learn.microsoft.com/en-us/fabric/fundamentals/table-maintenance-optimization).

Delta change data feed exposes changes made to the Delta target after enablement; it does not capture missed Oracle changes. Plan retention for downstream consumers. See [Fabric Delta change data feed](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-change-data-feed).

## Setup and testing checklist

1. Discover metadata and review exceptional mappings, including numeric precision/scale and timezone behavior.
2. Confirm and profile the replication key before selecting incremental ingestion.
3. Confirm watermark change behavior, extraction boundaries, and replay safety.
4. Approve delete mode, action, source predicate, snapshot guarantees, and reconciliation schedule.
5. Confirm the external pipeline implements the stored configuration and updates state after successful commits.
6. Test inserts, updates, deletes, restorations, null/duplicate keys, numeric boundaries, and partial failures.
7. Agree consistency-check scope and frequency, source impact, staging retention, and maintenance requirements.

Future workflow proposal: metadata review → key approval → ingestion choice → delete policy → consistency-test plan → optimization review → final approval. The last two review steps remain design proposals.

## Implementation references

- [Oracle discovery and key profiling](thirdparty/oracle/utils.py)
- [Key candidates and migration planning](app/migration/plans.py)
- [Delete-policy validation](thirdparty/delete_policy.py)
- [Config DB delete-policy migration](thirdparty/configdb/migrations/004_oracle_delete_policy.sql)
- [Fabric delete pipeline contract](thirdparty/fabric/ORACLE_DELETE_PIPELINE_CONTRACT.md)
- [Fabric provisioning notebook](thirdparty/fabric/nb_migration_provisioning.py)

Update this memory when decisions change. Keep proposed behavior clearly separated from verified implementation.
