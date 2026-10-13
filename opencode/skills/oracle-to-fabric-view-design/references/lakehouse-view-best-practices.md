# Spark SQL Lakehouse view generation

Apply this policy before returning an initial view bundle or a SQL correction. It incorporates the user's `lakehouse_view_best_practices.md` guidance within the existing Oracle-to-Fabric equivalence and JSON contracts.

## Correctness and target engine

- Generate **Fabric Lakehouse Spark SQL**, using the selected Lakehouse's supported runtime. SQL analytics endpoint T-SQL is a different target and must not supply unverified function or DDL syntax.
- Verify uncertain Fabric functions, casts, view support and examples with `microsoft-learn`; use `oracle-db-doc` for source semantics and `oracle-sqlcl` for source metadata/profiles. Record official URLs and the runtime assumptions in `evidence`.
- Preserve row multiplicity, joins, filters, grouping, output names/order, null and empty-string behavior, numeric meaning and date/time semantics. Optimization must not change business results. Preserve requested target names and verified Oracle output aliases; meaningful new names belong to internal CTEs and intermediate expressions.
- Use the existing NUMBER profiling, datetime and BLOB policies. Do not remap base columns to make the SQL easier. Do not add output columns, persistent helper objects or undeclared dependencies.

## Readable structure and scalable execution

- Use meaningful, non-recursive CTEs for complex views: for example `ORDER_LINE_BASE`, `ORDER_AGGREGATE`, `ORDER_ENRICHED`, followed by an explicit final projection. A simple direct projection needs no artificial layers.
- Centralize repeated joins, business rules and deterministic derived expressions in CTEs where equivalent. Reuse derived dates and normalized amounts rather than repeating expressions. Avoid deep nesting and single-letter CTE names; qualify source column references with clear table/CTE aliases.
- Never use wildcard **column projections** (`SELECT *`, `H.*`) in any layer. Expand them using verified metadata and preserve source output order. `COUNT(*)` is an aggregate over rows, not a wildcard projection, and may remain where it preserves Oracle semantics.
- Minimize avoidable scans and joins, prune unused intermediate columns and apply filters early only where equivalent. Predicate movement across outer joins, aggregates or windows requires proof of equivalence. Preserve required self-joins, `UNION ALL` branches, `DISTINCT`, cross joins and correlated logic rather than deleting them to satisfy a scan-count target.
- A CTE centralizes SQL text; it does **not** guarantee materialization, caching, a single physical scan or faster execution. The existing provisioning notebook expands CTEs into scoped derived tables for persistent-view compatibility. Keep the approved SQL CTE-based and readable; do not claim the deployed plan shares computation without evidence.
- Do not refactor nondeterministic or volatile expressions as though they were reusable constants. Preserve their evaluation semantics or flag the conflict.
- Aim for stable formatting, indentation, aliases, column order and mappings for the same verified input. Reuse an approved plan during repair. Do not claim byte-identical LLM generation is guaranteed.
- Do not add a top-level `ORDER BY` for presentation; consumers sort their results. Preserve order needed for `LIMIT`, top-N selection, analytic expressions or other row-selection semantics. Removing such order is a semantic change.

## Casts, financial arithmetic and identifiers

- Make necessary cross-type conversions explicit, especially comparisons, join keys, arithmetic and window ordering. Do not add redundant casts that round, truncate or shift otherwise-correct values.
- Preserve business identifiers exactly in the approved representation. A numeric Oracle identifier stored as STRING may need a verified numeric expression for numeric ordering; lexicographic sorting is not equivalent. Do not change the projected identifier or silently normalize distinct keys.
- Validate STRING-to-number conversions against source metadata, transformation provenance and required profiles. A bounded sample does not prove the entire or future value domain. Record checked scope and remaining assumptions without outputting source values.
- `TRY_CAST` is conditional on runtime support **and** an approved invalid-value-to-NULL policy. It is not a substitute for profiling and must not hide invalid keys, remove join matches, change ranks or omit values from aggregates. Use a verified exact `CAST` when that matches Oracle behavior; flag unresolved unsafe domains instead of guessing.
- Prefer exact `DECIMAL(p,s)` for exact financial amounts and calculations; derive precision/scale from metadata, profiles and expression bounds, including multiplication, division and aggregate growth. Avoid FLOAT/DOUBLE unless the source/business contract requires them.
- Do not apply `DECIMAL(38,18)` universally: it allows 20 integer digits and may overflow or lose required fractional precision. Verify Spark expression result types and runtime overflow/rounding behavior. Explain any unavoidable precision conflict as a blocker rather than silently rounding.

## NULLs, windows and data-quality safeguards

- Define important NULL behavior explicitly using typed `CASE`, `COALESCE` or `NULLIF` only where they reproduce the source/business contract. Preserve Oracle empty-string semantics and the existing datetime policy.
- Do not invent defaults such as `COALESCE(CUSTOMER_ID, 'UNKNOWN')`: this can collapse distinct states or change grouping and joins. Do not insert divide-by-zero-to-NULL guards unless the Oracle definition or an approved decision requires that behavior. Preserve error behavior where material; ask only about genuine unresolved semantic choices.
- For `ROW_NUMBER` and row-sensitive windows, use proven stable tie-breakers when consistent with source intent. Verify numeric versus string ordering, NULL placement, partition grain and explicit `ROWS`/`RANGE` frames.
- Do not append tie-breakers blindly to `RANK`, `DENSE_RANK` or peer-sensitive `RANGE` frames: doing so changes peer groups/results. Preserve Oracle ordering and frames. If source ordering is ambiguous and affects selected rows or running results, record the limitation; request a business decision only when evidence cannot establish the intended order.

## Short comments and separate evidence

- The current adapter joins each view's `translation_notes` into its `logic_description`, which the notebook uses as the catalog `COMMENT`. Therefore keep **all translation_notes together** to 1–3 business-focused sentences, fewer than 500 characters. Describe purpose, subject area and intended usage.
- Put detailed lineage, original SQL references, mapping decisions, documentation URLs, runtime caveats, quality-check results and repair diagnostics in the bundle's existing `evidence` (prefix entries with `SCHEMA.VIEW`), `warnings` or `unresolved_decisions` as appropriate. Do not include logs, agent reasoning or code history in the catalog description.
- Return exactly the supplied JSON contract. `spark_sql` contains one bounded Spark SELECT, optionally WITH; no CREATE/ALTER statements, COMMENT clauses, SQL comments or trailing semicolons. Do not invent a `business_description` field or issue DDL yourself.

## Required final review

Before returning the bundle, check every view for:

1. Explicit, qualified projections; correct output aliases/order and exact declared target dependencies.
2. Meaningful CTE layers for complex logic; shared rules and no avoidable duplicate joins/expressions. Explain unavoidable repetition.
3. Explicit, justified conversions; preserved identifiers; exact financial precision and safe numeric ordering.
4. Oracle-compatible NULLs, empty strings, date/time expressions, window peers/frames and row multiplicity.
5. No presentation-only top-level sorting or unauthorized data-quality defaults.
6. A short business comment and detailed evidence kept separately.
7. Readable, stable SQL and documented runtime assumptions. Distinguish local parsing, documentation review, sampled profiling and actual Fabric execution; only claim the checks actually performed. Performance claims require an executed plan/measurements, not a CTE count.

During SQL-only repair, apply the checks to eligible changed SQL while preserving successful view SQL, frozen mappings, approved replication choices and dependency identities. Report required out-of-scope changes instead of applying them.

## Official references

- [Apache Spark CTE syntax](https://spark.apache.org/docs/3.5.4/sql-ref-syntax-qry-select-cte.html): query-scoped named results, not a promise of physical scan reuse.
- [Apache Spark ANSI behavior](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html): cast failures, tolerant casts and decimal arithmetic. Check documentation for the actual Fabric runtime before applying version-dependent examples.
