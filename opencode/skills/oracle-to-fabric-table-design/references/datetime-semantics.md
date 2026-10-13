# Oracle datetime semantics and Fabric execution

This user-approved policy applies to table design, base tables in view bundles,
and every derived datetime expression. Apply it without asking routine timezone
questions. Record the semantic category, precision, extraction expression,
representation, and runtime requirements in evidence/transformation_notes/warnings.

## DATE and plain TIMESTAMP: preserve wall-clock fields

Oracle DATE and TIMESTAMP(n) contain calendar fields, without a stored timezone.
Neither DBTIMEZONE nor SESSIONTIMEZONE identifies their business timezone.
Preserve year/month/day/hour/minute/second/fraction exactly. Do not use
SYS_EXTRACT_UTC, AT TIME ZONE, FROM_TZ, to_utc_timestamp or from_utc_timestamp
to shift these values. A value `2025-03-31 23:30:00` must stay exactly that.

Use the **Pinned UTC Wall-Clock** convention: Spark parsing, writing and view
evaluation use `spark.sql.session.timeZone = UTC`; the same calendar fields are
represented in TIMESTAMP under that convention. This does not assert that the
Oracle source represented a UTC instant. DATE maps to TIMESTAMP, retaining time
to whole seconds, never DATE merely because of its Oracle type name.
TIMESTAMP(0..6) maps to TIMESTAMP if the connector preserves precision.
TIMESTAMP(7..9) uses exact STRING, e.g.
`TO_CHAR(col, 'YYYY-MM-DD"T"HH24:MI:SS.FF9', 'NLS_DATE_LANGUAGE=English')`.
Never silently truncate fractional seconds for a view calculation.

Direct native extraction must preserve fields across the Oracle connector;
document an explicit formatted-text extraction and UTC parsing if the connector
otherwise interprets naive fields using a machine timezone. Test month/quarter
boundaries, leap days, non-midnight values, fractional seconds and nulls using
bounded aggregate comparisons, without exposing source row values.
The provisioning notebook pins its Spark session to UTC. The separately owned
loader and other consumers must also enforce this convention: list that as a
runtime requirement, not as a verified implementation. Existing plans and loaded
data are not retroactively converted by updating this skill.

## Timezone-aware types

TSTZ stores timezone information as an offset or a named region. The preferred
expanded design is UTC TIMESTAMP via `SYS_EXTRACT_UTC(col)` plus original offset
via `TO_CHAR(col, 'TZH:TZM')`; preserve TZR/TZD or a canonical original value when
region identity matters. Offset alone cannot preserve a named region or its
future daylight-saving rules.

**Current app contract: exactly one target column per source column.** Do not
return extra offset columns in that schema. Use an exact canonical STRING
including date/time, fractional precision and offset; retain named region and
daylight designation when present, using an explicit tested TO_CHAR format such
as `YYYY-MM-DD"T"HH24:MI:SS.FF9 TZH:TZM TZR TZD`. Describe how to reconstruct it.
If the view requires timezone operations this representation cannot faithfully
support in the available runtime, report a material blocker. Do not discard the
offset simply to get TIMESTAMP or silently invent multi-column support.

TSLTZ does not preserve the original offset/region; Oracle normalizes storage
relative to DBTIMEZONE and renders it in SESSIONTIMEZONE. Propose UTC TIMESTAMP
with `SYS_EXTRACT_UTC(CAST(col AS TIMESTAMP WITH TIME ZONE))`, verifying behavior
against the actual Oracle/connector version and an explicitly configured
extraction session (UTC preferred). Do not first cast to a naive timestamp and
guess an offset. Do not claim the CAST is session-independent without checking
the extraction behavior. Precision above six still requires an exact canonical
UTC STRING. Never invent an original offset for TSLTZ.

## View translation

Track datetime lineage through casts, CTEs, joins, aggregates and nested views.
Use calendar semantics for naive source values and instant semantics only for
aware values normalized to UTC. Explicit comparisons/casts must match the
approved base representation; no TIMESTAMP_NTZ under the current app contract.

- TRUNC: distinguish date truncation from numeric truncation; preserve requested
  day/month/quarter/year/week semantics and Oracle's return type.
- ADD_MONTHS: preserve time of day and Oracle last-day-of-month behavior. Spark
  add_months returns DATE; a direct substitution can drop time and differ at
  month ends. Reconstruct the time component and last-day rule when needed.
- LAST_DAY and NEXT_DAY: preserve source time of day where Oracle does; Spark
  date-returning functions alone are insufficient. Check weekday language.
- MONTHS_BETWEEN: verify last-day/same-day rules, time components, 31-day fraction
  and rounding rather than assuming identical defaults.
- TO_CHAR: translate Oracle format tokens explicitly. MONTH needs language,
  padding and case treatment; Q is quarter, not a generic copied format token.
- GREATEST/LEAST: Oracle null propagation and type coercion differ from Spark;
  add explicit null guards and compatible casts. Do not silently ignore nulls.
- Date arithmetic, predicates and partitions must retain boundary values. UTC
  session pinning prevents timezone shifts; it does not fix function semantics.
- NLS_CALENDAR, NLS_DATE_LANGUAGE and SYSDATE/CURRENT_DATE/current timestamp
  depend on runtime context. Obtain evidence or raise a genuine blocker when
  that context matters; never guess a business timezone for those expressions.

Check the actual Oracle and Fabric Spark versions with live metadata/documentation.
Reference: [Oracle SYS_EXTRACT_UTC](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/SYS_EXTRACT_UTC.html),
[Spark datetime functions](https://spark.apache.org/docs/latest/api/sql/datetime-functions/).
