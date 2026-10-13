# Autonomous mapping of unqualified Oracle NUMBER

Apply this policy to both table designs and every base table in a view design. The user has
authorized routine mapping decisions from read-only profiles and business meaning; do not
ask them to pick STRING, INT, BIGINT or DECIMAL for each column. This is permission to
propose a plan, not to approve, persist or execute it.

## Identify the source correctly

Read DATA_TYPE, DATA_PRECISION and DATA_SCALE from ALL_TAB_COLUMNS. Unqualified NUMBER
normally has NULL precision and scale. A metadata adapter may display those as zero;
confirm against the dictionary before treating NUMBER(0,0) as a declaration. Scale zero
alone does not mean unqualified: NUMBER(10,0) has a real integer precision. Missing
precision with an explicitly declared scale still supplies scale evidence. Preserve all
original metadata; never rewrite NULL precision/scale into a supposed source constraint.

## Profile before deciding

Use the explicitly selected SQLcl connection and validated identifiers. Execute small
read-only queries sequentially. Start with at most 10,000 rows per table (or a smaller
limit from the caller). Combine a few relevant columns per query when practical. Return
aggregate statistics, not source records. Record the exact SQL, row limit, timestamp,
rows examined, non-null/null counts, distinct count, fractional count, negative count,
integer range fit and decimal fit checks. Deduplicate profiles when several views use
the same column. ROWNUM is a bounded convenience sample, not a random or representative
sample; label it accordingly. Do not silently scan an entire large table to gain certainty.

Example for a validated numeric column (replace identifiers, not arbitrary SQL text):

```sql
SELECT COUNT(*) AS sampled_rows,
       COUNT(v) AS non_null_rows,
       COUNT(*) - COUNT(v) AS null_rows,
       COUNT(DISTINCT v) AS distinct_values,
       SUM(CASE WHEN v <> TRUNC(v) THEN 1 ELSE 0 END) AS fractional_rows,
       SUM(CASE WHEN v < 0 THEN 1 ELSE 0 END) AS negative_rows,
       SUM(CASE WHEN v < -2147483648 OR v > 2147483647 THEN 1 ELSE 0 END) AS outside_int,
       SUM(CASE WHEN v < -9223372036854775808 OR v > 9223372036854775807 THEN 1 ELSE 0 END) AS outside_bigint
FROM (SELECT column_name AS v FROM owner.table_name WHERE ROWNUM <= 10000);
```

An integer mapping requires zero fractional rows AND the chosen signed range. For a
candidate DECIMAL(p,s), add a fit check against the same bounded sample:

```sql
SUM(CASE WHEN ABS(v) >= POWER(10, p - s) OR v <> ROUND(v, s) THEN 1 ELSE 0 END)
```

Substitute validated integer literals for p and s, with 1 <= p <= 38 and 0 <= s <= p.
Every non-null value must pass both the range and scale checks. Use a few candidate
scales guided by metadata and business units, increasing scale when fractional values
require it. Do not count digits in TM9 text without handling its scientific notation.
Do not infer scale zero from rounded display output. Empty/all-null samples give no
numeric-domain evidence; missing profile results are not zero failures.

## Decide from business role and usage

Use comments/documentation, declared PK/FK/check constraints, relationships and actual
view expressions. Names and low cardinality are supporting clues, not proof. Label
inferences. A numeric physical type does not establish that arithmetic is meaningful.

| Evidence and role | Proposed target |
| --- | --- |
| Customer/order/product IDs, reference numbers, categories, status codes, or other labels with no meaningful arithmetic | STRING using canonical Oracle text |
| Number of persons, quantities, counts or positions with a whole-number domain | INT or BIGINT when the sampled range and plausible domain/growth fit; otherwise a supported DECIMAL(p,0) |
| Amounts, rates, prices, balances, fractional quantities or values used meaningfully in arithmetic/SUM/AVG | DECIMAL(p,s) that passes range and fractional checks; choose precision/headroom from business domain and evidence |
| Ambiguous role, empty/all-null data, unavailable profile, or exact numeric values outside supported decimal precision/scale | Canonical STRING with a warning; do not guess or round |

Do not choose DECIMAL(10,0) merely because a count is small today. It is an option when
its integer range and growth assumption are justified; INT/BIGINT or a wider decimal
may be more suitable. DECIMAL(38,0) can still round fractions and overflow broad Oracle
NUMBER values. Never silently use DOUBLE for exact amounts or identifiers.

Use `TO_CHAR(column_name, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')` for canonical numeric
STRING extraction, preserving NULL. This preserves the stored numeric value as text;
Oracle NUMBER did not preserve originally entered leading zeros. Do not invent padding.

## View-specific consistency

Joins, COUNT(ID), GROUP BY, partition keys and equality tests do not by themselves make
an identifier a measure. Treat LINE_NUMBER as an ordinal: keep it numeric when numeric
ordering/window frames/running totals rely on it. Meaningful SUM/AVG/arithmetic on a
column weighs toward numeric storage even if its name ends in _ID. Investigate apparent
conflicts rather than trusting the suffix.

Use compatible representations for both sides of a join. For identifiers stored as
STRING, preserve Oracle numeric ordering with explicit numeric expressions only where
the profiled domain fits the cast; never replace numeric ordering with lexical ordering.
Preserve numeric MIN/MAX, RANGE windows, division and aggregation result precision too.
If no exact supported cast can preserve the view, report the specific translation blocker
and ask only for the necessary alternative; do not reopen all routine type choices.

## Evidence, assumptions and questions

Record the proposed type, extraction expression, role evidence, aggregate profile scope,
fit checks and rationale in the existing evidence/transformation_notes/warnings fields.
Do not add fields outside the app's JSON contract. Samples are observations, not enforced
future bounds. Where no validated constraint establishes the domain, explicitly record
the range/scale/growth assumption and recommend full-data validation and rejection of
out-of-range/fractional values before loading. Do not claim those runtime checks exist
or have executed unless verified. Routine sample uncertainty is a warning, not a request
for the user to select a datatype. Use exact STRING fallback when numerical evidence is
insufficient; flag downstream numeric translation limitations separately.

Reuse applicable explicitly approved business exceptions. The current user-approved
automatic profiling policy supersedes older guidance to ask about every unqualified
NUMBER. Never transfer a decision from another table as evidence for this table.
Only ask a native question for an unresolved material conflict that prevents an exact
design after profiling/research (for example, an unsupported calculation). Final plan
review and non-numeric decisions such as timezone conventions remain unchanged.
