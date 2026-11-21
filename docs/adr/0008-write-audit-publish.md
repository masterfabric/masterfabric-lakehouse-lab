# 0008. Write-audit-publish on Iceberg branches, audited by Great Expectations

- Status: Accepted
- Date: 2026-11-21

## Context

The 20-rule quality gate (ADR 0004) runs after silver is written: it can stop gold from being
rebuilt, but analysts reading silver through Trino already see the bad data. A defect introduced
by a code release (rather than by the source) is also invisible to row-level rules: every row of
a delivery can be well-formed while charges are 100 times too high.

## Options

1. Write to a staging table, validate, then copy into the target: doubles the write and loses the
   target's history for the staged data.
2. Validate the DataFrame before writing: no record of what was rejected, and nothing to debug
   with after the job ends.
3. **Iceberg branches**: write the delivery to an audit branch of the real table, validate the
   branch, and fast-forward `main` only if the audit passes.

## Decision

Option 3. `write.wap.enabled=true` on `silver.cdr`; each delivery is written with
`spark.wap.branch=audit_<day>_<n>`; a Great Expectations suite validates exactly the delivery's
rows on the branch; `CALL system.fast_forward('silver.cdr', 'main', <branch>)` publishes.
Failed branches are kept for debugging. Tags (`before_wap`, `published_<day>`) mark known-good
states so `rollback_to_snapshot` can undo a bad direct write.

The suite has eight contract expectations and three learned from `main` (rows per day within
±50 %, charge maximum below 2× the historical maximum, mean charge within ±30 %).

## Measured (docs/results/wap.json)

| | Release 2.3 delivery | Release 2.4 delivery (charges ×100, city lost) |
|---|---:|---:|
| Rows written to the branch | 14,684 | 14,684 |
| Rows visible on `main` while auditing | 0 | 0 new |
| Expectations passed | 11 / 11 | 8 / 11 |
| Audit time | 1.64 s | 1.04 s |
| Outcome | fast-forward in 0.061 s, main 123,689 → 138,373 | not published, main stays 138,373 |

An accidental `DELETE` of the day on `main` (138,373 → 123,689 rows) was undone by
`rollback_to_snapshot` in 0.057 s, without copying data.

## Consequences

- Readers of `main` never see an unaudited delivery; publishing is a metadata commit.
- Branches and tags hold snapshots alive: snapshot expiry (ADR 0007) must keep referenced
  snapshots and old audit branches need a retention rule.
- Spark's cache treated scans of two different branches as the same plan, and Great Expectations
  persists its batch; the first version of the job silently re-validated the first branch and
  published the buggy one. The audit now clears the cache and selects the delivery by source
  file, and a test covers the failing case.
- Learned thresholds drift with the data; they are a guard against gross errors, not a substitute
  for the contract rules.
