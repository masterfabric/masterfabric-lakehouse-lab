# 0009. Lineage from the Spark jobs with OpenLineage, stored in Marquez

- Status: Accepted
- Date: 2026-11-21

## Context

The medallion contracts say which table feeds which, but only in prose. Impact analysis ("what
breaks if `bronze.cdr_raw.termination` changes?") and audit questions ("where does this gold
column come from?") need lineage captured from what the jobs actually did.

## Options

1. Hand-maintained lineage in documentation: goes stale with the first refactor.
2. Parse the SQL of each job: misses DataFrame code and dynamic table names.
3. **OpenLineage Spark listener**: emits run, job, dataset and column-lineage events from Spark's
   query plans at runtime, to any OpenLineage backend.

## Decision

Option 3, with Marquez as the lab backend. The listener jar is in the Spark image but is only
enabled for `make lineage` and `make wap`, so the regular pipeline and its timings are unchanged.
Marquez runs in an optional compose profile with its own Postgres.

## Measured (docs/results/lineage.json)

One pipeline run plus the write-audit-publish job recorded 54 jobs and 16 datasets in Marquez.
Collapsed to the five Spark applications this is 23 dataset edges from the landing folders to the
gold marts, and column lineage on 10 datasets: `gold.churn_features.drop_rate` traces back to
`silver.cdr.event_type`, `silver.cdr.is_dropped` and `bronze.cdr_raw.termination`.

## Consequences

- Lineage is produced by the jobs themselves, so it stays current.
- Each Spark action becomes a Marquez job (54 for five applications); views need to be collapsed
  per application, as `scripts/lineage_export.py` does.
- Column lineage reaches the landing files for the subscriber and cell feeds; for CDRs it stops at
  `bronze.cdr_raw`. Branch writes appear as writes to `silver.cdr` (the branch is not a separate
  dataset in the events).
- On CDP, Apache Atlas collects Spark lineage through its own Spark hook. OpenLineage is the
  engine-neutral alternative: the same listener sends to any OpenLineage backend, and only the
  transport settings change.
