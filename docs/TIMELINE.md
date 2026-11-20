# Build timeline

How this repository was built, one logical change per commit. Times are
Europe/Istanbul (UTC+3) on 2026-11-21; everything ran on an Apple M4 Pro.

Each step was run and verified before it was committed, and the repository was public and pushed
after every commit from the generator step onwards. The history was never rewritten: the MSISDN
bug found during the gold step is fixed in its own commit (`2d5de7c`) rather than squashed away.


## Foundation

| Time | Commit | Change | Why |
|---|---|---|---|
| 01:25 | [`453adce`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/453adce) | chore: scaffold repository with AGPL-3.0 license and community files | License, NOTICE with third-party licenses, community files, gitignore that keeps data and .env out. |
| 01:31 | [`3d2e776`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/3d2e776) | feat(generator): add deterministic synthetic telecom data generator | Realistic raw feeds with injected defects and a manifest of exactly what was injected. |
| 01:41 | [`4229863`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/4229863) | feat(stack): add Docker Compose lakehouse stack and Makefile | Five services on localhost ports: RustFS, bucket init, Iceberg REST catalog, Spark, Trino; Makefile. |

## Medallion pipeline

| Time | Commit | Change | Why |
|---|---|---|---|
| 01:41 | [`3b85dad`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/3b85dad) | feat(bronze): land raw feeds into Iceberg with lineage and exactly-once file ingestion | Raw strings + lineage columns; ops.ingest_log makes ingestion exactly-once per file. |
| 01:43 | [`b524b21`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/b524b21) | feat(silver): validate, deduplicate and conform with incremental MERGE INTO | Incremental snapshot reads, rule-based quarantine, newer-wins MERGE INTO. |
| 01:46 | [`2d5de7c`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/2d5de7c) | fix(generator): emit unique 12-digit MSISDNs | Bug found by the gold mart (200 instead of 2,000 subscribers): MSISDNs were truncated into collisions. |
| 01:46 | [`50073a5`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/50073a5) | feat(gold): add usage, cell congestion and churn feature marts | Usage, cell-hour load and churn feature marts, rebuilt atomically. |
| 01:47 | [`e401557`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/e401557) | feat(quality): add declarative data quality gate with manifest reconciliation | 20 SQL rules incl. reconciliation against the manifest; non-zero exit blocks publishing. |
| 01:52 | [`5854096`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/5854096) | refactor(silver): MERGE with explicit column lists | Explicit MERGE column lists so schema evolution on the target cannot break loads. |
| 01:52 | [`9ca5832`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/9ca5832) | feat(pipeline): add end-to-end runner with per-layer timings | One spark-submit per layer, timings and layer metrics to docs/results. |

## Iceberg and engines

| Time | Commit | Change | Why |
|---|---|---|---|
| 01:53 | [`85489c8`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/85489c8) | feat(iceberg): demonstrate evolution, late MERGE, time travel and maintenance | Schema and partition evolution, 10 late-correction MERGE batches, time travel, compaction, expiry, orphan scan. |
| 01:54 | [`2ed6cb6`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/2ed6cb6) | feat(scala): add typed Scala cell congestion ranking job | Typed Scala Dataset job: per-city cell ranking and upgrade priority. |

## Verification

| Time | Commit | Change | Why |
|---|---|---|---|
| 01:55 | [`a61639e`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/a61639e) | test: add generator unit tests and Spark/Iceberg behaviour tests | 8 generator unit tests, 6 Spark/Iceberg tests on a throwaway catalog. |

## Architecture docs

| Time | Commit | Change | Why |
|---|---|---|---|
| 01:57 | [`1aad76a`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/1aad76a) | docs(adr): record seven architecture decisions | Seven ADRs: format, storage, catalog, contracts, marts, engines, maintenance. |

## Verification

| Time | Commit | Change | Why |
|---|---|---|---|
| 02:02 | [`fc4af5e`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/fc4af5e) | feat(bench): add scale benchmark and lake storage report | Scale 1 to 8 benchmark on a purged lake each time; storage report. |

## Iceberg and engines

| Time | Commit | Change | Why |
|---|---|---|---|
| 02:02 | [`e284a47`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/e284a47) | feat(trino): add showcase interactive queries with warm timings | Seven analyst queries on Trino, including time travel on a gold mart. |
| 02:03 | [`1e70726`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/1e70726) | feat(notebook): add executed lakehouse tour notebook | Notebook built from source and executed against the live lab. |

## Foundation

| Time | Commit | Change | Why |
|---|---|---|---|
| 02:09 | [`4ce6fe1`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/4ce6fe1) | feat(stack): archive landing files to the s3://landing raw zone | Immutable raw-zone copy in s3://landing/raw. |

## Evidence and documentation

| Time | Commit | Change | Why |
|---|---|---|---|
| 02:09 | [`3ffd53e`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/3ffd53e) | docs(results): record measured results from the M4 Pro | Every measured number as JSON, the source for README tables and charts. |
| 02:09 | [`72749b1`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/72749b1) | docs(diagrams): render architecture, medallion flow and CDP mapping | Architecture, medallion flow with real counts, CDP mapping. |
| 02:09 | [`3e323aa`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/3e323aa) | docs(charts): plot benchmark, maintenance, congestion and churn results | Scaling, compaction, small-file growth, congestion heatmap, churn ROC. |
| 02:09 | [`1ce3609`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/1ce3609) | docs(screenshots): capture real UI screenshots with Playwright | Headless captures of the S3 console, Spark History Server, Trino and the notebook. |
| 02:09 | [`c0cfcc8`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/c0cfcc8) | docs(hero): compose hero image from screenshots and measured numbers | Hero composed from screenshots; facts read from results at render time. |
| 02:12 | [`adb9549`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/adb9549) | docs(readme): write the full README with architecture, walkthrough and results | Complete README. |
| 02:12 | [`13e4041`](https://github.com/masterfabric/masterfabric-lakehouse-lab/commit/13e4041) | docs: add contributing guide | Contributing guide. |

## Verification at each stage

| Stage | What was run | Result |
|---|---|---|
| generator | `python -m generator.telco_gen --scale 0.1 --days 2` | manifest counts match files |
| bronze | `make bronze` twice | second run: `new_files: 0` for every feed |
| silver | `make silver` twice | distinct CDRs = 124,286 = generator clean count; second run no-op |
| gold | `make gold` | 13,860 subscriber-days after the MSISDN fix (was 1,400) |
| quality | `make quality` | 20/20 |
| Iceberg demos | `make demos` then `make gold quality` | 0 files rewritten by evolution; 73 → 14 files; 20/20 after gold rebuild |
| tests | `make test` | 8 + 6 passed |
| benchmark | `make bench` | scales 1, 2, 4, 8 all 20/20; 32.4 s to 68.6 s |
| assets | `make trino-sql notebook screenshots assets` | every image regenerated from results |

