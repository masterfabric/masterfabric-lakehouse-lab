# Contributing to MasterFabric Lakehouse Lab

Thanks for your interest! This repository is a runnable lakehouse lab: a synthetic telecom data
generator, a Spark medallion pipeline on Apache Iceberg, a Trino query layer and the tooling that
measures and documents all of it.

## Good first contributions

- **A new quality rule** in `lakehouse/quality.py` (one SQL query returning a violation count).
- **A new gold mart** with a documented grain, plus a Trino query that uses it.
- **A new defect** in the generator (with its count in `_manifest.json`) and the silver rule and
  reconciliation check that handle it.
- **Another engine** reading the same catalog (Flink, DuckDB, PyIceberg), as a separate Compose
  profile.
- **Docs**: corrections, clearer explanations, CDP or other platform mappings.

## Setup

```bash
make up                 # Docker + Compose v2; Colima works on macOS without admin rights
make pipeline demos     # full run
uv venv -p 3.12 .venv && uv pip install -p .venv -e '.[tools]'   # host tooling (tests, charts)
```

## Before you open a pull request

```bash
make test               # 8 host unit tests + 6 Spark/Iceberg tests in the container
make reset pipeline demos gold quality   # the gate must report 20/20 (or more, if you add rules)
```

- If your change alters measured numbers, re-run the producing target (`make bench`,
  `make trino-sql`, `make assets`) and commit the updated `docs/results/` files together with
  the code. Never type numbers into the README by hand that a run did not produce.
- Keep the layers' contracts (see `docs/adr/0004-medallion-contracts.md`): bronze never rejects,
  silver never drops silently, gold is rebuilt atomically.
- A new architectural choice gets an ADR in `docs/adr/`.

## Style

- Python: type hints, small pure functions for transformation logic (`lakehouse/rules.py` is the
  model), 120-column lines. SQL in upper-case keywords.
- Scala: Dataset API with case classes; Catalyst expressions instead of UDFs where possible.
- English only, in code, comments, docs and commit messages.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/), one logical change per commit, with
a body that explains what and why:

```
feat(silver): quarantine sessions with negative volume

The packet core occasionally reports counter wrap-arounds as negative
bytes. Route them to silver.quarantine instead of letting them reduce
the daily usage totals, and reconcile the count in the quality gate.
```

## Reporting issues

- Questions and bugs: [GitHub Issues](https://github.com/masterfabric/masterfabric-lakehouse-lab/issues)
- Security: [private vulnerability report](https://github.com/masterfabric/masterfabric-lakehouse-lab/security/advisories/new)

By contributing you agree that your contributions are licensed under the GNU AGPL v3.0
([LICENSE](LICENSE), [NOTICE](NOTICE)) and that you follow the [Code of Conduct](CODE_OF_CONDUCT.md).
