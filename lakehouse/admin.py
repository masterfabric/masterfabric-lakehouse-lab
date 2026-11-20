"""Lake administration: storage report and a full purge (used by the benchmark).

    spark-submit lakehouse/admin.py report   -> docs/results/storage_report.json
    spark-submit lakehouse/admin.py purge    -> DROP TABLE ... PURGE for every lab table
"""

from __future__ import annotations

import sys

from lakehouse.spark import CATALOG, file_stats, session, write_result

NAMESPACES = ("bronze", "silver", "gold", "ops")


def tables(spark) -> list[str]:
    out = []
    for ns in NAMESPACES:
        if spark.sql(f"SHOW NAMESPACES IN {CATALOG}").filter(f"namespace = '{ns}'").count():
            out += [f"{ns}.{r.tableName}" for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").collect()]
    return out


def report(spark) -> dict:
    rows = []
    for t in tables(spark):
        st = file_stats(spark, t)
        snaps = spark.sql(f"SELECT count(*) FROM {CATALOG}.{t}.snapshots").first()[0]
        rows.append({"table": t, **st, "snapshots": snaps})
        print(f"[storage] {t:36} files={st['files']:5} records={st['records']:9} bytes={st['bytes']}")
    write_result("storage_report", {"tables": rows})
    return {"tables": rows}


def purge(spark) -> None:
    for t in tables(spark):
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{t} PURGE")
        print(f"[purge] {t}")


if __name__ == "__main__":
    s = session("admin")
    {"report": report, "purge": purge}[sys.argv[1]](s)
