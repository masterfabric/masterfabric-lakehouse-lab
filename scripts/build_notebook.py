"""Build notebooks/lakehouse_tour.ipynb from source cells (so the notebook is reviewable as code).
Executed inside the Spark container by `make notebook`, which also runs it so outputs are real."""

import nbformat as nbf

cells = [
    ("md", "# Lakehouse tour\n\nA guided look at the lake that `make pipeline demos` produced: layers, "
           "quarantine, Iceberg metadata tables, time travel and the gold marts. Every output below "
           "was produced by executing this notebook against the running lab."),
    ("code", "from lakehouse.spark import session\nspark = session('notebook-tour')\n"
             "spark.sql('SHOW NAMESPACES IN lake').show()"),
    ("md", "## 1. Row counts per layer"),
    ("code", "tables = ['bronze.cdr_raw', 'bronze.sessions_raw', 'silver.cdr', 'silver.sessions', 'silver.quarantine',\n"
             "          'gold.daily_subscriber_usage', 'gold.cell_hourly_load', 'gold.churn_features',\n"
             "          'gold.cell_congestion_ranking']\n"
             "spark.createDataFrame([(t, spark.table(f'lake.{t}').count()) for t in tables], 'table string, rows long').show(truncate=False)"),
    ("md", "## 2. Why records were quarantined\n\nEach failed record keeps the first rule it broke and its raw payload."),
    ("code", "spark.sql('''SELECT feed, reason, count(*) AS records, first(payload) AS example\n"
             "             FROM lake.silver.quarantine GROUP BY feed, reason ORDER BY records DESC''').show(truncate=70)"),
    ("md", "## 3. Iceberg metadata: snapshots and partition specs of `silver.cdr`"),
    ("code", "spark.sql('''SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added,\n"
             "                    summary['total-records'] AS total, summary['total-data-files'] AS data_files\n"
             "             FROM lake.silver.cdr.snapshots ORDER BY committed_at''').show(truncate=False)\n"
             "spark.sql('SELECT spec_id, count(*) AS files, sum(record_count) AS records FROM lake.silver.cdr.files GROUP BY 1').show()"),
    ("md", "## 4. Time travel\n\n`gold.daily_subscriber_usage` was rebuilt after the correction feed. Its first "
           "snapshot is still readable, so the effect of the corrections can be measured exactly."),
    ("code", "first = spark.sql('SELECT snapshot_id FROM lake.gold.daily_subscriber_usage.snapshots ORDER BY committed_at LIMIT 1').first()[0]\n"
             "spark.sql(f'''SELECT 'first build' AS version, count(*) AS subscriber_days, sum(voice_calls) AS calls, sum(charge_try) AS charge\n"
             "              FROM lake.gold.daily_subscriber_usage VERSION AS OF {first}\n"
             "              UNION ALL SELECT 'current', count(*), sum(voice_calls), sum(charge_try) FROM lake.gold.daily_subscriber_usage''').show()"),
    ("md", "## 5. Gold: hourly load profile of the five busiest cells"),
    ("code", "%matplotlib inline\nimport matplotlib.pyplot as plt\n"
             "top = [r.cell_id for r in spark.sql('SELECT cell_id FROM lake.gold.cell_congestion_ranking ORDER BY congested_hours DESC LIMIT 5').collect()]\n"
             "pdf = spark.sql(f'''SELECT cell_id, hour(hour_ts) AS hod, avg(utilization) AS u FROM lake.gold.cell_hourly_load\n"
             "                    WHERE cell_id IN ({','.join(repr(c) for c in top)}) GROUP BY 1, 2 ORDER BY 1, 2''').toPandas()\n"
             "fig, ax = plt.subplots(figsize=(9, 4))\n"
             "for cid, g in pdf.groupby('cell_id'):\n    ax.plot(g.hod, g.u, marker='o', ms=3, label=cid)\n"
             "ax.axhline(0.8, ls='--', c='crimson', lw=1, label='congestion threshold')\n"
             "ax.set_xlabel('hour of day (UTC)'); ax.set_ylabel('mean utilization'); ax.legend(ncol=3, fontsize=8)\n"
             "ax.set_title('Busiest cells: mean utilization by hour of day'); plt.show()"),
    ("md", "## 6. Churn features: score deciles"),
    ("code", "spark.sql('''SELECT decile, count(*) AS subscribers, round(avg(churn_risk_score), 3) AS score,\n"
             "                    round(avg(usage_trend), 3) AS usage_trend, round(avg(drop_rate), 4) AS drop_rate\n"
             "             FROM (SELECT *, ntile(10) OVER (ORDER BY churn_risk_score DESC) AS decile FROM lake.gold.churn_features)\n"
             "             GROUP BY decile ORDER BY decile''').show()"),
]

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nb.cells = [nbf.v4.new_markdown_cell(src) if kind == "md" else nbf.v4.new_code_cell(src) for kind, src in cells]
import sys

if len(sys.argv) > 1 and sys.argv[1] == "--strip-stderr":
    # After execution: drop JVM start-up warnings (stderr) so the outputs show results only.
    nb = nbf.read("notebooks/lakehouse_tour.ipynb", as_version=4)
    for c in nb.cells:
        if c.cell_type == "code":
            c.outputs = [o for o in c.outputs if not (o.get("output_type") == "stream" and o.get("name") == "stderr")]
nbf.write(nb, "notebooks/lakehouse_tour.ipynb")
print("notebooks/lakehouse_tour.ipynb written")
