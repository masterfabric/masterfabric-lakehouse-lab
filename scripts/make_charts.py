"""Render result charts from docs/results/*.json and live Trino queries (matplotlib).

Every number plotted is read from a measured result file or queried from the lake; nothing
is typed in by hand. Also computes the baseline churn score's ROC AUC against the
generator's held-out labels and stores it in docs/results/churn_baseline.json.
"""

from __future__ import annotations

import csv
import gzip
import json

import trino

from mfviz import HEX, IMG, RESULTS, ROOT, mpl_setup

plt = mpl_setup()
C = HEX


def load(name):
    return json.loads((RESULTS / f"{name}.json").read_text())


def pipeline_scaling():
    b = load("benchmark")
    runs = b["runs"]
    steps = ["generate", "bronze", "silver", "gold", "quality"]
    colors = [C["YELLOW"], C["ORANGE"], C["ICE"], C["GREEN"], C["PURPLE"]]
    fig, ax = plt.subplots(figsize=(11, 5.2))
    labels = [f"scale {r['scale']:g}\n{r['raw_records'] / 1e6:.2f} M raw rows" for r in runs]
    left = [0.0] * len(runs)
    for s, col in zip(steps, colors):
        vals = [r["wall_seconds"][s] for r in runs]
        ax.barh(labels, vals, left=left, color=col, label=s, height=0.62)
        left = [l + v for l, v in zip(left, vals)]
    for i, r in enumerate(runs):
        spark_s = sum(r["wall_seconds"][s] for s in steps[1:4])
        ax.text(left[i] + 0.8, i, f"{r['total_seconds']:.1f} s total · {r['raw_records'] / spark_s / 1000:,.0f} k rows/s "
                                  f"through bronze→gold · DQ {r['dq_passed']}", va="center", fontsize=10, color=C["WHITE"])
    ax.set_xlim(0, max(left) * 1.9)
    ax.invert_yaxis()
    ax.set_xlabel("wall-clock seconds (each layer is a separate spark-submit, JVM start-up included)")
    ax.set_title(f"End-to-end pipeline by scale · {b['meta']['host']} · Colima VM {b['meta']['container_cpus']} vCPU · "
                 f"Spark {b['meta']['spark_master']}", loc="left")
    ax.legend(ncol=5, loc="upper right", fontsize=9)
    fig.savefig(IMG / "pipeline_scaling.png")
    plt.close(fig)


def compaction():
    d = load("iceberg_demos")["maintenance"]

    def count(files, content):
        return sum(f["files"] for f in files if f["content"] == content)

    before, after = d["before"]["files"], d["after_compaction"]["files"]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.2))
    ax = axes[0]
    xs = ["before", "after"]
    data = [count(before, "data"), count(after, "data")]
    dels = [count(before, "position_deletes"), count(after, "position_deletes")]
    ax.bar(xs, data, color=C["ICE"], label="data files")
    ax.bar(xs, dels, bottom=data, color=C["ORANGE"], label="position-delete files")
    for i, (a, b2) in enumerate(zip(data, dels)):
        ax.text(i, a + b2 + 1, f"{a} + {b2}", ha="center", color=C["WHITE"])
    ax.set_title("Files in silver.cdr", loc="left")
    ax.legend(fontsize=9)
    ax = axes[1]
    q = [d["before"]["query"]["median_ms"], d["after_compaction"]["query"]["median_ms"]]
    ax.bar(xs, q, color=[C["RED"], C["GREEN"]])
    for i, v in enumerate(q):
        ax.text(i, v + 1, f"{v} ms", ha="center", color=C["WHITE"])
    ax.set_title("One-day query, median of 7", loc="left")
    ax = axes[2]
    s = [d["snapshots_before_expiry"], d["snapshots_after_expiry"]]
    ax.bar(["before expiry", "after expiry"], s, color=[C["PURPLE"], C["GREEN"]])
    for i, v in enumerate(s):
        ax.text(i, v + 0.2, str(v), ha="center", color=C["WHITE"])
    ax.set_title("Snapshots retained", loc="left")
    fig.suptitle("Iceberg maintenance: rewrite_data_files + rewrite_position_delete_files + expire_snapshots",
                 x=0.01, ha="left", color=C["WHITE"], fontsize=13)
    fig.tight_layout()
    fig.savefig(IMG / "compaction.png")
    plt.close(fig)


def late_corrections():
    log = load("iceberg_demos")["late_corrections"]
    fig, ax = plt.subplots(figsize=(11, 4.2))
    x = list(range(1, len(log) + 1))
    ax.plot(x, [b["total-data-files"] for b in log], marker="o", color=C["ICE"], label="total data files")
    ax2 = ax.twinx()
    ax2.plot(x, [b["added-position-deletes"] for b in log], marker="s", color=C["ORANGE"],
             label="position deletes (cumulative)")
    ax2.grid(False)
    ax2.tick_params(colors=C["GREY"])
    ax.set_xlabel("correction micro-batch (40 re-rated + 30 late CDRs each, MERGE INTO, merge-on-read)")
    ax.set_ylabel("data files")
    ax2.set_ylabel("position deletes")
    ax.set_title("The small-file problem, measured: each MERGE adds files and delete records", loc="left")
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")
    fig.savefig(IMG / "late_corrections.png")
    plt.close(fig)


def trino_rows(sql):
    cur = trino.dbapi.connect(host="127.0.0.1", port=18080, user="lab", catalog="lake").cursor()
    cur.execute(sql)
    return cur.fetchall()


def congestion_heatmap():
    rows = trino_rows("""
        WITH top AS (SELECT cell_id, city FROM lake.gold.cell_congestion_ranking
                     ORDER BY congested_hours DESC, p95_utilization DESC LIMIT 14)
        SELECT t.cell_id || ' · ' || t.city, hour(l.hour_ts), avg(l.utilization)
        FROM lake.gold.cell_hourly_load l JOIN top t ON l.cell_id = t.cell_id GROUP BY 1, 2""")
    cells = sorted({r[0] for r in rows}, key=lambda c: -sum(r[2] for r in rows if r[0] == c))
    grid = [[0.0] * 24 for _ in cells]
    for c, h, u in rows:
        grid[cells.index(c)][h] = u
    fig, ax = plt.subplots(figsize=(12, 5.4))
    im = ax.imshow(grid, aspect="auto", cmap="magma", vmin=0, vmax=1.1)
    ax.set_yticks(range(len(cells)), cells, fontsize=9)
    ax.set_xticks(range(24), [f"{h:02d}" for h in range(24)], fontsize=8)
    ax.grid(False)
    ax.set_xlabel("hour of day (UTC)")
    cb = fig.colorbar(im, ax=ax, pad=0.01)
    cb.set_label("mean utilization (concurrency / capacity)", color=C["WHITE"])
    cb.ax.yaxis.set_tick_params(color=C["GREY"], labelcolor=C["GREY"])
    ax.set_title("gold.cell_hourly_load · the 14 most congested cells, mean utilization by hour", loc="left")
    fig.savefig(IMG / "congestion_heatmap.png")
    plt.close(fig)


def churn_roc():
    with gzip.open(ROOT / "data" / "landing" / "labels" / "churn_labels.csv.gz", "rt") as fh:
        labels = {r["subscriber_id"]: int(r["churned_next_30d"]) for r in csv.DictReader(fh)}
    scores = {sid: float(s) for sid, s in trino_rows("SELECT subscriber_id, churn_risk_score FROM lake.gold.churn_features")}
    pairs = sorted(((scores[k], labels[k]) for k in scores if k in labels), key=lambda p: -p[0])
    pos = sum(y for _, y in pairs)
    neg = len(pairs) - pos
    # ROC by sweeping thresholds; AUC by the trapezoid rule (ties handled by grouping).
    tpr, fpr, tp, fp, i = [0.0], [0.0], 0, 0, 0
    while i < len(pairs):
        j = i
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            tp += pairs[j][1]
            fp += 1 - pairs[j][1]
            j += 1
        tpr.append(tp / pos)
        fpr.append(fp / neg)
        i = j
    auc = sum((fpr[k] - fpr[k - 1]) * (tpr[k] + tpr[k - 1]) / 2 for k in range(1, len(fpr)))
    top_decile = pairs[: len(pairs) // 10]
    lift = (sum(y for _, y in top_decile) / len(top_decile)) / (pos / len(pairs))
    (RESULTS / "churn_baseline.json").write_text(json.dumps({
        "subscribers": len(pairs), "churners": pos, "base_rate": round(pos / len(pairs), 4),
        "roc_auc": round(auc, 4), "top_decile_lift": round(lift, 2)}, indent=2) + "\n")
    fig, ax = plt.subplots(figsize=(6.4, 5.6))
    ax.plot(fpr, tpr, color=C["ORANGE"], lw=2.2, label=f"baseline score · AUC {auc:.3f}")
    ax.plot([0, 1], [0, 1], ls="--", color=C["GREY"], lw=1, label="random")
    ax.set_xlabel("false positive rate")
    ax.set_ylabel("true positive rate")
    ax.set_title(f"Rule-based churn score vs. held-out labels\nn={len(pairs):,} · base rate {pos / len(pairs):.1%} · "
                 f"top-decile lift {lift:.2f}×", loc="left", fontsize=12)
    ax.legend(loc="lower right")
    fig.savefig(IMG / "churn_roc.png")
    plt.close(fig)
    return auc, lift


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    pipeline_scaling()
    compaction()
    late_corrections()
    congestion_heatmap()
    print("churn baseline AUC %.4f, top-decile lift %.2f" % churn_roc())
    print("charts written to", IMG)
