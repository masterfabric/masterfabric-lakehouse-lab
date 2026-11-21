"""Images for write-audit-publish, the Great Expectations gate and lineage, from docs/results.

- docs/images/wap_flow.png      branch/audit/publish/rollback flow with the measured counts and times
- docs/images/gx_results.png    the 11 expectations for both deliveries, pass/fail and observed values
- docs/images/lineage_graph.png dataset lineage recorded by OpenLineage, laid out by layer
"""

from __future__ import annotations

import json

from make_diagrams import arrow, box, canvas
from mfviz import GREEN, GREY, HEX, ICE, IMG, ORANGE, PURPLE, RESULTS, WHITE, YELLOW, font, mpl_setup

RED = (248, 81, 73)
W = json.loads((RESULTS / "wap.json").read_text())
LIN = json.loads((RESULTS / "lineage.json").read_text())


def n(x):
    return f"{x:,}"


def wap_flow():
    g, b, rb = W["good_delivery"], W["buggy_delivery"], W["rollback"]
    im, d = canvas(1600, 900)
    d.text((60, 40), "Write-audit-publish on Iceberg branches · silver.cdr", font=font(34, 800), fill=WHITE)
    d.text((60, 88), "Measured run: one good delivery, one from a buggy release, one accidental delete.",
           font=font(20, 450), fill=GREY)
    box(d, (50, 170, 330, 330), "main", [f"{n(g['main_before'])} rows", "tag before_wap"], ICE)
    # good path
    box(d, (420, 150, 760, 330), "branch audit_…_a", [f"+{n(g['branch_rows_new_day'])} rows (release 2.3)",
                                                      f"write {g['write_s']} s", "main sees 0 of them"], GREEN)
    a = g["audit"]
    box(d, (850, 150, 1150, 330), "GX audit", [f"{a['passed']}/{a['evaluated']} expectations pass",
                                               f"{g['audit_s']} s"], GREEN)
    box(d, (1240, 150, 1550, 330), "publish", ["fast_forward main", f"{g['publish_s']} s",
                                               f"main = {n(g['main_after'])} rows"], GREEN)
    arrow(d, (330, 240), (420, 240), GREEN)
    arrow(d, (760, 240), (850, 240), GREEN)
    arrow(d, (1150, 240), (1240, 240), GREEN)
    # buggy path
    a = b["audit"]
    box(d, (420, 400, 760, 600), "branch audit_…_b", [f"+{n(W['buggy_branch_kept_for_debugging']['null_city'])} rows (release 2.4)",
                                                      "charges ×100, city lost", f"write {b['write_s']} s"], RED)
    failed = [r for r in a["results"] if not r["success"]]
    box(d, (850, 400, 1150, 600), "GX audit", [f"{a['passed']}/{a['evaluated']} pass · {len(failed)} fail",
                                               "city null: 100%", f"mean charge {failed[-1]['observed']:.1f}",
                                               f"{b['audit_s']} s"], RED)
    box(d, (1240, 400, 1550, 600), "not published", ["branch kept to debug", f"main still {n(b['main_after'])}",
                                                     "consumers never saw it"], RED)
    arrow(d, (330, 300), (420, 480), RED)
    arrow(d, (760, 500), (850, 500), RED)
    arrow(d, (1150, 500), (1240, 500), RED)
    # rollback
    box(d, (420, 680, 760, 850), "accidental DELETE", ["directly on main",
                                                       f"{n(rb['rows_before'])} → {n(rb['rows_after_accidental_delete'])} rows"], ORANGE)
    box(d, (850, 680, 1150, 850), "rollback_to_snapshot", [f"tag {rb['tag']}", f"{rb['rollback_s']} s"], ORANGE)
    box(d, (1240, 680, 1550, 850), "restored", [f"{n(rb['rows_after_rollback'])} rows", "no data copied"], ORANGE)
    arrow(d, (760, 765), (850, 765), ORANGE)
    arrow(d, (1150, 765), (1240, 765), ORANGE)
    im.save(IMG / "wap_flow.png", optimize=True)


def gx_results():
    plt = mpl_setup()
    rows = W["good_delivery"]["audit"]["results"]
    bad = W["buggy_delivery"]["audit"]["results"]
    names = []
    for r in rows:
        col = r["kwargs"].get("column")
        label = r["expectation"].replace("expect_", "").replace("column_values_to_", "").replace("_", " ")
        names.append(f"{col}: {label}" if col else label)
    fig, ax = plt.subplots(figsize=(14, 6.4))
    for j, res in enumerate((rows, bad)):
        for i, r in enumerate(res):
            ok = r["success"]
            ax.add_patch(plt.Rectangle((j + 0.04, i + 0.08), 0.92, 0.84, color=HEX["GREEN"] if ok else HEX["RED"],
                                       alpha=0.85 if not ok else 0.55))
            obs = r["observed"]
            if r.get("unexpected_percent") not in (None, 0, 0.0):
                txt = f"{r['unexpected_percent']:.1f}% rows fail"
            elif isinstance(obs, float):
                txt = f"observed {obs:.3f}"
            elif "row_count" in r["expectation"]:
                txt = f"{obs:,} rows"
            else:
                txt = "0 unexpected"
            ax.text(j + 0.5, i + 0.5, ("pass · " if ok else "FAIL · ") + txt, ha="center", va="center",
                    fontsize=10, color=HEX["WHITE"], fontweight="bold" if not ok else "normal")
    ax.set_xlim(0, 2); ax.set_ylim(len(rows), 0)
    ax.set_xticks([0.5, 1.5], ["delivery from release 2.3 → published", "delivery from release 2.4 → blocked"])
    ax.set_yticks([i + 0.5 for i in range(len(rows))], names, fontsize=10)
    ax.tick_params(length=0); ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_title("Great Expectations audit of each branch: 11 expectations, last three learned from main",
                 loc="left", fontsize=13)
    fig.tight_layout()
    fig.savefig(IMG / "gx_results.png", dpi=150)


LAYER_X = {"landing": 60, "bronze": 410, "silver": 760, "gold": 1140}
JOB_COL = {"bronze": ORANGE, "silver": ICE, "gold": YELLOW, "wap": PURPLE, "quality": GREEN}


def lineage_graph():
    edges = [e for e in LIN["dataset_edges"] if e["to"].split("/")[0] in LAYER_X]
    nodes = sorted({e["from"] for e in edges} | {e["to"] for e in edges})
    by_layer = {}
    for nd in nodes:
        by_layer.setdefault(nd.split("/")[0], []).append(nd)
    im, d = canvas(1600, 900)
    d.text((60, 36), "Lineage recorded by OpenLineage from the Spark jobs (Marquez export)", font=font(32, 800), fill=WHITE)
    d.text((60, 82), f"{LIN['jobs_recorded']} jobs and {LIN['datasets_recorded']} datasets recorded · "
           f"{len(LIN['dataset_edges'])} dataset edges · column lineage on {len(LIN['column_lineage'])} datasets",
           font=font(20, 450), fill=GREY)
    pos = {}
    for layer, xs in LAYER_X.items():
        items = by_layer.get(layer, [])
        top = 150 + (620 - 92 * len(items)) // 2
        for k, nd in enumerate(items):
            y = top + k * 92
            pos[nd] = (xs, y)
            d.rounded_rectangle((xs, y, xs + 310, y + 62), radius=10, fill=(24, 30, 46), outline=(52, 62, 86), width=2)
            d.text((xs + 14, y + 18), nd, font=font(19, 650), fill=WHITE)
    for e in edges:
        (x0, y0), (x1, y1) = pos[e["from"]], pos[e["to"]]
        col = JOB_COL.get(e["job"], GREY)
        if x0 == x1:  # same-layer dependency (gold reads gold, silver joins silver dimensions)
            d.line([(x0 + 310, y0 + 31), (x0 + 335, y0 + 31), (x0 + 335, y1 + 31), (x1 + 310, y1 + 31)], fill=col, width=2)
        else:
            arrow(d, (x0 + 310, y0 + 31), (x1, y1 + 31), col, width=2)
    x = 60
    for job, col in JOB_COL.items():
        if job == "quality":
            continue
        d.rounded_rectangle((x, 800, x + 22, 822), radius=4, fill=col)
        d.text((x + 32, 798), f"{job} job", font=font(18, 600), fill=GREY)
        x += 180
    tr = LIN["trace"]["gold/churn_features.drop_rate"]
    d.text((800, 790), "Column lineage, gold churn_features.drop_rate ←", font=font(18, 650), fill=WHITE)
    d.text((800, 818), ", ".join(t.replace(":", ".") for t in tr if "bronze" in t or "silver" in t), font=font(16, 450), fill=GREY)
    im.save(IMG / "lineage_graph.png", optimize=True)


if __name__ == "__main__":
    wap_flow()
    gx_results()
    lineage_graph()
    print("wap_flow.png gx_results.png lineage_graph.png written")
