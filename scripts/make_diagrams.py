"""Render the architecture, medallion-flow and Cloudera-mapping diagrams (Pillow).

Counts in the medallion diagram come from docs/results/pipeline_scale1.json, so the picture
always shows the numbers of the last real run.
"""

from __future__ import annotations

import json

from PIL import Image, ImageDraw

from mfviz import (BG, BG2, GREEN, GREY, ICE, IMG, LINE, ORANGE, PANEL, PURPLE, RESULTS, WHITE, YELLOW,
                   font)


def canvas(w, h):
    im = Image.new("RGB", (w, h), BG)
    px = im.load()
    for y in range(h):
        for x in range(0, w, 1):
            t = (x / w * 0.6 + y / h * 0.4)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im, ImageDraw.Draw(im)


def box(d, xy, title, lines=(), accent=ICE, title_size=24, line_size=18):
    x0, y0, x1, y1 = xy
    d.rounded_rectangle(xy, radius=18, fill=PANEL, outline=accent, width=3)
    d.rounded_rectangle((x0, y0, x0 + 10, y1), radius=4, fill=accent)
    d.text((x0 + 26, y0 + 16), title, font=font(title_size, 750), fill=WHITE)
    y = y0 + 16 + title_size + 12
    for ln in lines:
        d.text((x0 + 26, y), ln, font=font(line_size, 450), fill=GREY)
        y += int(line_size * 1.45)


def arrow(d, a, b, color=GREY, label=None, width=3, at=0.5):
    d.line([a, b], fill=color, width=width)
    import math
    ang = math.atan2(b[1] - a[1], b[0] - a[0])
    for s in (2.6, -2.6):
        d.line([b, (b[0] - 16 * math.cos(ang + s / 6), b[1] - 16 * math.sin(ang + s / 6))], fill=color, width=width)
    if label:
        mx, my = a[0] + (b[0] - a[0]) * at, a[1] + (b[1] - a[1]) * at
        f = font(16, 600)
        w = d.textlength(label, font=f)
        d.rounded_rectangle((mx - w / 2 - 8, my - 14, mx + w / 2 + 8, my + 14), radius=10, fill=BG)
        d.text((mx - w / 2, my - 10), label, font=f, fill=color)


def architecture():
    im, d = canvas(1600, 900)
    d.text((60, 40), "MasterFabric Lakehouse Lab · architecture", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Five containers on one laptop. Spark and Trino share one Iceberg catalog and one object store.",
           font=font(20, 450), fill=GREY)
    box(d, (60, 170, 390, 400), "Landing zone", ["generator/telco_gen.py", "CSV.gz  · CRM, cells, CDR", "JSONL.gz · data sessions",
                                                 "defects injected on purpose", "_manifest.json = ground truth"], YELLOW)
    box(d, (470, 150, 960, 520), "Spark 3.5.5 + Iceberg 1.8.1", [
        "bronze.py   raw strings + lineage", "silver.py   incremental MERGE INTO", "gold.py     atomic INSERT OVERWRITE",
        "quality.py  20-rule gate", "iceberg_demos.py  evolution · time travel", "CellCongestionRanking.scala",
        "Jupyter · Spark History Server"], ORANGE)
    box(d, (1180, 150, 1560, 380), "Iceberg REST catalog", ["apache/iceberg-rest-fixture", "SQLite on a volume",
                                                            "namespaces: bronze · silver", "gold · ops"], PURPLE)
    box(d, (1180, 470, 1560, 760), "Object storage (S3 API)", ["RustFS 1.0.6 + console", "s3://warehouse/  Iceberg data",
                                                               "  Parquet + manifests", "  + metadata JSON",
                                                               "s3://landing/  raw zone"], ICE)
    box(d, (470, 600, 960, 830), "Trino 475", ["interactive MPP SQL (Impala role)", "Iceberg connector, same catalog",
                                                "time travel: FOR VERSION AS OF", "scripts/trino_queries.py"], GREEN)
    arrow(d, (390, 285), (470, 285), YELLOW)
    arrow(d, (960, 265), (1180, 265), PURPLE, "commit")
    arrow(d, (960, 470), (1180, 600), ICE, "S3FileIO", at=0.22)
    arrow(d, (960, 760), (1180, 720), ICE, "native S3")
    arrow(d, (960, 640), (1180, 380), PURPLE, "REST", at=0.78)
    d.text((60, 862), "All ports bound to 127.0.0.1 · S3 19000/19001 · catalog 18181 · Trino 18080 · Jupyter 18888 · Spark history 18090",
           font=font(18, 500), fill=GREY)
    im.save(IMG / "architecture.png", optimize=True)


def medallion():
    run = json.loads((RESULTS / "pipeline_scale1.json").read_text())
    inj, silver, br = run["injected"], run["layers"]["silver"], run["layers"]["bronze"]["feeds"]
    raw = {f["feed"]: f["rows"] for f in br}
    gold = run["layers"]["gold"]
    im, d = canvas(1600, 900)
    d.text((60, 40), "Medallion flow · real counts from the scale-1 run", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Every raw record is accounted for: duplicates collapse, bad records are quarantined with a reason.",
           font=font(20, 450), fill=GREY)
    cols = [
        ("Landing", YELLOW, [f"{raw['cdr']:,} CDR rows", f"{raw['sessions']:,} session rows",
                             f"{raw['subscribers']:,} subscribers", f"{raw['cells']:,} cells",
                             f"{inj['duplicates']:,} duplicates injected", f"{inj['late']:,} late arrivals"]),
        ("Bronze", ORANGE, ["all columns STRING", "+ _source_file, _batch_id", "+ _ingested_at",
                            "each file exactly once", "(ops.ingest_log)", f"{run['wall_seconds']['bronze']} s wall"]),
        ("Silver", ICE, [f"{silver['cdr']['valid']:,} CDRs", f"{silver['sessions']['valid']:,} sessions",
                         f"{silver['cdr']['quarantined'] + silver['sessions']['quarantined']:,} quarantined",
                         "typed · deduped · conformed", "MERGE INTO, newer wins", f"{run['wall_seconds']['silver']} s wall"]),
        ("Gold", GREEN, [f"{gold['daily_subscriber_usage_rows']:,} subscriber-days",
                         f"{gold['cell_hourly_load_rows']:,} cell-hours", f"{gold['churn_features_rows']:,} churn rows",
                         f"{gold['congested_cell_hours']} congested cell-hours", "atomic INSERT OVERWRITE",
                         f"{run['wall_seconds']['gold']} s wall"]),
    ]
    x = 60
    for i, (t, c, lines) in enumerate(cols):
        box(d, (x, 170, x + 330, 480), t, lines, c, title_size=30, line_size=21)
        if i < 3:
            arrow(d, (x + 330, 325), (x + 400, 325), GREY)
        x += 400
    q = run["layers"]["quality"]
    d.rounded_rectangle((60, 540, 1540, 800), radius=18, fill=PANEL, outline=LINE, width=2)
    d.text((90, 565), f"Quality gate: {q['passed']}/{q['rules']} rules passed", font=font(28, 750), fill=GREEN)
    reasons = silver["quarantine_by_reason"]
    y = 620
    for k, v in reasons.items():
        d.text((90, y), f"quarantine  {k:28}", font=font(19, 500, mono=True), fill=GREY)
        d.text((560, y), f"{v:>5}", font=font(19, 700, mono=True), fill=ORANGE)
        y += 30
    d.text((760, 620), f"duplicates removed = injected ({inj['duplicates']:,})", font=font(20, 500), fill=WHITE)
    d.text((760, 658), f"malformed MSISDNs quarantined = injected ({inj['bad_msisdn']})", font=font(20, 500), fill=WHITE)
    d.text((760, 696), "silver + quarantine = every distinct raw record", font=font(20, 500), fill=WHITE)
    d.text((760, 734), f"total pipeline wall time: {run['total_seconds']} s", font=font(20, 500), fill=WHITE)
    im.save(IMG / "medallion_flow.png", optimize=True)


def cloudera_mapping():
    rows = [
        ("Object storage", "RustFS (S3 API)", "HDFS / Apache Ozone (S3 gateway)", ICE),
        ("Table format", "Apache Iceberg v2", "Apache Iceberg v2 (GA in CDP)", ICE),
        ("Catalog", "Iceberg REST catalog", "Hive Metastore (+ Ranger, Atlas)", PURPLE),
        ("Batch / ETL", "Spark 3.5 local[4]", "Spark on YARN · Cloudera Data Engineering", ORANGE),
        ("Interactive SQL", "Trino 475", "Impala (CDW / Base) · Hive LLAP", GREEN),
        ("Notebooks", "Jupyter in the Spark image", "Cloudera Machine Learning · Hue", YELLOW),
        ("Orchestration", "make targets", "Oozie · Airflow in CDE · Control-M", GREY),
        ("Governance", "quality gate + ops tables", "Ranger policies · Atlas lineage · SDX", RED if False else GREY),
    ]
    im, d = canvas(1600, 900)
    d.text((60, 40), "How the lab maps to a Cloudera CDP estate", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Same table format and the same patterns; only the infrastructure underneath changes.",
           font=font(20, 450), fill=GREY)
    d.text((340, 150), "This lab", font=font(24, 750), fill=WHITE)
    d.text((900, 150), "Cloudera CDP equivalent", font=font(24, 750), fill=WHITE)
    y = 200
    for cap, lab, cdp, c in rows:
        d.text((60, y + 14), cap, font=font(20, 650), fill=GREY)
        d.rounded_rectangle((330, y, 780, y + 62), radius=14, fill=PANEL, outline=c, width=2)
        d.text((352, y + 17), lab, font=font(21, 600), fill=WHITE)
        arrow(d, (790, y + 31), (880, y + 31), c)
        d.rounded_rectangle((890, y, 1540, y + 62), radius=14, fill=PANEL, outline=LINE, width=2)
        d.text((912, y + 17), cdp, font=font(21, 600), fill=WHITE)
        y += 82
    im.save(IMG / "cloudera_mapping.png", optimize=True)


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    architecture()
    medallion()
    cloudera_mapping()
    print("diagrams written to", IMG)
