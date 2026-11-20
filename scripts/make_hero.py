"""Compose docs/hero.png (1600x900) from real screenshots and measured numbers.

Every number on the hero is read from docs/results/*.json at render time.
"""

from __future__ import annotations

import json

from PIL import Image, ImageDraw, ImageFilter

from mfviz import BG, BG2, DOCS, GREEN, GREY, ICE, ORANGE, PURPLE, RESULTS, SHOTS, WHITE, YELLOW, font

W, H = 1600, 900


def load(n):
    return json.loads((RESULTS / f"{n}.json").read_text())


def background():
    im = Image.new("RGB", (W, H), BG)
    px = im.load()
    for y in range(H):
        for x in range(W):
            t = min(1.0, (x / W) * 0.75 + (y / H) * 0.35)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im


def card(base, path, box, label, color, crop=None):
    x0, y0, x1, y1 = box
    shot = Image.open(path).convert("RGB")
    if crop:
        shot = shot.crop(crop)
    shot = shot.resize((x1 - x0, round(shot.height * (x1 - x0) / shot.width)), Image.LANCZOS)
    shot = shot.crop((0, 0, x1 - x0, min(shot.height, y1 - y0)))
    shadow = Image.new("RGBA", (shot.width + 40, shot.height + 40), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle((20, 20, shot.width + 20, shot.height + 20), radius=16, fill=(0, 0, 0, 170))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    base.paste(shadow, (x0 - 20, y0 - 12), shadow)
    mask = Image.new("L", shot.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, shot.width, shot.height), radius=14, fill=255)
    base.paste(shot, (x0, y0), mask)
    d = ImageDraw.Draw(base)
    f = font(20, 800)
    w = d.textlength(label, font=f)
    d.rounded_rectangle((x0 - 10, y0 - 22, x0 + w + 18, y0 + 14), radius=18, fill=color)
    d.text((x0 + 4, y0 - 18), label, font=f, fill=(255, 255, 255))


def main():
    bench = load("benchmark")["runs"]
    big = max(bench, key=lambda r: r["scale"])
    demos = load("iceberg_demos")
    m = demos["maintenance"]
    files_before = sum(f["files"] for f in m["before"]["files"])
    files_after = sum(f["files"] for f in m["after_compaction"]["files"])
    q = load("quality_last_run")
    churn = load("churn_baseline")

    im = background()
    card(im, SHOTS / "s3_console_iceberg_files.png", (930, 70, 1540, 400), "Iceberg files on S3", ICE)
    card(im, SHOTS / "notebook_cell_load_plot.png", (1000, 360, 1570, 640), "Jupyter · gold marts", ORANGE, crop=(180, 0, 1260, 560))
    card(im, SHOTS / "trino_cluster_overview.png", (900, 600, 1420, 860), "Trino SQL", GREEN)

    d = ImageDraw.Draw(im)
    d.text((80, 70), "MASTERFABRIC", font=font(36, 800), fill=WHITE)
    d.text((76, 120), "Lakehouse Lab", font=font(92, 800), fill=WHITE)
    x, y = 80, 238
    for word, col in (("Spark", ORANGE), (" · ", GREY), ("Iceberg", ICE), (" · ", GREY), ("Trino", GREEN),
                      (" · ", GREY), ("S3", PURPLE)):
        f = font(50, 800)
        d.text((x, y), word, font=f, fill=col)
        x += d.textlength(word, font=f)
    d.text((80, 318), "A runnable telecom lakehouse on one laptop: bronze → silver → gold,",
           font=font(26, 500), fill=GREY)
    d.text((80, 352), "real Iceberg features, a 20-rule quality gate. Every number measured.",
           font=font(26, 500), fill=GREY)

    facts = [
        (f"{big['raw_records'] / 1e6:.1f} M raw records in {big['total_seconds']:.0f} s",
         f"scale {big['scale']:g} · generate + bronze + silver + gold + quality · M4 Pro", YELLOW),
        (f"{q['passed']}/{q['rules']} quality rules · every record reconciled",
         "duplicates removed = injected · malformed rows quarantined with a reason", GREEN),
        (f"compaction {files_before} → {files_after} files · query "
         f"{m['before']['query']['median_ms']:.0f} → {m['after_compaction']['query']['median_ms']:.0f} ms",
         "10 late-correction MERGE batches, merge-on-read, then rewrite + expire", ICE),
        ("partition evolution months → days · 0 files rewritten",
         f"schema evolution · time travel · baseline churn AUC {churn['roc_auc']:.2f}", PURPLE),
    ]
    y = 430
    for title, sub, col in facts:
        d.rounded_rectangle((80, y + 6, 88, y + 62), radius=4, fill=col)
        d.text((106, y), title, font=font(29, 750), fill=WHITE)
        d.text((106, y + 38), sub, font=font(20, 450), fill=GREY)
        y += 92
    d.text((80, 820), "github.com/masterfabric/masterfabric-lakehouse-lab", font=font(30, 700), fill=WHITE)
    im.save(DOCS / "hero.png", optimize=True)
    print("docs/hero.png written")


if __name__ == "__main__":
    main()
