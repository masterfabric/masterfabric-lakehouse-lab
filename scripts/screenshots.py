"""Capture real UI screenshots of the running lab with Playwright (headless Chromium).

Credentials are read from .env and typed into the S3 console login form; they are never
printed or written anywhere. Output: docs/screenshots/*.png (1440x900 at 2x, then
downscaled to 1440 wide).
"""

from __future__ import annotations

import json
import subprocess
import urllib.request
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if line and not line.startswith("#") and "=" in line)
S3 = f"http://127.0.0.1:{ENV.get('PORT_S3_CONSOLE', '19001')}"
TRINO = f"http://127.0.0.1:{ENV.get('PORT_TRINO', '18080')}"
HIST = f"http://127.0.0.1:{ENV.get('PORT_SPARK_HISTORY', '18090')}"


def shrink(path: Path, width: int = 1440) -> None:
    im = Image.open(path)
    if im.width > width:
        im.resize((width, round(im.height * width / im.width)), Image.LANCZOS).save(path, optimize=True)


def shot(page, name: str, full: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    shrink(path)
    print("saved", path.relative_to(ROOT))


def latest_app(name: str) -> str:
    apps = json.loads(urllib.request.urlopen(f"{HIST}/api/v1/applications").read())
    return next(a["id"] for a in apps if a["name"] == f"mf-lakehouse:{name}")


def s3_console(page) -> None:
    page.goto(f"{S3}/rustfs/console/auth/login/")
    page.fill("input[name=accessKey]", ENV["LAKE_S3_ACCESS_KEY"])
    page.fill("input[name=secretKey]", ENV["LAKE_S3_SECRET_KEY"])
    page.keyboard.press("Enter")
    page.wait_for_url("**/browser/**")
    page.wait_for_timeout(4500)  # let the login toast disappear
    shot(page, "s3_console_buckets")
    page.goto(f"{S3}/rustfs/console/browser/?bucket=warehouse")
    page.wait_for_timeout(1500)
    for folder in ("silver/", "cdr/", "data/"):
        page.get_by_text(folder, exact=True).first.click()
        page.wait_for_timeout(1500)
    page.get_by_text("event_time_day=2026-10-05/", exact=True).first.click()
    page.wait_for_timeout(1800)
    shot(page, "s3_console_iceberg_files")


def trino_ui(page) -> None:
    import trino

    cur = trino.dbapi.connect(host="127.0.0.1", port=int(ENV.get("PORT_TRINO", 18080)), user="lab",
                              catalog="lake").cursor()
    cur.execute("""SELECT c.city, count(*) AS congested_cell_hours, round(avg(l.utilization), 3) AS avg_util
                   FROM lake.gold.cell_hourly_load l JOIN lake.silver.cells c ON l.cell_id = c.cell_id
                   WHERE l.is_congested GROUP BY 1 ORDER BY 2 DESC""")
    cur.fetchall()
    qid = cur.query_id
    page.goto(f"{TRINO}/ui/login.html")
    page.fill("input[name=username]", "lab")
    page.keyboard.press("Enter")
    page.wait_for_timeout(2500)
    finished = page.get_by_text("Finished", exact=True)
    if finished.count():
        finished.first.click()
        page.wait_for_timeout(2500)
    shot(page, "trino_cluster_overview")
    page.goto(f"{TRINO}/ui/query.html?{qid}")
    page.wait_for_timeout(3000)
    shot(page, "trino_query_detail")


def spark_history(page) -> None:
    page.goto(f"{HIST}/")
    page.wait_for_timeout(2500)
    shot(page, "spark_history_apps")
    app = latest_app("silver")
    page.goto(f"{HIST}/history/{app}/SQL/")
    page.wait_for_timeout(2500)
    shot(page, "spark_silver_sql_queries")
    sql = json.loads(urllib.request.urlopen(f"{HIST}/api/v1/applications/{app}/sql?details=false&length=200").read())
    merge = max((q for q in sql if "MergeRows" in q.get("planDescription", "") or "MERGE" in q.get("description", "")),
                key=lambda q: q.get("duration", 0), default=max(sql, key=lambda q: q.get("duration", 0)))
    page.goto(f"{HIST}/history/{app}/SQL/execution/?id={merge['id']}")
    page.wait_for_timeout(3500)
    shot(page, "spark_silver_merge_plan")


def notebook(page) -> None:
    html = ROOT / "notebooks" / "lakehouse_tour.html"
    subprocess.run(["docker", "exec", "-w", "/lab", "mf-lake-spark", "jupyter", "nbconvert", "--to", "html",
                    "notebooks/lakehouse_tour.ipynb"], check=True, capture_output=True)
    page.goto(html.as_uri())
    page.wait_for_timeout(1500)
    page.evaluate("document.querySelectorAll('h2')[3].scrollIntoView()")
    page.wait_for_timeout(500)
    shot(page, "notebook_time_travel")
    page.evaluate("document.querySelectorAll('h2')[4].scrollIntoView()")
    page.wait_for_timeout(500)
    shot(page, "notebook_cell_load_plot")
    html.unlink()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        for step in (s3_console, trino_ui, spark_history, notebook):
            step(page)
        browser.close()


if __name__ == "__main__":
    main()
