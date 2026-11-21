"""Screenshots for the lineage and data-quality additions (stack + lineage profile must be up).

- Marquez: dataset lineage graph of gold.daily_subscriber_usage.
- Great Expectations Data Docs: the index, and the failed validation of the buggy delivery.
Output: docs/screenshots/*.png, 1440 px wide.
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

from screenshots import ENV, OUT, ROOT, shot

WEB = f"http://127.0.0.1:{ENV.get('PORT_MARQUEZ_WEB', '13100')}"
DOCS = ROOT / ".out" / "gx" / "gx" / "uncommitted" / "data_docs" / "local_site"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bad = next((DOCS / "validations" / "silver_cdr_audit_20261008_b").rglob("*.html"))
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        page.goto(f"{WEB}/lineage/dataset/s3%3A%2F%2Fwarehouse/gold%2Fdaily_subscriber_usage")
        page.wait_for_timeout(6000)
        shot(page, "marquez_lineage_gold")
        page.goto(bad.as_uri())
        page.wait_for_timeout(1500)
        page.get_by_text("Failed Only").click()  # show only the expectations the batch failed
        page.wait_for_timeout(800)
        # drop the fixed newsletter banner so it does not cut through a full-page capture
        page.evaluate("document.querySelectorAll('*').forEach(e => {"
                      "if (getComputedStyle(e).position === 'fixed') e.remove(); })")
        shot(page, "gx_failed_validation", full=True)
        page.goto((DOCS / "index.html").as_uri())
        page.wait_for_timeout(1500)
        shot(page, "gx_data_docs_index")
        b.close()


if __name__ == "__main__":
    main()
