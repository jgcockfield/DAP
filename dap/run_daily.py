# dap/run_daily.py

from __future__ import annotations

import argparse
import uuid
from datetime import datetime

from dap.sheets.client import load_sheets_config
from dap.sheets.readers import read_all_prospects, read_contacted_emails
from dap.sheets.writers import append_run_log


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without external side effects.")
    parser.add_argument("--no-email", action="store_true", help="Skip email stage (placeholder for now).")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    started_at = utc_now_iso()

    urls_seeded_count = 0
    sites_scraped_count = 0
    emails_sent_count = 0
    errors_count = 0
    top_error = ""

    try:
        cfg = load_sheets_config()

        prospects = read_all_prospects(cfg)
        urls_seeded_count = len(prospects)
        _contacted = read_contacted_emails(prospects)

        # Future stages: discovery, scraping, enrichment, email

        finished_at = utc_now_iso()

        if not args.dry_run:
            append_run_log(
                cfg,
                {
                    "run_id": run_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "urls_seeded_count": str(urls_seeded_count),
                    "sites_scraped_count": str(sites_scraped_count),
                    "emails_sent_count": str(emails_sent_count),
                    "errors_count": str(errors_count),
                    "top_error": top_error,
                },
            )
        else:
            print("[DRY-RUN] would append runs log row")

        print(f"run_id={run_id} dry_run={args.dry_run} prospects_rows={len(prospects)}")
        return 0

    except Exception as e:
        errors_count += 1
        top_error = str(e)
        finished_at = utc_now_iso()

        try:
            if not args.dry_run:
                cfg = load_sheets_config()
                append_run_log(
                    cfg,
                    {
                        "run_id": run_id,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "urls_seeded_count": str(urls_seeded_count),
                        "sites_scraped_count": str(sites_scraped_count),
                        "emails_sent_count": str(emails_sent_count),
                        "errors_count": str(errors_count),
                        "top_error": top_error[:200],
                    },
                )
        except Exception:
            pass

        print(f"ERROR run_id={run_id} err={top_error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

from dap.crawler import run as crawl_urls

# crawl step\ncrawl_results = crawl_urls(prospects)

# normalize prospects\ncrawl_items = [{'url': row['url']} for row in prospects]

crawl_results = crawl_urls(crawl_items)
