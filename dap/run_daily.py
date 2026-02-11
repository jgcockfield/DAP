from __future__ import annotations

import argparse
import uuid
from datetime import datetime

from dap.sheets.client import load_sheets_config
from dap.sheets.readers import read_all_prospects, read_contacted_emails
from dap.sheets.writers import append_run_log
from dap.crawler import run as crawl_urls

from dap.enrich import enrich
from dap.sheets.writers_enrich import apply_enrichment
from dap.email import send_emails


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without external side effects.")
    parser.add_argument("--no-email", action="store_true", help="Skip email stage (placeholder for now).")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of URLs to crawl (0 = no limit).")
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
        contacted_emails = set(read_contacted_emails([r for r in prospects if r.get("website_url")]))

        # normalize prospects into crawl items
        crawl_items = [{'url': row.get('website_url')} for row in prospects if row.get('website_url')]
        urls_seeded_count = len(crawl_items)

        if args.limit > 0:
            crawl_items = crawl_items[:args.limit]
            urls_seeded_count = len(crawl_items)

        # crawl step (stub)
        if not args.dry_run:
            crawl_results = crawl_urls(crawl_items)
        else:
            crawl_results = []
        sites_scraped_count = len(crawl_results)

        updates = enrich(prospects, crawl_results)
        enriched_count = len(updates)
        written_count = 0

        if not args.dry_run:
            written_count = apply_enrichment(cfg, updates)

        # email stage
        if not args.dry_run and not args.no_email:
            emails_sent_count = send_emails(cfg, prospects, updates, contacted_emails)

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
                    "enriched_count": str(enriched_count),
                    "written_count": str(written_count),
                    "emails_sent_count": str(emails_sent_count),
                    "errors_count": str(errors_count),
                    "top_error": top_error,
                },
            )
        else:
            print("[DRY-RUN] would append runs log row")

        print(f"seeded={urls_seeded_count} scraped={sites_scraped_count} enriched={enriched_count} written={written_count} emailed={emails_sent_count}")
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
                        "enriched_count": str(enriched_count),
                        "written_count": str(written_count),
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
