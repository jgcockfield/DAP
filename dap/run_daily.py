from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime
from urllib.parse import urlparse

from dotenv import load_dotenv

from dap.crawler import run as crawl_urls
from dap.discovery.search_seed import discover
from dap.email import mark_sent, send_emails
from dap.enrich import enrich
from dap.sheets.client import load_sheets_config
from dap.sheets.readers import read_all_prospects
from dap.sheets.writers import append_run_log, upsert_prospects
from dap.sheets.writers_enrich import apply_enrichment

load_dotenv()


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run without external side effects.")
    parser.add_argument("--no-email", action="store_true", help="Skip email stage.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of URLs to crawl (0 = no limit).")
    parser.add_argument("--live", action="store_true", help="Actually send emails (safety gate).")
    parser.add_argument("--max-emails", type=int, default=5, help="Max emails to process per run.")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    started_at = utc_now_iso()

    urls_seeded_count = 0
    sites_scraped_count = 0
    enriched_count = 0
    written_count = 0
    emails_sent_count = 0
    errors_count = 0
    top_error = ""

    try:
        cfg = load_sheets_config()

        # Phase 1: Discovery
        discovered = discover(cfg, dry_run=args.dry_run)
        print(f"discovered={len(discovered)}")

        prospects = read_all_prospects(cfg)

        # Phase 1b: Seed discovered domains into prospects (domain-level dedupe)
        existing_domains = {
            (p.get("domain", "") or "").strip().lower()
            for p in prospects
            if (p.get("domain", "") or "").strip()
        }

        rows_to_seed = []
        for d in discovered:
            dom = (d.get("domain", "") or "").strip().lower()
            if not dom or dom in existing_domains:
                continue

            rows_to_seed.append(
                {
                    "domain": dom,
                    "website_url": d.get("url", ""),
                    "company_name": d.get("title") or d.get("name") or d.get("company_name") or "",
                    "source_keyword": d.get("source_keyword", ""),
                    "status": "discovered",
                    "notes": f"seeded via serper query={d.get('query', '')}",
                }
            )

        if args.dry_run:
            seeded_count = len(rows_to_seed)
        else:
            seeded_count = upsert_prospects(cfg, rows_to_seed, key="domain")

        print(f"seeded_discovery={seeded_count}")

        # Reload prospects so newly seeded rows enter crawl phase
        if not args.dry_run and seeded_count > 0:
            prospects = read_all_prospects(cfg)

        # Phase 2: Build crawl items (only those without a primary email yet)
        crawl_items = [
            {"url": row.get("website_url"), "domain": (row.get("domain") or "")}
            for row in prospects
            if row.get("website_url") and not (row.get("primary_email") or "").strip()
        ]

        # Phase 2.x: domain-level dedupe before crawling
        seen = set()
        deduped = []
        for it in crawl_items:
            dom = (it.get("domain") or "").strip().lower()
            if not dom:
                dom = urlparse(it["url"]).netloc.strip().lower()
            key = dom or it["url"].strip().lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append({"url": it["url"], "domain": dom})
        crawl_items = deduped

        if args.limit > 0:
            crawl_items = crawl_items[: args.limit]

        urls_seeded_count = len(crawl_items)

        # Crawl
        crawl_results = crawl_urls(crawl_items) if not args.dry_run else []
        sites_scraped_count = len([r for r in crawl_results if isinstance(r, dict)])

        # Enrich + writeback
        updates = enrich(prospects, crawl_results) if crawl_results else []
        enriched_count = len(updates)

        if not args.dry_run:
            written_count = apply_enrichment(cfg, updates)

        # Email (dry-run summary)
        if args.dry_run and not args.no_email:
            email_result = send_emails(cfg, prospects, updates)
            would_email = email_result.get("to_email", [])
            print(f"[DRY-RUN] would_queue_emails={len(would_email)} (max_emails={args.max_emails})")

        # Daily global cap (UTC day)
        daily_cap = int(os.getenv("DAILY_EMAIL_CAP", "50") or "50")
        today = datetime.utcnow().date().isoformat()
        sent_today = 0
        for p in prospects:
            ts = (p.get("sent_at") or p.get("last_emailed_at") or "").strip()
            if ts[:10] == today:
                sent_today += 1
        remaining_today = max(0, daily_cap - sent_today)

        # Email (live)
        if not args.dry_run and not args.no_email and args.live and remaining_today > 0:
            email_result = send_emails(cfg, prospects, updates)
            to_email = email_result.get("to_email", [])

            # per-run cap AND daily cap
            to_email = to_email[: min(args.max_emails, remaining_today)]
            emails_sent_count = 0

            for item in to_email:
                prospect = item.get("prospect")
                website_url = prospect.get("website_url") if prospect else None
                if not website_url:
                    continue

                if item.get("send_status") == "sent":
                    apply_enrichment(cfg, [mark_sent(website_url, item.get("sendgrid_message_id"))])
                    emails_sent_count += 1

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

        print(
            f"seeded={seeded_count} scraped={sites_scraped_count} enriched={enriched_count} written={written_count} emailed={emails_sent_count}"
        )
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