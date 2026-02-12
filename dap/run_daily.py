from __future__ import annotations

import argparse
import uuid
from datetime import datetime
from urllib.parse import urlparse

from dap.sheets.client import load_sheets_config
from dap.sheets.readers import read_all_prospects, read_contacted_emails
from dap.sheets.writers import append_run_log, upsert_prospects
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
    enriched_count = 0
    written_count = 0

    try:
        cfg = load_sheets_config()

        # Phase 1: Discovery (stub wiring)
        from dap.discovery.search_seed import discover

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

        contacted_emails = set(read_contacted_emails(prospects))

        # Phase 2: Build crawl items
        # MINIMAL FIX: only crawl rows that still need email enrichment
        crawl_items = [
            {"url": row.get("website_url"), "domain": (row.get("domain") or "")}
            for row in prospects
            if row.get("website_url") and not (row.get("primary_email") or "").strip()
        ]

        # Phase 2.x: domain-level dedupe before crawling
        _seen = set()
        _deduped = []
        for it in crawl_items:
            dom = (it.get("domain") or "").strip().lower()
            if not dom:
                dom = urlparse(it["url"]).netloc.strip().lower()
            key = dom or it["url"].strip().lower()
            if key in _seen:
                continue
            _seen.add(key)
            _deduped.append({"url": it["url"], "domain": dom})
        crawl_items = _deduped

        if args.limit > 0:
            crawl_items = crawl_items[: args.limit]

        urls_seeded_count = len(crawl_items)
        # crawl step
        if not args.dry_run:
            crawl_results = crawl_urls(crawl_items)
        else:
            crawl_results = []

        sites_scraped_count = len([r for r in crawl_results if isinstance(r, dict)])

        updates = enrich(prospects, crawl_results) if crawl_results else []
        enriched_count = len(updates)

        if not args.dry_run:
            written_count = apply_enrichment(cfg, updates)

        # email stage
        if not args.dry_run and not args.no_email:
            to_email = send_emails(cfg, prospects, updates, contacted_emails)
            emails_sent_count = len(to_email)
            print("SEND_QUEUE_SAMPLE:", [(x["prospect"].get("company_name"), x["email"]) for x in to_email[:20]])

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
