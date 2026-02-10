def enrich(prospects, crawl_results):
    updates = []

    by_url = {r["url"]: r for r in crawl_results}

    for p in prospects:
        url = p.get("website_url")
        if not url or url not in by_url:
            continue

        r = by_url[url]
        updates.append({
            "website_url": url,
            "page_title": r.get("title", ""),
            "page_description": r.get("description", ""),
            "last_checked_at": "now",  # placeholder
        })

    return updates