from __future__ import annotations

import urllib.request
import urllib.error
import re

from dap.crawl_config import USER_AGENT


def run(items, timeout_s: int = 10):
    results = []
    for item in items:
        url = item.get("url")
        if not url:
            continue

        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                html = resp.read().decode("utf-8", errors="ignore")

                html = re.sub(r"<script.*?>.*?</script>", "", html, flags=re.S | re.I)
                html = re.sub(r"<style.*?>.*?</style>", "", html, flags=re.S | re.I)

                title = ""
                start = html.lower().find("<title>")
                end = html.lower().find("</title>")
                if start != -1 and end != -1 and end > start:
                    title = html[start + 7 : end].strip()

                description = ""
                marker = 'name="description"'
                idx = html.lower().find(marker)
                if idx != -1:
                    content_idx = html.lower().find('content=', idx)
                    if content_idx != -1:
                        quote = html[content_idx + 8]
                        endq = html.find(quote, content_idx + 9)
                        if endq != -1:
                            description = html[content_idx + 9 : endq].strip()

                results.append({
                    "url": url,
                    "status": resp.status,
                    "title": title,
                    "description": description,
                    "html": html,
                })
        except urllib.error.HTTPError as e:
            results.append({"url": url, "status": e.code})
        except Exception as e:
            results.append({"url": url, "status": "error", "error": str(e)[:200]})

    return results
