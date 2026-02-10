from __future__ import annotations

import urllib.request
import urllib.error

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
                results.append({"url": url, "status": resp.status, "html": html})
        except urllib.error.HTTPError as e:
            results.append({"url": url, "status": e.code})
        except Exception as e:
            results.append({"url": url, "status": "error", "error": str(e)[:200]})

    return results
