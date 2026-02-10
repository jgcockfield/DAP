def run(items):
    # items: list[dict] with at least {'url': str}
    results = []
    for item in items:
        url = item.get('url')
        if not url:
            continue
        results.append({'url': url, 'status': 'seeded'})
    return results
