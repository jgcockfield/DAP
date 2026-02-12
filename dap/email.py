def send_emails(cfg, prospects, updates, contacted_emails):
    to_email = []

    for p in prospects:
        raw = p.get("all_emails") or p.get("primary_email") or ""
        emails = [e.strip() for e in raw.split(",") if e.strip()]

        for email in emails:
            if email in contacted_emails:
                continue
            if p.get("status") == "contacted":
                continue
            to_email.append({"prospect": p, "email": email})

    return to_email


