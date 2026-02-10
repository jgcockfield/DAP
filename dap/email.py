def send_emails(cfg, prospects, updates, contacted_emails):
    to_email = []
    for p in prospects:
        email = p.get("primary_email")
        if not email:
            continue
        if email in contacted_emails:
            continue
        if p.get("status") == "contacted":
            continue
        to_email.append(p)
    return len(to_email)
