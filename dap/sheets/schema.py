# dap/sheets/schema.py

PROSPECT_SHEET_NAME_DEFAULT = "prospects"
RUNS_SHEET_NAME_DEFAULT = "runs"

# Canonical v1 columns for the `prospects` worksheet.
# Keep this order stable; it is the canonical export/migration order.
PROSPECT_COLUMNS_V1: list[str] = [
    "company_name",
    "website_url",
    "domain",
    "source_keyword",
    "contact_method",
    "primary_email",
    "all_emails",
    "form_url",
    "country",
    "city",
    "notes",
    "status",
    "last_checked_at",
]

# Optional v1.1+ columns (do not assume these exist in the sheet).
PROSPECT_COLUMNS_OPTIONAL_V11: list[str] = [
    "language",
    "category",
    "phone_numbers",
    "http_status",
    "scrape_error",
    "email_sent_at",
    "email_provider_message_id",
]

# Minimal columns for the `runs` worksheet.
RUNS_COLUMNS_V1: list[str] = [
    "run_id",
    "started_at",
    "finished_at",
    "urls_seeded_count",
    "sites_scraped_count",
    "emails_sent_count",
    "errors_count",
    "top_error",
]
