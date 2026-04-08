# Doctor Data Scraper

This repository now has two data pipelines:

- [scrape_doctors.py](scrape_doctors.py): website crawler for directories you are permitted to crawl.
- [ingest_providers.py](ingest_providers.py): legal-first official ingestion pipeline.

Primary fields stored:

- doctor_name
- clinic_name
- phone_number
- email
- npi
- specialty
- address fields

## 1) Install

1. Create a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## 2) Configure .env

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=doc_user
POSTGRES_PASSWORD=doc_password
POSTGRES_DB=doc_db

# General logging
LOG_LEVEL=INFO

# Scraper options
START_URL=https://health.usnews.com/doctors
USER_AGENT=DoctorDataBot/1.0 (+youremail@example.com)
REQUEST_DELAY_SECONDS=1.5
REQUEST_TIMEOUT_SECONDS=30
MAX_PAGES=500
MAX_DOCTORS=10000
ALLOWED_NETLOCS=health.usnews.com,www.usnews.com
MAX_RETRIES=3
RETRY_BACKOFF_SECONDS=3

# Multi-source ingestion options
NPPES_INDEX_URL=https://download.cms.gov/nppes/NPI_Files.html
# Optional direct ZIP override:
# NPPES_BULK_ZIP_URL=https://download.cms.gov/nppes/....zip
NPI_API_STATE=NY
NPI_API_CITY=NEW YORK
# Optional direct NPI lookup (preferred for API tests):
# NPI_API_NUMBER=1234567890
NPI_API_LIMIT=200
# Optional for cms_csv source:
# CMS_PROVIDER_CSV_URL=https://....csv
```

## 3) Run Official Multi-Source Ingestion

Use this first for scale and legal reliability:

```bash
python ingest_providers.py --sources nppes_bulk,npi_api --max-rows 0
```

Examples:

```bash
# quick validation
python ingest_providers.py --sources nppes_bulk --max-rows 5000

# API-only by state
python ingest_providers.py --sources npi_api --max-rows 2000

# CMS CSV if CMS_PROVIDER_CSV_URL is configured
python ingest_providers.py --sources cms_csv --max-rows 0
```

Implemented source adapters:

- nppes_bulk
- npi_api
- cms_csv

NPI API note:

- Use either `NPI_API_NUMBER`, or both `NPI_API_STATE` and `NPI_API_CITY`.
- For complete national data ingestion, use `nppes_bulk`.

Protected/permissioned sources are intentionally not force-scraped:

- healthgrades
- zocdoc
- webmd
- yelp
- kaggle

For these, use official APIs, exported files, or explicit permission.

## 4) Run Website Scraper

```bash
python scrape_doctors.py
```

## 5) Duplicate Prevention

Records are deduplicated via:

- source + external_id when available
- deterministic SHA-256 dedupe hash over normalized identity/location/contact fields

## 6) Notes

- This project does not implement anti-bot bypass.
- Use official datasets for high-volume ingestion.
- SQL migrations are auto-applied in lexical order from [sql](sql).
