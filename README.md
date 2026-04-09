# Medicare Agents Hub Scraper (Florida Focus)

This project is now focused on scraping:

- https://medicareagentshub.com

Target output fields:

- name
- post
- working_place
- email
- phone
- profile_url

Only Florida doctors/providers are included.

## Important Note

The site is protected by Cloudflare challenge pages. Direct HTTP scraping returns `403`.

This scraper uses Playwright (real browser automation), so you can solve the challenge once in the browser window and continue scraping.

The browser profile is now persistent by default, so cookies/session are reused across runs to reduce repeated challenge failures.

## Setup

1. Create and activate virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```bash
python scrape_doctors.py
```

If challenge pages appear, keep the browser window open and solve them there. The script now waits automatically and resumes when the challenge clears.

By default it:

- starts from key paths (`/`, `/doctors`, `/providers`, `/directory`, `/search`, `/florida`)
- crawls internal links only
- keeps Florida-related profiles only
- writes outputs to:
  - `data/florida_doctors.csv`
  - `data/florida_doctors.json`

## Useful Flags

```bash
python scrape_doctors.py \
  --max-pages 3000 \
  --max-records 100000 \
  --delay-seconds 1.2 \
  --start-paths / /doctors /providers /directory /florida
```

For unattended runs (if your environment already passes challenge):

```bash
python scrape_doctors.py --headless
```

If Cloudflare takes longer on your network, increase wait time:

```bash
python scrape_doctors.py --challenge-wait-seconds 1200
```

To reuse a stable browser profile between runs (default already enabled):

```bash
python scrape_doctors.py --user-data-dir .pw-user-data
```

## Output Quality

Extraction focuses on your required fields only:

- name: from `h1`/profile title
- post: title/designation/job title selectors
- working_place: clinic/practice/workplace selectors and labeled fallback
- email: `mailto:` and page text regex
- phone: `tel:` and page text regex

Records are deduplicated by `(name, email, phone, working_place)`.
