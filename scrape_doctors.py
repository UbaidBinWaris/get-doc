import hashlib
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional, Set
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
DOCTOR_PROFILE_PATH_PATTERN = re.compile(r"^/doctors/[^/]+-\d+$")


@dataclass
class DoctorRecord:
    source_url: str
    doctor_name: str
    clinic_name: Optional[str]
    phone_number: Optional[str]
    email: Optional[str]


def normalize_space(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned.strip() or None


def normalize_phone(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or None


def normalize_email(value: Optional[str]) -> Optional[str]:
    return value.strip().lower() if value else None


def dedupe_hash(doctor_name: str, clinic_name: Optional[str], phone_number: Optional[str], email: Optional[str]) -> str:
    key = "|".join(
        [
            normalize_space(doctor_name or "") or "",
            normalize_space(clinic_name or "") or "",
            normalize_phone(phone_number or "") or "",
            normalize_email(email or "") or "",
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def db_connect():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    if not user or not password or not db:
        raise RuntimeError("POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB must be set in .env")
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)


def ensure_schema(conn):
    sql_file = os.path.join(os.path.dirname(__file__), "sql", "001_create_doctors_table.sql")
    with open(sql_file, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def upsert_doctor(conn, record: DoctorRecord):
    row_hash = dedupe_hash(record.doctor_name, record.clinic_name, record.phone_number, record.email)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO doctors (
                source_url,
                doctor_name,
                clinic_name,
                phone_number,
                email,
                dedupe_hash,
                first_seen_at,
                last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (dedupe_hash)
            DO UPDATE SET
                source_url = EXCLUDED.source_url,
                doctor_name = EXCLUDED.doctor_name,
                clinic_name = COALESCE(EXCLUDED.clinic_name, doctors.clinic_name),
                phone_number = COALESCE(EXCLUDED.phone_number, doctors.phone_number),
                email = COALESCE(EXCLUDED.email, doctors.email),
                last_seen_at = NOW();
            """,
            (
                record.source_url,
                normalize_space(record.doctor_name),
                normalize_space(record.clinic_name),
                normalize_phone(record.phone_number),
                normalize_email(record.email),
                row_hash,
            ),
        )
    conn.commit()


def build_robots_parser(base_url: str) -> Optional[RobotFileParser]:
    robots_url = urljoin(base_url, "/robots.txt")
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        parser.read()
        return parser
    except Exception:
        logging.warning("Could not read robots.txt from %s.", robots_url)
        return None


def can_fetch(parser: Optional[RobotFileParser], user_agent: str, target_url: str) -> bool:
    if parser is None:
        return True
    return parser.can_fetch(user_agent, target_url)


def extract_text(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            txt = normalize_space(node.get_text(" ", strip=True))
            if txt:
                return txt
    return None


def extract_email(soup: BeautifulSoup) -> Optional[str]:
    mailto = soup.select_one('a[href^="mailto:"]')
    if mailto and mailto.get("href"):
        return normalize_email(mailto["href"].replace("mailto:", "").strip())
    m = EMAIL_PATTERN.search(soup.get_text(" ", strip=True))
    return normalize_email(m.group(0)) if m else None


def extract_phone(soup: BeautifulSoup) -> Optional[str]:
    tel = soup.select_one('a[href^="tel:"]')
    if tel and tel.get("href"):
        return normalize_phone(tel["href"].replace("tel:", "").strip())
    m = PHONE_PATTERN.search(soup.get_text(" ", strip=True))
    return normalize_phone(m.group(0)) if m else None


def parse_doctor_page(url: str, html: str) -> Optional[DoctorRecord]:
    soup = BeautifulSoup(html, "html.parser")

    name = extract_text(
        soup,
        [
            "h1",
            "[data-testid*='doctor-name']",
            "meta[property='og:title']",
            "title",
        ],
    )

    # Many provider pages store clinic/practice in labeled sections or address containers.
    clinic = extract_text(
        soup,
        [
            "[data-testid*='practice']",
            "[data-testid*='location']",
            "address",
            ".practice-name",
            ".office-name",
        ],
    )

    email = extract_email(soup)
    phone = extract_phone(soup)

    if not name:
        return None

    return DoctorRecord(
        source_url=url,
        doctor_name=name,
        clinic_name=clinic,
        phone_number=phone,
        email=email,
    )


def is_allowed_domain(target: str, allowed_netlocs: Set[str]) -> bool:
    return urlparse(target).netloc.lower() in allowed_netlocs


def looks_like_doctor_profile(url: str) -> bool:
    path = urlparse(url).path.lower()
    if "/doctor/" in path:
        return True
    return DOCTOR_PROFILE_PATH_PATTERN.match(path) is not None


def extract_candidate_links(current_url: str, html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        absolute = urljoin(current_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        clean = absolute.split("#")[0]
        links.add(clean)
    return links


def fetch_page(session: requests.Session, url: str, timeout: int) -> Optional[str]:
    max_retries = int(os.getenv("MAX_RETRIES", "2"))
    retry_backoff_seconds = float(os.getenv("RETRY_BACKOFF_SECONDS", "2"))

    total_attempts = max_retries + 1
    for attempt in range(1, total_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code != 200:
                logging.warning(
                    "Non-200 response %s for %s (attempt %d/%d)",
                    resp.status_code,
                    url,
                    attempt,
                    total_attempts,
                )
                return None
            return resp.text
        except requests.RequestException as exc:
            logging.warning(
                "Request failed for %s (attempt %d/%d): %s",
                url,
                attempt,
                total_attempts,
                exc,
            )
            if attempt < total_attempts:
                time.sleep(retry_backoff_seconds * attempt)
    return None


def push_links(queue: deque, visited: Set[str], allowed_netlocs: Set[str], current_url: str, html: str) -> tuple[int, int]:
    links = extract_candidate_links(current_url, html)
    added = 0
    for link in links:
        if is_allowed_domain(link, allowed_netlocs) and link not in visited:
            queue.append(link)
            added += 1
    return added, len(links)


def save_doctor_if_profile(conn, url: str, html: str) -> bool:
    if not looks_like_doctor_profile(url):
        return False

    record = parse_doctor_page(url, html)
    if not record:
        return False

    upsert_doctor(conn, record)
    logging.info(
        "Saved: %s | clinic=%s | phone=%s | email=%s",
        record.doctor_name,
        record.clinic_name,
        record.phone_number,
        record.email,
    )
    return True


def crawl_and_scrape():
    load_dotenv()

    start_url = os.getenv("START_URL", "https://health.usnews.com/doctors")
    user_agent = os.getenv("USER_AGENT", "DoctorDataBot/1.0 (+contact@example.com)")
    request_delay = float(os.getenv("REQUEST_DELAY_SECONDS", "1.5"))
    max_pages = int(os.getenv("MAX_PAGES", "500"))
    max_doctors = int(os.getenv("MAX_DOCTORS", "10000"))
    timeout = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
    allowed_netlocs = {
        n.strip().lower() for n in os.getenv("ALLOWED_NETLOCS", "health.usnews.com").split(",") if n.strip()
    }

    logging.info("Start URL: %s", start_url)
    logging.info("Max pages: %d, max doctors: %d", max_pages, max_doctors)
    logging.info("Allowed netlocs: %s", sorted(allowed_netlocs))

    base_url = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
    robots_parser = build_robots_parser(base_url)

    if not can_fetch(robots_parser, user_agent, start_url):
        raise RuntimeError("robots.txt blocks this crawler for the configured USER_AGENT.")

    headers = {"User-Agent": user_agent}
    session = requests.Session()
    session.headers.update(headers)

    conn = db_connect()
    ensure_schema(conn)

    queue = deque([start_url])
    visited: Set[str] = set()
    scraped_count = 0
    page_count = 0
    fetch_failures = 0

    while queue and page_count < max_pages and scraped_count < max_doctors:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not is_allowed_domain(url, allowed_netlocs):
            continue

        if not can_fetch(robots_parser, user_agent, url):
            logging.debug("Blocked by robots.txt: %s", url)
            continue

        html = fetch_page(session, url, timeout)
        page_count += 1
        if not html:
            fetch_failures += 1
            logging.warning("Skipping %s because HTML could not be fetched.", url)
            continue

        added, discovered = push_links(queue, visited, allowed_netlocs, url, html)
        logging.info(
            "Page %d: discovered %d links, queued %d links from %s",
            page_count,
            discovered,
            added,
            url,
        )

        if save_doctor_if_profile(conn, url, html):
            scraped_count += 1
            logging.info("Saved doctor count: %d", scraped_count)

        time.sleep(request_delay)

    conn.close()
    logging.info(
        "Done. Pages visited: %d, fetch failures: %d, doctors saved/updated: %d",
        page_count,
        fetch_failures,
        scraped_count,
    )


if __name__ == "__main__":
    load_dotenv()
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    crawl_and_scrape()
