import argparse
import csv
import json
import logging
import re
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")

PROFILE_URL_HINTS = ["doctor", "doctors", "provider", "providers", "profile", "agent", "agents", "clinic"]

LINK_PRIORITY_HINTS = [
    "florida",
    "doctor",
    "provider",
    "agent",
    "directory",
    "clinic",
    "specialist",
]


@dataclass
class DoctorRecord:
    profile_url: str
    name: str
    post: Optional[str]
    working_place: Optional[str]
    email: Optional[str]
    phone: Optional[str]


def normalize_space(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    compact = " ".join(value.split())
    return compact.strip() or None


def normalize_phone(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return value.strip()


def find_first_text(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        text = normalize_space(node.get_text(" ", strip=True))
        if text:
            return text
    return None


def detect_cloudflare_challenge(html: str) -> bool:
    low = html.lower()
    return "just a moment" in low and "cf_chl" in low


def wait_for_cloudflare_clear(page, timeout_seconds: int, check_interval_seconds: float = 2.0) -> bool:
    end_at = time.time() + max(1, timeout_seconds)
    while time.time() < end_at:
        try:
            html = page.content()
        except Exception:
            time.sleep(check_interval_seconds)
            continue

        if not detect_cloudflare_challenge(html):
            return True

        time.sleep(check_interval_seconds)

    return False


def extract_emails(soup: BeautifulSoup, text: str) -> List[str]:
    found: Set[str] = set()
    for a in soup.select('a[href^="mailto:"]'):
        href = (a.get("href") or "").replace("mailto:", "").strip()
        for part in href.split("?"):
            if EMAIL_RE.fullmatch(part.strip()):
                found.add(part.strip().lower())
    for m in EMAIL_RE.findall(text):
        found.add(m.lower())
    return sorted(found)


def extract_phones(soup: BeautifulSoup, text: str) -> List[str]:
    found: Set[str] = set()
    for a in soup.select('a[href^="tel:"]'):
        href = (a.get("href") or "").replace("tel:", "").strip()
        if href:
            found.add(normalize_phone(href) or href)
    for m in PHONE_RE.findall(text):
        normalized = normalize_phone(m)
        if normalized:
            found.add(normalized)
    return sorted(found)


def parse_labeled_value(soup: BeautifulSoup, labels: Iterable[str]) -> Optional[str]:
    label_set = {x.lower() for x in labels}
    for node in soup.find_all(string=True):
        current = normalize_space(str(node))
        if not current:
            continue
        low = current.lower().strip(":")
        if low not in label_set:
            continue

        parent = node.parent
        if parent and parent.next_sibling:
            value = normalize_space(getattr(parent.next_sibling, "get_text", lambda *args, **kwargs: str(parent.next_sibling))(" ", strip=True))
            if value and value.lower() not in label_set:
                return value

        if parent:
            next_el = parent.find_next()
            if next_el:
                value = normalize_space(next_el.get_text(" ", strip=True))
                if value and value.lower() not in label_set:
                    return value
    return None


def is_florida_url(url: str) -> bool:
    """Return True if the URL is a Florida-specific directory page."""
    path = urlparse(url).path.lower()
    return (
        "florida" in path
        or "-fl-" in path
        or path.endswith("-fl")
        or "/fl/" in path
        or path.startswith("/fl-")
    )


def should_queue_url(url: str) -> bool:
    """Queue agent/agency profiles unconditionally; all other pages only if Florida-related."""
    path = urlparse(url).path.lower()
    if "/agents/" in path or "/agencies/" in path:
        return True
    return is_florida_url(url) or "medicare-agents-near-me" in path


def looks_like_florida(url: str, text: str) -> bool:
    url_l = url.lower()
    text_l = f" {text.lower()} "
    # URL-based: require proper FL state/city slug, not just any "-fl" suffix
    if "florida" in url_l or "-fl-" in url_l or "/fl/" in url_l or url_l.endswith("-fl"):
        return True
    # Text-based: full word "florida" only — weak hints like ", fl" cause false positives
    # from navigation menus that appear on every page of the site
    return " florida " in text_l or " florida," in text_l or " florida." in text_l


def looks_like_profile_page(url: str, soup: BeautifulSoup, text: str) -> bool:
    path = urlparse(url).path.lower()
    if any(hint in path for hint in PROFILE_URL_HINTS):
        return True
    h1 = find_first_text(soup, ["h1"])
    has_contact = bool(EMAIL_RE.search(text) or PHONE_RE.search(text))
    if h1 and has_contact:
        return True
    return False


def extract_record_from_page(url: str, html: str, is_florida_context: bool = False) -> Optional[DoctorRecord]:
    soup = BeautifulSoup(html, "html.parser")
    text = normalize_space(soup.get_text(" ", strip=True)) or ""

    if not looks_like_profile_page(url, soup, text):
        return None
    # Accept if provenance shows it was linked from a Florida page, OR text confirms Florida
    if not is_florida_context and not looks_like_florida(url, text):
        return None

    name = find_first_text(
        soup,
        [
            "h1",
            "[itemprop='name']",
            ".doctor-name",
            ".provider-name",
            "meta[property='og:title']",
            "title",
        ],
    )
    if name and "|" in name:
        name = normalize_space(name.split("|")[0])

    post = find_first_text(
        soup,
        [
            ".title",
            ".designation",
            ".position",
            "[itemprop='jobTitle']",
            ".provider-title",
        ],
    )

    working_place = find_first_text(
        soup,
        [
            ".clinic-name",
            ".practice-name",
            ".company-name",
            ".hospital-name",
            "[itemprop='worksFor']",
        ],
    )
    if not working_place:
        working_place = parse_labeled_value(
            soup,
            ["clinic", "practice", "hospital", "office", "workplace", "organization", "company"],
        )

    emails = extract_emails(soup, text)
    phones = extract_phones(soup, text)

    if not name:
        return None

    return DoctorRecord(
        profile_url=url,
        name=name,
        post=post,
        working_place=working_place,
        email=emails[0] if emails else None,
        phone=phones[0] if phones else None,
    )


def same_domain(target_url: str, allowed_domains: Set[str]) -> bool:
    host = urlparse(target_url).netloc.lower()
    return host in allowed_domains


def extract_links(current_url: str, html: str, allowed_domains: Set[str]) -> Tuple[List[str], List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    priority: Set[str] = set()
    normal: Set[str] = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(current_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        clean = absolute.split("#")[0]
        if not same_domain(clean, allowed_domains):
            continue

        low = clean.lower()
        if any(h in low for h in LINK_PRIORITY_HINTS):
            priority.add(clean)
        else:
            normal.add(clean)

    return sorted(priority), sorted(normal)


def save_records_csv(path: Path, records: List[DoctorRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name", "post", "working_place", "email", "phone", "profile_url"],
        )
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))


def save_records_json(path: Path, records: List[DoctorRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(r) for r in records]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_scraper(
    base_url: str,
    output_csv: Path,
    output_json: Path,
    max_pages: int,
    max_records: int,
    delay_seconds: float,
    headless: bool,
    start_paths: List[str],
    challenge_wait_seconds: int,
    user_data_dir: Path,
) -> None:
    parsed_base = urlparse(base_url)
    allowed_domains = {parsed_base.netloc.lower()}
    if parsed_base.netloc.startswith("www."):
        allowed_domains.add(parsed_base.netloc.replace("www.", "", 1))
    else:
        allowed_domains.add("www." + parsed_base.netloc)

    queue = deque(urljoin(base_url, p) for p in start_paths)
    visited: Set[str] = set()
    records_by_key: Dict[str, DoctorRecord] = {}
    # Track agent URLs discovered on Florida directory pages for provenance-based filtering
    florida_agent_urls: Set[str] = set()

    with sync_playwright() as pw:
        user_data_dir.mkdir(parents=True, exist_ok=True)
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.new_page()
        if not headless:
            page.bring_to_front()

        while queue and len(visited) < max_pages and len(records_by_key) < max_records:
            url = queue.popleft()
            if url in visited:
                continue
            visited.add(url)

            logging.info("Visiting %s", url)
            try:
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
            except PlaywrightTimeoutError:
                logging.warning("Timeout while loading %s", url)
                continue
            except Exception as exc:
                if "interrupted by another navigation" in str(exc):
                    # Site did a JS redirect mid-load — wait for the new page to settle
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=30_000)
                    except Exception:
                        logging.warning("Could not settle after redirect from %s", url)
                        continue
                else:
                    logging.warning("Failed to load %s: %s", url, exc)
                    continue

            try:
                page.wait_for_timeout(int(delay_seconds * 1000))
                html = page.content()
                final_url = page.url
            except Exception as exc:
                logging.warning("Could not get content from %s: %s", url, exc)
                continue

            # Mark redirect destination as visited so we don't re-crawl it separately
            if final_url != url and final_url not in visited:
                visited.add(final_url)

            if detect_cloudflare_challenge(html):
                logging.warning(
                    "Cloudflare challenge detected. Solve it in the opened browser window. "
                    "Waiting up to %s seconds for it to clear...",
                    challenge_wait_seconds,
                )

                cleared = wait_for_cloudflare_clear(page, timeout_seconds=challenge_wait_seconds)
                if not cleared:
                    logging.warning(
                        "Challenge did not clear for %s within timeout. Skipping this page.",
                        url,
                    )
                    continue

                try:
                    page.wait_for_timeout(int(delay_seconds * 1000))
                    html = page.content()
                    final_url = page.url
                except Exception as exc:
                    logging.warning("Could not continue after challenge on %s: %s", url, exc)
                    continue

            # Tag agent URLs discovered on Florida directory pages for provenance tracking
            page_is_florida = is_florida_url(final_url) or is_florida_url(url)
            priority_links, normal_links = extract_links(final_url, html, allowed_domains)
            if page_is_florida:
                for lnk in priority_links + normal_links:
                    if "/agents/" in urlparse(lnk).path.lower():
                        florida_agent_urls.add(lnk.lower())

            agent_is_florida = (
                url.lower() in florida_agent_urls
                or final_url.lower() in florida_agent_urls
                or page_is_florida
            )
            record = extract_record_from_page(final_url, html, is_florida_context=agent_is_florida)
            if record:
                key = "|".join(
                    [
                        normalize_space(record.name or "") or "",
                        normalize_space(record.email or "") or "",
                        normalize_space(record.phone or "") or "",
                        normalize_space(record.working_place or "") or "",
                    ]
                )
                if key not in records_by_key:
                    records_by_key[key] = record
                    logging.info(
                        "Saved record #%d: %s | %s | %s",
                        len(records_by_key),
                        record.name,
                        record.email or "no-email",
                        record.phone or "no-phone",
                    )

            for nxt in priority_links + normal_links:
                if nxt not in visited and should_queue_url(nxt):
                    queue.append(nxt)

            if delay_seconds > 0:
                time.sleep(delay_seconds)

        context.close()

    records = sorted(records_by_key.values(), key=lambda r: (r.name.lower(), r.profile_url.lower()))
    save_records_csv(output_csv, records)
    save_records_json(output_json, records)

    logging.info("Done. Pages visited: %d", len(visited))
    logging.info("Florida doctor records saved: %d", len(records))
    logging.info("CSV: %s", output_csv)
    logging.info("JSON: %s", output_json)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Florida doctors from medicareagentshub.com and export name/post/workplace/email/phone."
    )
    parser.add_argument("--base-url", default="https://medicareagentshub.com")
    parser.add_argument("--output-csv", default="data/florida_doctors.csv")
    parser.add_argument("--output-json", default="data/florida_doctors.json")
    parser.add_argument("--max-pages", type=int, default=1500)
    parser.add_argument("--max-records", type=int, default=50000)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode.")
    parser.add_argument(
        "--challenge-wait-seconds",
        type=int,
        default=600,
        help="How long to wait for Cloudflare challenge to clear in browser before skipping page.",
    )
    parser.add_argument(
        "--user-data-dir",
        default=".pw-user-data",
        help="Persistent browser profile directory to reuse cookies/session between runs.",
    )
    parser.add_argument(
        "--start-paths",
        nargs="+",
        default=[
            "/medicare-agents-near-me/florida",
            "/miami-fl-medicare-agents",
            "/tampa-fl-medicare-agents",
            "/orlando-fl-medicare-agents",
            "/jacksonville-fl-medicare-agents",
            "/fort-lauderdale-fl-medicare-agents",
            "/st-petersburg-fl-medicare-agents",
            "/hialeah-fl-medicare-agents",
            "/cape-coral-fl-medicare-agents",
            "/tallahassee-fl-medicare-agents",
        ],
        help="Initial paths to seed the crawler queue.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    run_scraper(
        base_url=args.base_url,
        output_csv=Path(args.output_csv),
        output_json=Path(args.output_json),
        max_pages=args.max_pages,
        max_records=args.max_records,
        delay_seconds=args.delay_seconds,
        headless=args.headless,
        start_paths=args.start_paths,
        challenge_wait_seconds=args.challenge_wait_seconds,
        user_data_dir=Path(args.user_data_dir),
    )


if __name__ == "__main__":
    main()