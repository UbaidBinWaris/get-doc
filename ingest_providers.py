import argparse
import csv
import hashlib
import logging
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


NPPES_INDEX_URL = "https://download.cms.gov/nppes/NPI_Files.html"
NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"


@dataclass
class ProviderRecord:
    source: str
    external_id: str
    source_url: str
    doctor_name: str
    clinic_name: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    npi: Optional[str] = None
    specialty: Optional[str] = None
    address_line1: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None


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
    if not value:
        return None
    return value.strip().lower()


def dedupe_hash(record: ProviderRecord) -> str:
    key = "|".join(
        [
            normalize_space(record.source) or "",
            normalize_space(record.external_id) or "",
            normalize_space(record.npi) or "",
            normalize_space(record.doctor_name) or "",
            normalize_space(record.clinic_name) or "",
            normalize_phone(record.phone_number) or "",
            normalize_email(record.email) or "",
            normalize_space(record.address_line1) or "",
            normalize_space(record.city) or "",
            normalize_space(record.state) or "",
            normalize_space(record.postal_code) or "",
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
    sql_dir = Path(__file__).parent / "sql"
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        raise RuntimeError("No SQL migration files were found in sql directory.")

    with conn.cursor() as cur:
        for file in sql_files:
            ddl = file.read_text(encoding="utf-8")
            cur.execute(ddl)
    conn.commit()


def update_if_source_external_exists(cur, record: ProviderRecord) -> bool:
    cur.execute(
        """
        UPDATE doctors
        SET
            source_url = %s,
            doctor_name = %s,
            clinic_name = COALESCE(%s, clinic_name),
            phone_number = COALESCE(%s, phone_number),
            email = COALESCE(%s, email),
            npi = COALESCE(%s, npi),
            specialty = COALESCE(%s, specialty),
            address_line1 = COALESCE(%s, address_line1),
            city = COALESCE(%s, city),
            state = COALESCE(%s, state),
            postal_code = COALESCE(%s, postal_code),
            country = COALESCE(%s, country),
            last_seen_at = NOW()
        WHERE source = %s AND external_id = %s
        """,
        (
            record.source_url,
            normalize_space(record.doctor_name),
            normalize_space(record.clinic_name),
            normalize_phone(record.phone_number),
            normalize_email(record.email),
            normalize_space(record.npi),
            normalize_space(record.specialty),
            normalize_space(record.address_line1),
            normalize_space(record.city),
            normalize_space(record.state),
            normalize_space(record.postal_code),
            normalize_space(record.country),
            normalize_space(record.source),
            normalize_space(record.external_id),
        ),
    )
    return cur.rowcount > 0


def insert_or_merge_by_dedupe(cur, record: ProviderRecord):
    row_hash = dedupe_hash(record)
    cur.execute(
        """
        INSERT INTO doctors (
            source,
            external_id,
            source_url,
            doctor_name,
            clinic_name,
            phone_number,
            email,
            npi,
            specialty,
            address_line1,
            city,
            state,
            postal_code,
            country,
            dedupe_hash,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (dedupe_hash)
        DO UPDATE SET
            source = COALESCE(EXCLUDED.source, doctors.source),
            external_id = COALESCE(EXCLUDED.external_id, doctors.external_id),
            source_url = EXCLUDED.source_url,
            doctor_name = EXCLUDED.doctor_name,
            clinic_name = COALESCE(EXCLUDED.clinic_name, doctors.clinic_name),
            phone_number = COALESCE(EXCLUDED.phone_number, doctors.phone_number),
            email = COALESCE(EXCLUDED.email, doctors.email),
            npi = COALESCE(EXCLUDED.npi, doctors.npi),
            specialty = COALESCE(EXCLUDED.specialty, doctors.specialty),
            address_line1 = COALESCE(EXCLUDED.address_line1, doctors.address_line1),
            city = COALESCE(EXCLUDED.city, doctors.city),
            state = COALESCE(EXCLUDED.state, doctors.state),
            postal_code = COALESCE(EXCLUDED.postal_code, doctors.postal_code),
            country = COALESCE(EXCLUDED.country, doctors.country),
            last_seen_at = NOW()
        """,
        (
            normalize_space(record.source),
            normalize_space(record.external_id),
            record.source_url,
            normalize_space(record.doctor_name),
            normalize_space(record.clinic_name),
            normalize_phone(record.phone_number),
            normalize_email(record.email),
            normalize_space(record.npi),
            normalize_space(record.specialty),
            normalize_space(record.address_line1),
            normalize_space(record.city),
            normalize_space(record.state),
            normalize_space(record.postal_code),
            normalize_space(record.country),
            row_hash,
        ),
    )


def upsert_provider(cur, record: ProviderRecord):
    if not normalize_space(record.source) or not normalize_space(record.external_id):
        raise ValueError("source and external_id are required")
    if not normalize_space(record.doctor_name):
        return

    if update_if_source_external_exists(cur, record):
        return
    insert_or_merge_by_dedupe(cur, record)


def normalized_header_map(fieldnames: Sequence[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for field in fieldnames:
        key = re.sub(r"[^a-z0-9]", "", field.lower())
        mapping[key] = field
    return mapping


def row_get(row: Dict[str, str], header_map: Dict[str, str], candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        normalized = re.sub(r"[^a-z0-9]", "", candidate.lower())
        original_key = header_map.get(normalized)
        if original_key and row.get(original_key):
            return row.get(original_key)
    return None


def full_name_from_nppes(row: Dict[str, str], header_map: Dict[str, str]) -> Optional[str]:
    first = row_get(row, header_map, ["Provider First Name"])
    middle = row_get(row, header_map, ["Provider Middle Name"])
    last = row_get(row, header_map, ["Provider Last Name (Legal Name)"])
    suffix = row_get(row, header_map, ["Provider Name Suffix Text"])
    parts = [p for p in [first, middle, last, suffix] if normalize_space(p)]
    if parts:
        return " ".join(parts)
    return row_get(row, header_map, ["Provider Organization Name (Legal Business Name)"])


def nppes_row_to_record(row: Dict[str, str], header_map: Dict[str, str], zip_url: str) -> Optional[ProviderRecord]:
    entity_type = row_get(row, header_map, ["Entity Type Code"])
    if entity_type and entity_type.strip() != "1":
        return None

    npi = row_get(row, header_map, ["NPI"])
    name = full_name_from_nppes(row, header_map)
    if not npi or not name:
        return None

    return ProviderRecord(
        source="nppes_bulk",
        external_id=npi,
        source_url=zip_url,
        doctor_name=name,
        clinic_name=row_get(row, header_map, ["Provider Organization Name (Legal Business Name)"]),
        phone_number=row_get(
            row,
            header_map,
            ["Provider Business Practice Location Address Telephone Number"],
        ),
        email=None,
        npi=npi,
        specialty=row_get(row, header_map, ["Healthcare Provider Taxonomy Code_1"]),
        address_line1=row_get(
            row,
            header_map,
            ["Provider First Line Business Practice Location Address"],
        ),
        city=row_get(row, header_map, ["Provider Business Practice Location Address City Name"]),
        state=row_get(row, header_map, ["Provider Business Practice Location Address State Name"]),
        postal_code=row_get(row, header_map, ["Provider Business Practice Location Address Postal Code"]),
        country=row_get(row, header_map, ["Provider Business Practice Location Address Country Code (If outside U.S.)"]),
    )


def find_latest_nppes_zip_link(session: requests.Session, index_url: str) -> str:
    resp = session.get(index_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        href_l = href.lower()
        is_zip = href_l.endswith(".zip")
        is_dissemination = "nppes_data_dissemination" in href_l
        is_report = "deactivated" in href_l or "reactivated" in href_l or "endpoint" in href_l
        if is_zip and is_dissemination and not is_report:
            links.append(requests.compat.urljoin(index_url, href))

    if not links:
        raise RuntimeError("Could not find a dissemination NPPES ZIP link on the CMS NPPES index page.")

    links.sort(reverse=True)
    return links[0]


def download_file(session: requests.Session, url: str, output_path: Path):
    with session.get(url, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        with output_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def find_csv_in_zip(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError("No CSV file found inside NPPES ZIP archive.")
        csv_members.sort()
        return csv_members[0]


def ingest_nppes_bulk(cur, session: requests.Session, max_rows: int):
    index_url = os.getenv("NPPES_INDEX_URL", NPPES_INDEX_URL)
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    zip_url = os.getenv("NPPES_BULK_ZIP_URL") or find_latest_nppes_zip_link(session, index_url)
    zip_path = data_dir / Path(urlparse_like_filename(zip_url)).name

    logging.info("Downloading NPPES bulk file: %s", zip_url)
    if not zip_path.exists():
        download_file(session, zip_url, zip_path)
    else:
        logging.info("Using cached bulk file: %s", zip_path)

    csv_member = find_csv_in_zip(zip_path)
    logging.info("Processing CSV member: %s", csv_member)

    processed = 0
    saved = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_member, "r") as raw:
            text = (line.decode("utf-8", errors="ignore") for line in raw)
            reader = csv.DictReader(text)
            if not reader.fieldnames:
                raise RuntimeError("NPPES CSV appears to have no headers.")

            header_map = normalized_header_map(reader.fieldnames)
            for row in reader:
                processed += 1
                if max_rows > 0 and processed > max_rows:
                    break

                rec = nppes_row_to_record(row, header_map, zip_url)
                if not rec:
                    continue

                upsert_provider(cur, rec)
                saved += 1

                if saved % 1000 == 0:
                    logging.info("NPPES processed=%d saved=%d", processed, saved)

    logging.info("NPPES done. processed=%d saved=%d", processed, saved)


def urlparse_like_filename(url: str) -> str:
    if "/" not in url:
        return url
    return url.rsplit("/", 1)[-1]


def npi_api_name(result: dict) -> Optional[str]:
    basic = result.get("basic") or {}
    first = normalize_space(basic.get("first_name"))
    last = normalize_space(basic.get("last_name"))
    org = normalize_space(basic.get("organization_name"))
    if first or last:
        return " ".join(p for p in [first, last] if p)
    return org


def npi_api_primary_address(result: dict) -> dict:
    addresses = result.get("addresses") or []
    for addr in addresses:
        if (addr.get("address_purpose") or "").lower() == "location":
            return addr
    return addresses[0] if addresses else {}


def npi_api_primary_taxonomy(result: dict) -> Optional[str]:
    taxonomies = result.get("taxonomies") or []
    for t in taxonomies:
        if t.get("primary") is True and t.get("desc"):
            return t.get("desc")
    for t in taxonomies:
        if t.get("desc"):
            return t.get("desc")
    return None


def npi_api_result_to_record(result: dict) -> Optional[ProviderRecord]:
    number = str(result.get("number") or "").strip()
    name = npi_api_name(result)
    if not number or not name:
        return None

    address = npi_api_primary_address(result)
    return ProviderRecord(
        source="npi_api",
        external_id=number,
        source_url=f"{NPI_API_URL}?version=2.1&number={number}",
        doctor_name=name,
        clinic_name=normalize_space((result.get("basic") or {}).get("organization_name")),
        phone_number=address.get("telephone_number"),
        email=None,
        npi=number,
        specialty=npi_api_primary_taxonomy(result),
        address_line1=address.get("address_1"),
        city=address.get("city"),
        state=address.get("state"),
        postal_code=address.get("postal_code"),
        country=address.get("country_code"),
    )


def build_npi_base_params() -> Optional[Dict[str, str]]:
    number = os.getenv("NPI_API_NUMBER", "").strip()
    state = os.getenv("NPI_API_STATE", "").strip()
    city = os.getenv("NPI_API_CITY", "").strip()

    params: Dict[str, str] = {"version": "2.1"}
    if number:
        params["number"] = number
        return params
    if state and city:
        params["state"] = state
        params["city"] = city
        return params
    return None


def fetch_npi_payload(session: requests.Session, base_params: Dict[str, str], limit: int, skip: int) -> dict:
    params = dict(base_params)
    params["limit"] = str(limit)
    params["skip"] = str(skip)
    resp = session.get(NPI_API_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def process_npi_results(cur, results: Sequence[dict], max_rows: int, saved: int) -> int:
    for result in results:
        rec = npi_api_result_to_record(result)
        if not rec:
            continue
        upsert_provider(cur, rec)
        saved += 1
        if max_rows > 0 and saved >= max_rows:
            break
    return saved


def ingest_npi_api(cur, session: requests.Session, max_rows: int):
    limit = min(int(os.getenv("NPI_API_LIMIT", "200")), 200)
    skip = 0
    saved = 0

    base_params = build_npi_base_params()
    if not base_params:
        logging.warning(
            "Skipping npi_api: set NPI_API_NUMBER, or set both NPI_API_STATE and NPI_API_CITY. "
            "For full national coverage, use nppes_bulk."
        )
        return

    is_number_lookup = "number" in base_params

    while True:
        payload = fetch_npi_payload(session, base_params, limit, skip)

        if payload.get("Errors"):
            logging.warning("NPI API returned validation errors: %s", payload.get("Errors"))
            break

        results = payload.get("results") or []
        if not results:
            break

        saved = process_npi_results(cur, results, max_rows, saved)
        if max_rows > 0 and saved >= max_rows:
            logging.info("NPI API done early due to max_rows=%d", max_rows)
            return

        logging.info("NPI API skip=%d batch=%d saved=%d", skip, len(results), saved)
        skip += limit

        if is_number_lookup:
            # Number lookup does not need pagination loops.
            break

    logging.info("NPI API done. saved=%d", saved)


def ingest_cms_csv(cur, source_name: str, csv_url: str, max_rows: int):
    # Generic adapter for official downloadable CMS CSV files.
    resp = requests.get(csv_url, timeout=120)
    resp.raise_for_status()

    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    if not reader.fieldnames:
        raise RuntimeError("CMS CSV has no headers")

    saved = 0
    for row in reader:
        if max_rows > 0 and saved >= max_rows:
            break

        name = row.get("doctor_name") or row.get("name") or row.get("physician_name")
        external = row.get("npi") or row.get("id") or row.get("provider_id")
        if not name or not external:
            continue

        rec = ProviderRecord(
            source=source_name,
            external_id=str(external),
            source_url=csv_url,
            doctor_name=str(name),
            clinic_name=row.get("clinic_name") or row.get("organization_name"),
            phone_number=row.get("phone") or row.get("phone_number"),
            email=row.get("email"),
            npi=row.get("npi"),
            specialty=row.get("specialty"),
            address_line1=row.get("address") or row.get("address_line1"),
            city=row.get("city"),
            state=row.get("state"),
            postal_code=row.get("zip") or row.get("postal_code"),
            country=row.get("country"),
        )
        upsert_provider(cur, rec)
        saved += 1

    logging.info("CMS CSV source=%s done. saved=%d", source_name, saved)


def run_blocked_scrape_notice(source: str):
    logging.warning(
        "Source '%s' is not executed in this tool because forcing anti-bot bypass is not supported. Use official API/export permissions instead.",
        source,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legal multi-source provider ingestion pipeline")
    parser.add_argument(
        "--sources",
        default="nppes_bulk,npi_api",
        help="Comma-separated: nppes_bulk,npi_api,cms_csv,healthgrades,zocdoc,webmd,yelp,kaggle",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap per source (0 means no cap)",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args()
    sources = [s.strip().lower() for s in args.sources.split(",") if s.strip()]

    conn = db_connect()
    ensure_schema(conn)

    session = requests.Session()
    session.headers.update({"User-Agent": os.getenv("USER_AGENT", "ProviderDataPipeline/1.0")})

    try:
        with conn.cursor() as cur:
            for source in sources:
                logging.info("Starting source: %s", source)
                if source == "nppes_bulk":
                    ingest_nppes_bulk(cur, session, args.max_rows)
                elif source == "npi_api":
                    ingest_npi_api(cur, session, args.max_rows)
                elif source == "cms_csv":
                    csv_url = os.getenv("CMS_PROVIDER_CSV_URL", "").strip()
                    if not csv_url:
                        logging.warning("Skipping cms_csv: set CMS_PROVIDER_CSV_URL in .env")
                    else:
                        ingest_cms_csv(cur, "cms_csv", csv_url, args.max_rows)
                elif source in {"healthgrades", "zocdoc", "webmd", "yelp", "kaggle"}:
                    run_blocked_scrape_notice(source)
                else:
                    logging.warning("Unknown source skipped: %s", source)

                conn.commit()
                logging.info("Completed source: %s", source)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
