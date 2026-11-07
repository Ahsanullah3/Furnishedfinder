import os
import sys
import json
import re
import time
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests as curl_requests
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Sheet1")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))          # lowered for FlareSolverr
BATCH_INDEX = int(os.environ.get("BATCH_INDEX", "0"))
AUTO_CHAIN = os.environ.get("AUTO_CHAIN", "true").lower() == "true"
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", "1.0"))

# FlareSolverr endpoint (runs inside the GitHub Actions job)
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")
USE_FLARESOLVERR = os.environ.get("USE_FLARESOLVERR", "true").lower() == "true"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Input: Sheet1 column A, starting row 2
URL_COLUMN = "A"
URL_START_ROW = 2

# Output: 14 fields starting at column O (O..AB)
OUT_COLS = [
    "property_id", "title", "price", "bedrooms", "bathrooms", "location",
    "landlord_name", "landlord_tenure", "email_verified", "phone_verified",
    "id_verified", "about", "status", "scraped_at",
]
OUT_START_COL_IDX = 15
OUT_END_COL_IDX = OUT_START_COL_IDX + len(OUT_COLS) - 1


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


OUT_START_LETTER = col_letter(OUT_START_COL_IDX)
OUT_END_LETTER = col_letter(OUT_END_COL_IDX)
OUT_RANGE_ROW = f"{OUT_START_LETTER}{{row}}:{OUT_END_LETTER}{{row}}"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(threadName)-14s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ff-scraper")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("gspread").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------
def get_sheet():
    log.info("Connecting to sheet id=%s worksheet=%s", SHEET_ID, WORKSHEET_NAME)
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    try:
        return sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        raise SystemExit(f"Worksheet {WORKSHEET_NAME!r} not found.")


def read_urls(ws) -> list:
    col_values = ws.col_values(1)
    urls = []
    for i, val in enumerate(col_values, start=1):
        if i < URL_START_ROW:
            continue
        url = (val or "").strip()
        if url:
            urls.append((i, url))
    log.info("Read %d URLs from %s!%s%d", len(urls), ws.title, URL_COLUMN, URL_START_ROW)
    return urls


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def extract_next_data(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        return {}


def deep_find(obj, *keys):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and v not in (None, "", [], {}):
                return v
            found = deep_find(v, *keys)
            if found not in (None, "", [], {}):
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = deep_find(item, *keys)
            if found not in (None, "", [], {}):
                return found
    return None


def parse_property(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    data = extract_next_data(html)

    title = deep_find(data, "listingName", "propertyName", "name", "title", "headline") if data else None
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

    price = deep_find(data, "price", "monthlyPrice", "rentAmount", "pricePerMonth") if data else None
    if not price:
        m = re.search(r"\$[\d,]+", soup.get_text(" "))
        price = m.group(0) if m else ""

    bedrooms = deep_find(data, "bedrooms", "beds", "bedroomCount") if data else ""
    bathrooms = deep_find(data, "bathrooms", "baths", "bathroomCount") if data else ""

    card = soup.find("div", attrs={"role": "region"})
    landlord_name, location = "", ""
    email_verified = phone_verified = id_verified = False

    if card:
        h3 = card.find("h3")
        if h3:
            landlord_name = h3.get_text(strip=True)
        for span in card.find_all("span"):
            txt = span.get_text(" ", strip=True)
            if re.fullmatch(r"[A-Za-z .'-]+,\s*[A-Z]{2}", txt or ""):
                location = txt
                break
        card_text = card.get_text(" ", strip=True)
        email_verified = "Email verified" in card_text
        phone_verified = "Phone number verified" in card_text
        id_verified = "ID verified" in card_text

    tenure = ""
    for li in soup.find_all("li"):
        label = li.find("span", class_=re.compile("font-semibold"))
        if label and label.get_text(strip=True).lower() == "tenure":
            tenure = li.get_text(" ", strip=True).replace(label.get_text(strip=True), "", 1).strip()
            break

    about = ""
    about_p = soup.find("p", class_=re.compile("whitespace-pre-wrap"))
    if about_p:
        about = about_p.get_text("\n", strip=True)

    return {
        "title": title or "",
        "price": str(price) if price is not None else "",
        "bedrooms": str(bedrooms) if bedrooms is not None else "",
        "bathrooms": str(bathrooms) if bathrooms is not None else "",
        "location": location,
        "landlord_name": landlord_name,
        "landlord_tenure": tenure,
        "email_verified": "YES" if email_verified else "NO",
        "phone_verified": "YES" if phone_verified else "NO",
        "id_verified": "YES" if id_verified else "NO",
        "about": about[:1000],
    }


def extract_property_id(url: str) -> str:
    m = re.search(r"/property/(\d+)", url)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# HTTP fetch: curl_cffi first, FlareSolverr fallback on block
# ---------------------------------------------------------------------------
_thread_local = threading.local()


def _get_session():
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = curl_requests.Session()
        _thread_local.session = s
    return s


def _looks_blocked(html: str) -> bool:
    return (
        "__NEXT_DATA__" not in html
        or "Just a moment" in html
        or "cf-challenge" in html.lower()
        or "Attention Required" in html
    )


def _fetch_via_curl_cffi(url: str) -> str:
    session = _get_session()
    r = session.get(url, impersonate="chrome", timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    if _looks_blocked(r.text):
        raise RuntimeError("curl_cffi blocked (challenge page)")
    return r.text


def _fetch_via_flaresolverr(url: str) -> str:
    if not USE_FLARESOLVERR:
        raise RuntimeError("FlareSolverr disabled")
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": REQUEST_TIMEOUT * 1000,
    }
    log.info("FlareSolverr solving: %s", url)
    r = requests.post(FLARESOLVERR_URL, json=payload, timeout=REQUEST_TIMEOUT + 30)
    data = r.json()
    if data.get("status") != "ok":
        raise RuntimeError(f"FlareSolverr failed: {data.get('message')}")
    html = data.get("solution", {}).get("response", "")
    if _looks_blocked(html):
        raise RuntimeError("FlareSolverr returned blocked page")
    log.info("FlareSolverr solved: %s", url)
    return html


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    reraise=True,
)
def _fetch(url: str) -> str:
    # Try fast path first
    try:
        return _fetch_via_curl_cffi(url)
    except Exception as e:
        log.debug("curl_cffi failed for %s: %s — escalating to FlareSolverr", url, e)

    # Escalate to browser-based solver
    return _fetch_via_flaresolverr(url)


def scrape_url(url: str) -> dict:
    try:
        html = _fetch(url)
        row = parse_property(html)
        row["status"] = "OK"
    except Exception as e:
        row = {k: "" for k in OUT_COLS}
        row["status"] = f"ERROR: {type(e).__name__}: {e}"
        log.warning("FAIL %s → %s", url, row["status"])

    row["property_id"] = extract_property_id(url)
    row["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    time.sleep(POLITE_DELAY)
    return row


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------
def run_batch(batch_index: int) -> int:
    ws = get_sheet()
    all_urls = read_urls(ws)
    total = len(all_urls)
    total_batches = max(1, (total + BATCH_SIZE - 1) // BATCH_SIZE)
    start = batch_index * BATCH_SIZE
    end = min(start + BATCH_SIZE, total)

    log.info("Batch %d/%d | URLs %d..%d of %d | concurrency=%d",
             batch_index + 1, total_batches, start + 1, end, total, CONCURRENCY)

    if start >= total:
        log.warning("Batch %d out of range (total=%d)", batch_index, total)
        return 0

    work = all_urls[start:end]
    updates, ok, fail = [], 0, 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY, thread_name_prefix="w") as pool:
        futures = {pool.submit(scrape_url, url): (sheet_row, url) for sheet_row, url in work}
        for n, fut in enumerate(as_completed(futures), start=1):
            sheet_row, url = futures[fut]
            try:
                data = fut.result()
            except Exception as e:
                log.exception("Unhandled: %s", url)
                data = {k: "" for k in OUT_COLS}
                data["status"] = f"ERROR: {type(e).__name__}: {e}"
                data["property_id"] = extract_property_id(url)
                data["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

            if data.get("status") == "OK":
                ok += 1
            else:
                fail += 1

            record = [str(data.get(c, "") or "") for c in OUT_COLS]
            updates.append({"range": OUT_RANGE_ROW.format(row=sheet_row), "values": [record]})

            if n % 5 == 0 or n == len(futures):
                log.info("Batch %d progress: %d/%d (ok=%d fail=%d)",
                         batch_index + 1, n, len(futures), ok, fail)

    log.info("Writing %d rows to %s:%s", len(updates), OUT_START_LETTER, OUT_END_LETTER)
    ws.batch_update(updates, value_input_option="RAW")
    log.info("Batch %d DONE. ok=%d fail=%d", batch_index + 1, ok, fail)
    return len(updates)


# ---------------------------------------------------------------------------
# Auto-chain
# ---------------------------------------------------------------------------
def trigger_next_batch(next_index: int) -> None:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    workflow = os.environ.get("GITHUB_WORKFLOW_FILE", "scrape.yml")
    ref = os.environ.get("GITHUB_REF_NAME", "main")
    if not token or not repo:
        log.error("Cannot auto-chain: GITHUB_TOKEN / GITHUB_REPOSITORY missing.")
        return

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    payload = {"ref": ref, "inputs": {"batch_index": str(next_index), "auto_chain": "true"}}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 204):
        log.info("Next batch triggered (index=%d)", next_index)
    else:
        log.error("Trigger failed: %s %s", r.status_code, r.text[:200])


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main():
    log.info("=== Scraper start | batch=%d size=%d concurrency=%d flaresolverr=%s ===",
             BATCH_INDEX, BATCH_SIZE, CONCURRENCY, USE_FLARESOLVERR)
    log.info("Input:  %s!%s%d", WORKSHEET_NAME, URL_COLUMN, URL_START_ROW)
    log.info("Output: %s!%s:%s", WORKSHEET_NAME, OUT_START_LETTER, OUT_END_LETTER)

    ws = get_sheet()
    total = len(read_urls(ws))
    if total == 0:
        log.warning("No URLs found. Add URLs in column A starting row 2.")
        return
    total_batches = max(1, (total + BATCH_SIZE - 1) // BATCH_SIZE)

    written = run_batch(BATCH_INDEX)
    next_index = BATCH_INDEX + 1
    if AUTO_CHAIN and next_index < total_batches:
        log.info("Chaining next batch (%d/%d)", next_index + 1, total_batches)
        trigger_next_batch(next_index)
    else:
        log.info("=== Scraper end | rows written=%d ===", written)


if __name__ == "__main__":
    main()
