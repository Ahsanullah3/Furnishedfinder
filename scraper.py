import os
import sys
import json
import re
import time
import logging
from datetime import datetime, timezone

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Sheet1")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
TABS = int(os.environ.get("TABS", "3"))              # parallel tabs in one Chrome
BATCH_INDEX = int(os.environ.get("BATCH_INDEX", "0"))
AUTO_CHAIN = os.environ.get("AUTO_CHAIN", "true").lower() == "true"
PAGE_LOAD_TIMEOUT = int(os.environ.get("PAGE_LOAD_TIMEOUT", "45"))
CHALLENGE_WAIT = int(os.environ.get("CHALLENGE_WAIT", "20"))
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", "1.0"))
HEADLESS = os.environ.get("HEADLESS", "true").lower() == "true"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

URL_COLUMN = "A"
URL_START_ROW = 2

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
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ff-scraper")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("gspread").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)


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
# Parsing (same as before)
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
# One Chrome, N tabs
# ---------------------------------------------------------------------------
def make_driver():
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--lang=en-US,en")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    driver = uc.Chrome(options=options, version_main=None)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def is_challenge_page(html: str) -> bool:
    lower = html.lower()
    return (
        "just a moment" in lower
        or "cf-challenge" in lower
        or "attention required" in lower
        or ("cloudflare" in lower and "__next_data__" not in lower)
    )


def load_url_in_tab(driver, handle: str, url: str) -> str:
    """Switch to tab `handle`, navigate, wait out challenge, return final HTML."""
    driver.switch_to.window(handle)
    driver.get(url)

    deadline = time.time() + CHALLENGE_WAIT
    while time.time() < deadline:
        html = driver.page_source
        if not is_challenge_page(html):
            break
        time.sleep(2)

    html = driver.page_source
    if is_challenge_page(html):
        raise RuntimeError("Cloudflare challenge did not resolve")
    return html


def scrape_tabs_round(driver, work_batch: list) -> list:
    """
    Open len(work_batch) tabs (up to TABS), load each URL, collect HTML.
    Returns list of (sheet_row, url, html_or_exception).
    """
    # Ensure we have N tab handles
    handles = driver.window_handles
    while len(handles) < len(work_batch):
        driver.switch_to.window(handles[-1])
        driver.execute_script("window.open('about:blank','_blank');")
        handles = driver.window_handles

    results = []
    # Phase 1: fire all navigations (non-blocking-ish — driver.get blocks per tab,
    # but Cloudflare's JS runs while we move to the next tab)
    for i, (sheet_row, url) in enumerate(work_batch):
        handle = handles[i]
        driver.switch_to.window(handle)
        try:
            driver.get(url)
        except Exception as e:
            results.append((sheet_row, url, e))
        else:
            results.append((sheet_row, url, None))

    # Phase 2: wait out challenges and grab HTML from each tab
    final = []
    for (sheet_row, url, nav_err) in results:
        if nav_err is not None:
            final.append((sheet_row, url, nav_err))
            continue
        try:
            html = load_url_in_tab(driver, handles[results.index((sheet_row, url, nav_err))], url)
            final.append((sheet_row, url, html))
        except Exception as e:
            final.append((sheet_row, url, e))

    return final


# ---------------------------------------------------------------------------
# Batch runner (single Chrome, TABS parallel)
# ---------------------------------------------------------------------------
def run_batch(batch_index: int) -> int:
    ws = get_sheet()
    all_urls = read_urls(ws)
    total = len(all_urls)
    total_batches = max(1, (total + BATCH_SIZE - 1) // BATCH_SIZE)
    start = batch_index * BATCH_SIZE
    end = min(start + BATCH_SIZE, total)

    log.info("Batch %d/%d | URLs %d..%d of %d | tabs=%d | headless=%s",
             batch_index + 1, total_batches, start + 1, end, total, TABS, HEADLESS)

    if start >= total:
        log.warning("Batch %d out of range (total=%d)", batch_index, total)
        return 0

    work = all_urls[start:end]
    updates, ok, fail = [], 0, 0

    log.info("Launching Chrome…")
    driver = make_driver()
    log.info("Chrome ready.")

    try:
        # Process in chunks of TABS
        for chunk_start in range(0, len(work), TABS):
            chunk = work[chunk_start:chunk_start + TABS]
            log.info("Processing chunk %d/%d (%d URLs)",
                     chunk_start // TABS + 1,
                     (len(work) + TABS - 1) // TABS,
                     len(chunk))

            try:
                results = scrape_tabs_round(driver, chunk)
            except Exception as e:
                log.exception("Chunk failed: %s", e)
                # Mark the whole chunk as failed, restart Chrome for next chunk
                for sheet_row, url in chunk:
                    data = {k: "" for k in OUT_COLS}
                    data["status"] = f"ERROR: chunk failure: {e}"
                    data["property_id"] = extract_property_id(url)
                    data["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    updates.append({
                        "range": OUT_RANGE_ROW.format(row=sheet_row),
                        "values": [[str(data.get(c, "") or "") for c in OUT_COLS]],
                    })
                    fail += 1
                try:
                    driver.quit()
                except Exception:
                    pass
                log.info("Restarting Chrome after chunk failure…")
                driver = make_driver()
                continue

            for sheet_row, url, html_or_err in results:
                if isinstance(html_or_err, Exception):
                    data = {k: "" for k in OUT_COLS}
                    data["status"] = f"ERROR: {type(html_or_err).__name__}: {html_or_err}"
                    log.warning("FAIL %s → %s", url, data["status"])
                    fail += 1
                else:
                    try:
                        data = parse_property(html_or_err)
                        data["status"] = "OK"
                        ok += 1
                    except Exception as e:
                        data = {k: "" for k in OUT_COLS}
                        data["status"] = f"ERROR: parse: {e}"
                        fail += 1

                data["property_id"] = extract_property_id(url)
                data["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                updates.append({
                    "range": OUT_RANGE_ROW.format(row=sheet_row),
                    "values": [[str(data.get(c, "") or "") for c in OUT_COLS]],
                })

            done = min(chunk_start + TABS, len(work))
            log.info("Batch %d progress: %d/%d (ok=%d fail=%d)",
                     batch_index + 1, done, len(work), ok, fail)
            time.sleep(POLITE_DELAY)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

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
    log.info("=== Scraper start | batch=%d size=%d tabs=%d headless=%s ===",
             BATCH_INDEX, BATCH_SIZE, TABS, HEADLESS)
    log.info("Input:  %s!%s%d", WORKSHEET_NAME, URL_COLUMN, URL_START_ROW)
    log.info("Output: %s!%s:%s", WORKSHEET_NAME, OUT_START_LETTER, OUT_END_LETTER)

    ws = get_sheet()
    total = len(read_urls(ws))
    if total == 0:
        log.warning("No URLs found.")
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
