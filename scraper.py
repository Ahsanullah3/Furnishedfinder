import os
import json
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Properties")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def get_sheet():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
    return ws


def extract_next_data(html: str) -> dict:
    """FurnishedFinder is a Next.js app; __NEXT_DATA__ holds structured JSON."""
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        return {}


def deep_find(obj, *keys):
    """Search nested dict/list for the first occurrence of any of the given keys."""
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

    # ---- Property info ----
    title = None
    if data:
        title = deep_find(
            data, "listingName", "propertyName", "name", "title", "headline"
        )
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None

    price = None
    if data:
        price = deep_find(
            data, "price", "monthlyPrice", "rentAmount", "pricePerMonth"
        )
    if not price:
        m = re.search(r"\$[\d,]+", soup.get_text(" "))
        price = m.group(0) if m else None

    bedrooms = deep_find(data, "bedrooms", "beds", "bedroomCount") if data else None
    bathrooms = deep_find(data, "bathrooms", "baths", "bathroomCount") if data else None

    # ---- Landlord block (matches your HTML sample) ----
    landlord_name = None
    name_el = soup.find("h3", class_=re.compile("truncate"))
    if name_el:
        landlord_name = name_el.get_text(strip=True)

    location = None
    # The pin icon span sits next to the city text inside the verification card.
    for span in soup.find_all("span"):
        txt = span.get_text(" ", strip=True)
        if re.fullmatch(r"[A-Za-z .'-]+,\s*[A-Z]{2}", txt or ""):
            location = txt
            break

    # Verification badges
    text_all = soup.get_text(" ", strip=True)
    email_verified = "Email verified" in text_all
    phone_verified = "Phone number verified" in text_all
    id_verified = "ID verified" in text_all

    tenure = None
    m = re.search(r"Tenure\s*([^\n]{1,60}?)(?:\s{2,}|$)", soup.get_text("\n"))
    if m:
        tenure = m.group(1).strip()

    about = None
    about_p = soup.find("p", class_=re.compile("whitespace-pre-wrap"))
    if about_p:
        about = about_p.get_text("\n", strip=True)

    return {
        "title": title,
        "price": str(price) if price is not None else None,
        "bedrooms": str(bedrooms) if bedrooms is not None else None,
        "bathrooms": str(bathrooms) if bathrooms is not None else None,
        "location": location,
        "landlord_name": landlord_name,
        "landlord_tenure": tenure,
        "email_verified": "YES" if email_verified else "NO",
        "phone_verified": "YES" if phone_verified else "NO",
        "id_verified": "YES" if id_verified else "NO",
        "about": (about or "")[:1000],
    }


def extract_property_id(url: str) -> str:
    m = re.search(r"/property/(\d+)", url)
    return m.group(1) if m else ""


def scrape_url(session: requests.Session, url: str) -> dict:
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        row = parse_property(r.text)
        row["status"] = "OK"
    except Exception as e:
        row = {
            "title": "", "price": "", "bedrooms": "", "bathrooms": "",
            "location": "", "landlord_name": "", "landlord_tenure": "",
            "email_verified": "", "phone_verified": "", "id_verified": "",
            "about": "", "status": f"ERROR: {e}",
        }
    row["property_id"] = extract_property_id(url)
    row["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return row


def main():
    ws = get_sheet()
    values = ws.get_all_values()
    if not values:
        raise SystemExit("Sheet is empty; add the header + URLs first.")

    header = values[0]
    url_col = header.index("url")  # 0
    rows = values[1:]

    # Output column order (must match header layout above)
    out_cols = [
        "property_id", "title", "price", "bedrooms", "bathrooms", "location",
        "landlord_name", "landlord_tenure", "email_verified", "phone_verified",
        "id_verified", "about", "status", "scraped_at",
    ]

    session = requests.Session()
    updates = []
    for i, row in enumerate(rows, start=2):  # row 2 is first data row
        url = row[url_col].strip() if len(row) > url_col else ""
        if not url:
            continue
        print(f"[{i}] scraping {url}")
        data = scrape_url(session, url)
        record = [str(data.get(c, "") or "") for c in out_cols]
        updates.append({"range": f"B{i}:O{i}", "values": [record]})
        time.sleep(1.5)  # be polite / avoid rate-limits

    if updates:
        ws.batch_update(updates, value_input_option="RAW")
    print(f"Done. Wrote {len(updates)} rows.")


if __name__ == "__main__":
    main()
