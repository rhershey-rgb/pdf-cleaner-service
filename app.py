import io
import re
import pdfplumber
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import requests

app = FastAPI()

# ------------------------
# Regex helpers
# ------------------------
DRIVER_HDR_RE = re.compile(r"(Delivered|Collected)\s+By:\s*(.+?)\s*\((\d+)\)", re.I)
LOCATION_RE = re.compile(r"Location:\s*(.+)", re.I)
DATE_RE = re.compile(r"\d{2}/\d{2}/\d{4}")
ACCOUNT_RE = re.compile(r"^C\d+")
SITE_CODE_RE = re.compile(r"^[A-Z0-9]{2,4}$")
POSTCODE_RE = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.I)


# ------------------------
# Utilities
# ------------------------
def clean_ws(s: str) -> str:
    return s.strip() if s else ""


def is_date_ddmmyyyy(s: str) -> bool:
    return bool(DATE_RE.fullmatch(s.strip())) if s else False


def is_consignment(s: str) -> bool:
    return bool(re.fullmatch(r"\d{10,15}", s.strip())) if s else False


def format_amount(s: str) -> str:
    if not s:
        return ""
    s = s.replace("£", "").replace("¬£", "").strip()
    try:
        return f"{float(s):.2f}"
    except Exception:
        return ""


def extract_location_from_first_page(pdf) -> str:
    try:
        first_txt = pdf.pages[0].extract_text() or ""
        for line in first_txt.splitlines():
            m = LOCATION_RE.search(line)
            if m:
                loc = m.group(1).strip()
                loc = re.split(r"\s{2,}|(?::\s*$)", loc)[0].strip()
                return loc
    except Exception:
        pass
    return "Unknown"


def page_context(page):
    """Return (section_type, driver_name, driver_id)"""
    txt = page.extract_text() or ""
    sec = "Unknown"
    if "Collected By:" in txt:
        sec = "Collection"
    elif "Delivered By:" in txt:
        sec = "Delivery"

    driver_name, driver_id = "", ""
    m = DRIVER_HDR_RE.search(txt)
    if m:
        driver_name = m.group(2).strip()
        driver_id = m.group(3).strip()
    return sec, driver_name, driver_id


def find_postcode(seq):
    for c in seq:
        if POSTCODE_RE.search(c):
            return POSTCODE_RE.search(c).group(0)
    return ""


def parse_pay_from_text(txt: str):
    if not txt:
        return "", txt
    m = re.search(r"(?:£|¬£)?\s*(\d+(?:\.\d{1,2})?)", txt)
    if m:
        val = format_amount(m.group(1))
        remain = txt.replace(m.group(0), "").strip()
        return val, remain
    return "", txt


def normalize_items_pay(items_val: str, pay_val: str) -> tuple[str, str]:
    """Ensure Items is a count and Pay is a 2dp currency string."""
    items = (items_val or "").strip()
    pay = (pay_val or "").strip()

    # If Items looks like money, move to Pay
    if re.fullmatch(r"(?:¬£|£)?\s*\d+(?:\.\d{1,2})?\s*", items):
        if not pay:
            pay = items
        items = ""

    # If Pay is just an int and Items is blank, flip it
    if re.fullmatch(r"\d+", pay) and (items == "" or not items.isdigit()):
        items, pay = pay, ""

    items = items if items.isdigit() else ""
    pay = format_amount(pay)
    return items, pay


# ------------------------
# Core parser
# ------------------------
def parse_pdf_to_rows(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        detected_location = extract_location_from_first_page(pdf)

        # remember last seen context for continued tables
        last_section = "Unknown"
        last_driver_name = ""
        last_driver_id = ""

        for page in pdf.pages:
            section_hint, page_driver_name, page_driver_id = page_context(page)

            # Inherit context if missing on this page
            if section_hint == "Unknown":
                section_hint = last_section
            if not page_driver_name:
                page_driver_name = last_driver_name
            if not page_driver_id:
                page_driver_id = last_driver_id

            # Update state
            last_section = section_hint
            last_driver_name = page_driver_name
            last_driver_id = page_driver_id

            tables = page.extract_tables() or []
            for t in tables:
                if not t or len(t) < 2:
                    continue
                for raw in t:
                    row = [clean_ws(x) for x in raw]
                    if not row or all(c == "" for c in row):
                        continue

                    joined = " ".join(row)
                    first = row[0] if row else ""
                    if ("Status" in first or "Consign" in joined or
                        ("Account" in first and "Collected" in joined)):
                        continue

                    # -------- Delivery parsing --------
                    if len(row) >= 5 and any(is_date_ddmmyyyy(x) for x in row):
                        r = (row + [""] * 10)[:10]
                        Status, ConsNum, Postcode, Service, DateStr, Size, Paid, Items, EffortPay, Enhancement = r
                        if is_date_ddmmyyyy(DateStr) and section_hint in ("Delivery", "Unknown"):
                            pay_from_paid = format_amount(Paid)
                            pay_from_size, size_remain = parse_pay_from_text(Size)
                            size_text = size_remain or Size
                            pay_value = pay_from_size or pay_from_paid
                            items_val = Items
                            if items_val and format_amount(items_val) == pay_value:
                                items_val = ""

                            # normalize Items vs Pay
                            items_val, pay_value = normalize_items_pay(items_val, pay_value)

                            rows.append({
                                "Type": "Delivery",
                                "Status": Status,
                                "Consignment Number": ConsNum,
                                "Postcode": Postcode,
                                "Service": Service,
                                "Date": DateStr,
                                "Size": size_text,
                                "Items": items_val,
                                "Pay": pay_value,
                                "Enhancement": format_amount(Enhancement),
                                "Account": "",
                                "Collected From": "",
                                "Collection Postcode": "",
                                "Location": detected_location,
                                "Driver Name": page_driver_name,
                                "Driver ID": page_driver_id,
                            })
                            continue

                    # -------- Collection parsing --------
                    date_idx = None
                    for i, v in enumerate(row):
                        if is_date_ddmmyyyy(v):
                            date_idx = i
                            break
                    if (section_hint in ("Collection", "Unknown")) and date_idx is not None:
                        pre = row[:date_idx]
                        date_val = row[date_idx]
                        size_cell = row[date_idx + 1] if date_idx + 1 < len(row) else ""
                        pay_val = ""
                        for c in reversed(row):
                            p, _ = parse_pay_from_text(c)
                            if p:
                                pay_val = p
                                break
                        postcode_val = find_postcode(pre)
                        cons_val = ""
                        for c in reversed(pre):
                            if "Stop Rate" in c:
                                cons_val = ""
                                break
                            if is_consignment(c):
                                cons_val = c
                                break
                        account_val = pre[0] if len(pre) >= 1 else ""
                        collected_from_val = pre[1] if len(pre) >= 2 else ""

                        tokens = size_cell.split()
                        site_code = ""
                        if tokens:
                            if SITE_CODE_RE.match(tokens[0]):
                                site_code = tokens[0]; tokens = tokens[1:]
                            elif SITE_CODE_RE.match(tokens[-1]):
                                site_code = tokens[-1]; tokens = tokens[:-1]
                        tmp2 = " ".join(tokens)
                        pay_from_size, remain_size = parse_pay_from_text(tmp2)
                        if pay_from_size:
                            pay_val = pay_from_size
                        size_text = remain_size or tmp2 or size_cell
                        if site_code:
                            collected_from_val = site_code

                        rows.append({
                            "Type": "Collection",
                            "Status": "",
                            "Consignment Number": cons_val,
                            "Postcode": "",
                            "Service": "",
                            "Date": date_val,
                            "Size": size_text,
                            "Items": "",
                            "Pay": pay_val,
                            "Enhancement": "",
                            "Account": account_val if ACCOUNT_RE.match(account_val) else "",
                            "Collected From": collected_from_val,
                            "Collection Postcode": postcode_val,
                            "Location": detected_location,
                            "Driver Name": page_driver_name,
                            "Driver ID": page_driver_id,
                        })
                        continue

    return pd.DataFrame(rows)


# ------------------------
# API models
# ------------------------
class UrlIn(BaseModel):
    file_url: str


# ------------------------
# Endpoints
# ------------------------
@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    pdf_bytes = await file.read()
    df = parse_pdf_to_rows(pdf_bytes)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=cleaned.csv"}
    )


@app.post("/process/url")
async def process_url(data: UrlIn):
    r = requests.get(data.file_url)
    r.raise_for_status()
    df = parse_pdf_to_rows(r.content)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=cleaned.csv"}
    )


@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}
