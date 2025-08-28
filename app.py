import io
import re
from typing import List, Tuple

import pdfplumber
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import requests

app = FastAPI(title="CNR PDF → CSV (Combined)", version="1.0.0")

# =========================
# Regex & helpers
# =========================
DRIVER_HDR_RE = re.compile(r"(Delivered|Collected)\s+By:\s*(.+?)\s*\((\d+)\)", re.I)
LOCATION_RE   = re.compile(r"Location:\s*(.+)", re.I)
DATE_RE       = re.compile(r"\d{2}/\d{2}/\d{4}")
ACCOUNT_RE    = re.compile(r"^C\d{3,}$", re.I)
SITE_CODE_RE  = re.compile(r"^[A-Z0-9]{2,4}$")
POSTCODE_RE   = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.I)
# tolerant consignment: long digit strings, optional leading letter and hyphens
CONS_RE       = re.compile(r"^[A-Za-z]?\d[\d-]{5,}$")

FINAL_COLUMNS = [
    "Type","Status","Consignment Number","Postcode","Service","Date","Size",
    "Items","Pay","Enhancement","Account","Collected From",
    "Collection Postcode","Location","Driver Name","Driver ID"
]

def clean_ws(s: str) -> str:
    return re.sub(r"\s+"," ",str(s)).strip() if s is not None else ""

def is_date_ddmmyyyy(s: str) -> bool:
    return bool(DATE_RE.fullmatch((s or "").strip()))

def format_amount(s: str) -> str:
    """Return 2dp numeric string; tolerate ¬£/£ and commas. Empty if not parseable."""
    if not s: return ""
    s = str(s).replace("¬£","£").replace("£","").replace(",","").strip()
    try:
        return f"{float(s):.2f}"
    except Exception:
        return ""

def parse_pay_from_text(txt: str) -> Tuple[str, str]:
    """Extract a money amount; return (amount_2dp, remaining_text_without_amount)."""
    if not txt: return "", txt
    t = txt.replace("¬£","£")
    m = re.search(r"(?:£|¬£)?\s*(\d+(?:\.\d{1,2})?)", t)
    if not m: return "", txt
    amount = format_amount(m.group(1))
    start, end = m.span()
    remain = (t[:start] + t[end:]).strip()
    remain = re.sub(r"\s{2,}", " ", remain)
    return amount, remain

def find_postcode(seq: List[str]) -> str:
    for c in seq:
        m = POSTCODE_RE.search(c or "")
        if m: return clean_ws(m.group(0).upper())
    return ""

def is_consignment(s: str) -> bool:
    return bool(CONS_RE.match((s or "").strip()))

def extract_location_from_first_page(pdf) -> str:
    try:
        first_txt = pdf.pages[0].extract_text() or ""
        for line in first_txt.splitlines():
            m = LOCATION_RE.search(line)
            if m:
                loc = m.group(1).strip()
                loc = re.split(r"\s{2,}", loc)[0].strip()
                return loc
    except Exception:
        pass
    return "Unknown"

def page_context(page) -> Tuple[str, str, str]:
    """Return (section_type, driver_name, driver_id)."""
    txt = page.extract_text() or ""
    sec = "Unknown"
    if "Collected By:" in txt: sec = "Collection"
    elif "Delivered By:" in txt: sec = "Delivery"
    name, did = "", ""
    m = DRIVER_HDR_RE.search(txt)
    if m:
        name = clean_ws(m.group(2))
        did  = clean_ws(m.group(3))
    return sec, name, did

# =========================
# Core parser (combined)
# =========================
def parse_pdf_to_rows(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        location = extract_location_from_first_page(pdf)

        # carry context across continued pages
        last_sec = "Unknown"
        last_name = ""
        last_id = ""

        for page in pdf.pages:
            sec, name, did = page_context(page)
            if sec == "Unknown": sec = last_sec
            if not name: name = last_name
            if not did:  did  = last_id
            last_sec, last_name, last_id = sec, name, did

            tables = page.extract_tables() or []
            for t in tables:
                if not t or len(t) < 2: 
                    continue
                for raw in t:
                    row = [clean_ws(x) for x in raw]
                    if not row or all(c == "" for c in row): 
                        continue

                    joined = " ".join(row)
                    first  = row[0] if row else ""
                    # skip headers
                    if ("Status" in first or "Consign" in joined or
                        ("Account" in first and "Collected" in joined)):
                        continue

                    # ---------- DELIVERY (9 columns) ----------
                    # Expected columns:
                    # Status | Consignment | Postcode | Service | Date | Size | Items | Paid | Enhancement
                    if any(is_date_ddmmyyyy(x) for x in row) and sec in ("Delivery","Unknown"):
                        r = (row + [""]*9)[:9]
                        Status, ConsNum, Postcode, Service, DateStr, Size, Items, Paid, Enhancement = r
                        if is_date_ddmmyyyy(DateStr):
                            # Size text cleaned (strip any embedded money if OCR merged it)
                            _p_from_size, size_remain = parse_pay_from_text(Size)
                            size_text = size_remain or Size

                            # Pay straight from Paid
                            pay_value = format_amount(Paid)

                            # Items from Items cell or recover from "Paid" like "1 £2.09"
                            items_val = (Items or "").strip()
                            if not items_val.isdigit():
                                paid_raw = (Paid or "")
                                # remove money and look for leftover integer (e.g., "1" from "1 £2.09")
                                leftover = re.sub(r"(?:¬£|£)?\s*\d+(?:\.\d{1,2})?", "", paid_raw).strip()
                                m_int = re.search(r"\b(\d+)\b", leftover)
                                if m_int:
                                    items_val = m_int.group(1)
                            # final fallback: manifests are almost always 1 per delivery row
                            if not items_val.isdigit():
                                items_val = "1"

                            enh_value = format_amount(Enhancement)

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
                                "Enhancement": enh_value,
                                "Account": "",
                                "Collected From": "",
                                "Collection Postcode": "",
                                "Location": location,
                                "Driver Name": name,
                                "Driver ID": did,
                            })
                            continue

                    # ---------- COLLECTION (variable shape) ----------
                    # Find date; use that to split pre vs details
                    date_idx = None
                    for i, v in enumerate(row):
                        if is_date_ddmmyyyy(v):
                            date_idx = i
                            break

                    if sec in ("Collection","Unknown") and date_idx is not None:
                        pre = row[:date_idx]
                        date_val = row[date_idx]
                        size_cell = row[date_idx+1] if date_idx+1 < len(row) else ""

                        # Pay: last money-like token in the row
                        pay_val = ""
                        for c in reversed(row):
                            p, _ = parse_pay_from_text(c)
                            if p:
                                pay_val = p
                                break

                        postcode_val = find_postcode(pre)

                        # Rightmost consignment before date (ignore "Stop Rate")
                        cons_val = ""
                        for c in reversed(pre):
                            if "Stop Rate" in c:
                                cons_val = ""
                                break
                            if is_consignment(c):
                                cons_val = c
                                break

                        # Account & Collected From — take first two cells, then refine from size cell
                        account_val = pre[0] if len(pre) >= 1 else ""
                        collected_from_val = pre[1] if len(pre) >= 2 else ""

                        # Extract site code & money from size_cell (e.g., "TPC Small £0.09")
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
                            "Pay": format_amount(pay_val),
                            "Enhancement": "",
                            "Account": account_val if ACCOUNT_RE.match(account_val) else "",
                            "Collected From": collected_from_val,
                            "Collection Postcode": postcode_val,
                            "Location": location,
                            "Driver Name": name,
                            "Driver ID": did,
                        })
                        continue
                    # non-data rows skipped

    df = pd.DataFrame(rows)

    # Ensure all columns exist & order
    for col in FINAL_COLUMNS:
        if col not in df.columns: df[col] = ""
    df = df[FINAL_COLUMNS]

    # Keep only rows that actually have a consignment number
    df = df[df["Consignment Number"].astype(str).str.strip() != ""].reset_index(drop=True)

    return df

# =========================
# API
# =========================
class UrlIn(BaseModel):
    file_url: str  # must be a direct-download or public URL

@app.get("/")
def root():
    return {"status":"ok","endpoints":["/process/url","/process/file","/healthz"]}

@app.get("/healthz")
def healthz():
    return {"status":"healthy"}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        df = parse_pdf_to_rows(r.content)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="cleaned.csv"'},
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        df = parse_pdf_to_rows(pdf_bytes)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        outname = (file.filename or "cleaned").replace(".pdf","") + ".csv"
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{outname}"'},
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
