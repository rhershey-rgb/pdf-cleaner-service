import io, re, urllib.request
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="CNR PDF → CSV", version="1.0.0")

# Optional CORS (safe defaults)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- Regex & constants --------------------

UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d[A-Z]{2})\b", re.I)
CONS_RE = re.compile(r"^[A-Za-z]?\d[\d-]{5,}$")          # consignment number
ACCOUNT_RE = re.compile(r"^C\d{3,}$", re.I)              # account code like C123460
MONEY_RE = re.compile(r"£?\s*(\d+(?:\.\d{2})?)")
SITE_CODE_RE = re.compile(r"^[A-Z]{2,4}$")

DRIVER_HDR_RE = re.compile(r"(Delivered|Collected)\s+By:\s*(.+?)\s*\((\d+)\)", re.I)
LOCATION_RE = re.compile(r"Location:\s*(.+)", re.I)

FINAL_COLUMNS = [
    "Type","Status","Consignment Number","Postcode","Service","Date","Size",
    "Items","Pay","Enhancement","Account","Collected From",
    "Collection Postcode","Location","Driver Name","Driver ID"
]

# -------------------- utils --------------------

def clean_ws(s) -> str:
    if s is None: return ""
    return re.sub(r"\s+"," ",str(s)).strip()

def is_date_ddmmyyyy(s: str) -> bool:
    try:
        datetime.strptime(s, "%d/%m/%Y")
        return True
    except Exception:
        return False

def format_amount(val: str) -> str:
    """Return 2dp numeric string; tolerate ¬£, £ and commas. Empty if not parseable."""
    try:
        f = float(str(val).replace("¬£","£").replace("£","").replace(",","").strip())
        return f"{f:.2f}"
    except Exception:
        return ""

def parse_pay_from_text(text: str) -> Tuple[str, str]:
    """Extract first money amount; return (amount_2dp, remaining_text)."""
    if not text: return "", text
    t = text.replace("¬£","£")
    m = MONEY_RE.search(t)
    if not m: return "", text
    amount = format_amount(m.group(1))
    start, end = m.span()
    remain = (t[:start] + t[end:]).strip()
    remain = re.sub(r"\s{2,}"," ",remain)
    return amount, remain

def find_postcode(cells: List[str]) -> str:
    for c in cells:
        m = UK_POSTCODE_RE.search(c or "")
        if m: return clean_ws(m.group(0).upper())
    return ""

def is_consignment(s: str) -> bool:
    return bool(CONS_RE.match(str(s).strip())) if s else False

def extract_location_from_first_page(pdf) -> str:
    """Read 'Location: ...' from first page; fallback 'Unknown'."""
    try:
        first_txt = pdf.pages[0].extract_text() or ""
        for line in first_txt.splitlines():
            m = LOCATION_RE.search(line)
            if m:
                loc = m.group(1).strip()
                # Stop at double-spaces or end-of-line noise if present
                loc = re.split(r"\s{2,}", loc)[0].strip()
                return loc
    except Exception:
        pass
    return "Unknown"

def page_context(page) -> Tuple[str, str, str]:
    """
    Return (section_type, driver_name, driver_id)
    section_type in {"Delivery","Collection","Unknown"}.
    """
    txt = page.extract_text() or ""
    sec = "Unknown"
    if "Collected By:" in txt: sec = "Collection"
    elif "Delivered By:" in txt: sec = "Delivery"

    driver_name, driver_id = "", ""
    m = DRIVER_HDR_RE.search(txt)
    if m:
        driver_name = clean_ws(m.group(2))
        driver_id = clean_ws(m.group(3))
    return sec, driver_name, driver_id

# -------------------- core parsing --------------------

def parse_pdf_to_rows(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        detected_location = extract_location_from_first_page(pdf)

        for page in pdf.pages:
            section_hint, page_driver_name, page_driver_id = page_context(page)
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

                    # skip obvious header lines
                    if ("Status" in first or "Consign" in joined or
                        ("Account" in first and "Collected" in joined)):
                        continue

                    # ---------- DELIVERY ----------
                    if len(row) >= 5 and any(is_date_ddmmyyyy(x) for x in row):
                        r = (row + [""]*10)[:10]
                        Status, ConsNum, Postcode, Service, DateStr, Size, Paid, Items, EffortPay, Enhancement = r

                        if is_date_ddmmyyyy(DateStr) and section_hint in ("Delivery","Unknown"):
                            # Parse size/pay; handle cases like "Small ¬£0.09"
                            pay_from_size, size_remain = parse_pay_from_text(Size)
                            pay_from_paid = format_amount(Paid)
                            pay_value = pay_from_size or pay_from_paid
                            size_text = size_remain or Size

                            # Dedup if Items is just a money that equals Pay
                            if Items and format_amount(Items) == pay_value:
                                Items = ""

                            rows.append({
                                "Type": "Delivery",
                                "Status": Status,
                                "Consignment Number": ConsNum,
                                "Postcode": Postcode,
                                "Service": Service,
                                "Date": DateStr,
                                "Size": size_text,
                                "Items": Items,
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

                    # ---------- COLLECTION ----------
                    # Heuristic: find a date; treat as collection if page says so
                    date_idx = None
                    for i, v in enumerate(row):
                        if is_date_ddmmyyyy(v):
                            date_idx = i
                            break
                    if (section_hint in ("Collection","Unknown")) and date_idx is not None:
                        pre = row[:date_idx]
                        date_val = row[date_idx]
                        size_cell = row[date_idx+1] if date_idx+1 < len(row) else ""

                        # Pay: last currency-looking cell in the row
                        pay_val = ""
                        for c in reversed(row):
                            p, _ = parse_pay_from_text(c)
                            if p:
                                pay_val = p
                                break

                        postcode_val = find_postcode(pre)

                        # Rightmost consignment-like token before date; ignore "Stop Rate"
                        cons_val = ""
                        for c in reversed(pre):
                            if "Stop Rate" in c:
                                cons_val = ""
                                break
                            if is_consignment(c):
                                cons_val = c
                                break

                        # Account + provisional site code from the two leading cells
                        account_val = pre[0] if len(pre) >= 1 else ""
                        collected_from_val = pre[1] if len(pre) >= 2 else ""

                        # Many rows embed site code + money + size together (e.g., "TPC Small ¬£0.09")
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
                    # otherwise skip non-data rows silently

    df = pd.DataFrame(rows)

    # ensure all expected columns exist
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # normalize numeric fields
    df["Pay"] = df["Pay"].apply(format_amount)
    df["Enhancement"] = df["Enhancement"].apply(format_amount)

    # keep only rows with consignment numbers (your preference)
    df = df[df["Consignment Number"].astype(str).str.strip() != ""].reset_index(drop=True)

    # order columns
    df = df[FINAL_COLUMNS]
    return df

def df_to_csv_stream(df: pd.DataFrame, filename="cleaned.csv") -> StreamingResponse:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# -------------------- API --------------------

class URLPayload(BaseModel):
    file_url: str  # direct PDF URL only; location/drivers auto-detected

@app.get("/")
def root():
    return {
        "ok": True,
        "routes": ["/process/url", "/process/file", "/healthz"],
        "hint": "POST a JSON body with { 'file_url': '<pdf url>' } to /process/url",
    }

@app.get("/healthz")
def health():
    return {"status": "healthy"}

@app.post("/process/url")
@app.post("/process/url/")
def process_by_url(payload: URLPayload):
    try:
        with urllib.request.urlopen(payload.file_url) as r:
            pdf_bytes = r.read()
        df = parse_pdf_to_rows(pdf_bytes)
        return df_to_csv_stream(df, filename="cleaned.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
@app.post("/process/file/")
async def process_by_file(
    file: UploadFile = File(...),
):
    try:
        pdf_bytes = await file.read()
        df = parse_pdf_to_rows(pdf_bytes)
        out_name = (file.filename or "cleaned").replace(".pdf","") + ".csv"
        return df_to_csv_stream(df, filename=out_name)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

