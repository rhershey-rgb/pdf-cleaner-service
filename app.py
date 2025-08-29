import io, re, csv, threading, os
from typing import List, Tuple, Iterable, Dict

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

# =========================
# App / security / limits
# =========================
app = FastAPI(title="CNR PDF → CSV (Streaming, Locked)", version="1.9.0")

JOB_TOKEN = os.getenv("JOB_TOKEN", "")                 # if blank, auth is disabled
MAX_BYTES = int(os.getenv("MAX_BYTES", "2000000"))     # 2 MB default

def require_token(headers) -> bool:
    """Allow if JOB_TOKEN not set; otherwise require X-Job-Token header match."""
    if not JOB_TOKEN:
        return True
    return headers.get("x-job-token") == JOB_TOKEN

# Exactly one parse at a time
parse_lock = threading.Lock()

# =========================
# Regex / constants
# =========================
DRIVER_HDR_RE = re.compile(r"(Delivered|Collected)\s+By:\s*(.+?)\s*\((\d+)\)", re.I)
LOCATION_RE   = re.compile(r"Location:\s*(.+)", re.I)
DATE_RE       = re.compile(r"\d{2}/\d{2}/\d{4}")
ACCOUNT_RE    = re.compile(r"^[A-Z]\d{5,}$", re.I)        # e.g., L798133 / I782374 / C123460
SITE_CODE_RE  = re.compile(r"^[A-Z0-9]{2,4}$")
POSTCODE_RE   = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.I)
CONS_RE       = re.compile(r"^[A-Za-z]?\d[\d-]{5,}$")
STOP_RATE_RE  = re.compile(r"\bStop\s+Rate\b", re.I)

FINAL_COLUMNS = [
    "Type","Status","Consignment Number","Postcode","Service","Date","Size",
    "Items","Pay","Enhancement","Account","Collected From",
    "Collection Postcode","Location","Driver Name","Driver ID"
]

# =========================
# Helpers
# =========================
def clean_ws(s: str) -> str:
    return re.sub(r"\s+"," ",str(s)).strip() if s is not None else ""

def is_date_ddmmyyyy(s: str) -> bool:
    return bool(DATE_RE.fullmatch((s or "").strip()))

def format_amount(s: str) -> str:
    if not s: return ""
    s = str(s).replace("¬£","£").replace("£","").replace(",","").strip()
    try: return f"{float(s):.2f}"
    except: return ""

def parse_pay_from_text(txt: str) -> Tuple[str, str]:
    """Return (amount_str_2dp, remaining_text_without_amount)."""
    if not txt: return "", txt
    t = txt.replace("¬£","£")
    m = re.search(r"(?:£|¬£)?\s*(\d+(?:\.\d{1,2})?)", t)
    if not m: return "", txt
    amount = format_amount(m.group(1))
    a, b = m.span()
    remain = (t[:a] + t[b:]).strip()
    return amount, re.sub(r"\s{2,}"," ",remain)

def find_postcode(seq: List[str]) -> str:
    for c in seq:
        m = POSTCODE_RE.search(c or "")
        if m: return clean_ws(m.group(0).upper())
    return ""

def is_consignment(s: str) -> bool:
    return bool(CONS_RE.match((s or "").strip()))

def extract_location_from_first_page(pdf) -> str:
    try:
        txt = pdf.pages[0].extract_text() or ""
        for line in txt.splitlines():
            m = LOCATION_RE.search(line)
            if m:
                loc = m.group(1).strip()
                return re.split(r"\s{2,}", loc)[0].strip()
    except: pass
    return "Unknown"

def page_context(page) -> Tuple[str,str,str]:
    """Return (section, driver_name, driver_id). section ∈ {'Delivery','Collection','Unknown'}."""
    txt = page.extract_text() or ""
    sec = "Collection" if "Collected By:" in txt else "Delivery" if "Delivered By:" in txt else "Unknown"
    name = did = ""
    m = DRIVER_HDR_RE.search(txt)
    if m: name, did = clean_ws(m.group(2)), clean_ws(m.group(3))
    return sec, name, did

# =========================
# Core parser → yields dict rows
# With Collection rules:
#   - Force Type='Collection' for rows in 'Collected By:' OR rows that look like a collection
#   - Collection Items = 1
#   - Pay numeric (supports 'Stop Rate £x.xx')
#   - Back-fill missing Date from NEXT row with SAME postcode; fallback to most-common date
# =========================
def iter_parsed_rows(pdf_bytes: bytes) -> Iterable[Dict[str,str]]:
    from collections import Counter

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        location = extract_location_from_first_page(pdf)
        last_sec = last_name = last_id = ""

        # pending rows that lack date, keyed by Collection Postcode
        pending_by_pc: Dict[str, List[dict]] = {}
        seen_dates: List[str] = []

        def push_pending_for_pc(pc: str, date_val: str):
            """Fill date for any pending rows for this postcode and yield them."""
            rows = pending_by_pc.pop(pc, [])
            for sr in rows:
                sr["Date"] = date_val
                yield sr

        def add_pending(row_dict: dict, pc: str):
            if not pc: return
            pending_by_pc.setdefault(pc, []).append(row_dict)

        def most_common_date() -> str:
            vals = [d for d in seen_dates if d]
            return Counter(vals).most_common(1)[0][0] if vals else ""

        def looks_like_collection_row(cells: List[str]) -> bool:
            """
            Heuristic for collection rows even without clear header:
            - first cell looks like an Account (C123460 / L798133 / etc),
            - second cell (Collected From) has text,
            - and some money is present in the row.
            """
            acc = (cells[0] if len(cells) > 0 else "") or ""
            colfrom = (cells[1] if len(cells) > 1 else "") or ""
            has_money = any(re.search(r"(?:£|¬£)?\s*\d+(?:\.\d{1,2})?", c or "") for c in cells)
            return bool(ACCOUNT_RE.match(acc) and colfrom.strip() and has_money)

        for page in pdf.pages:
            sec, name, did = page_context(page)
            sec  = sec  or last_sec  or "Unknown"
            name = name or last_name
            did  = did  or last_id
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

                    # Skip obvious header lines
                    if ("Status" in first or "Consign" in joined or
                        ("Account" in first and "Collected" in joined)):
                        continue

                    # -------- STOP RATE line (no date in the row) --------
                    if sec in ("Collection","Unknown") and any(STOP_RATE_RE.search(c or "") for c in row):
                        account_val = row[0] if len(row)>0 else ""
                        collected_from_val = row[1] if len(row)>1 else ""
                        postcode_val = find_postcode(row)

                        pay_val = ""
                        for c in reversed(row):
                            p,_ = parse_pay_from_text(c)
                            if p: pay_val = p; break

                        pending_row = {
                            "Type":"Collection",
                            "Status":"",
                            "Consignment Number":"Stop Rate",
                            "Postcode":"",
                            "Service":"",
                            "Date":"",                      # to be filled later
                            "Size":"",
                            "Items":"1",                   # Collections always 1
                            "Pay":format_amount(pay_val),
                            "Enhancement":"",
                            "Account":account_val,
                            "Collected From":collected_from_val,
                            "Collection Postcode":postcode_val,
                            "Location":location,
                            "Driver Name":name,
                            "Driver ID":did
                        }
                        add_pending(pending_row, postcode_val)
                        continue

                    # -------- DELIVERY rows (dated) --------
                    if any(is_date_ddmmyyyy(x) for x in row) and sec in ("Delivery","Unknown"):
                        r = (row + [""]*9)[:9]
                        Status, Cons, Postcode, Service, DateStr, Size, Items, Paid, Enh = r
                        if is_date_ddmmyyyy(DateStr):
                            seen_dates.append(DateStr)

                            # Size may contain stray currency – strip it
                            _a, size_remain = parse_pay_from_text(Size)
                            size_text = size_remain or Size

                            pay_value = format_amount(Paid)

                            items_val = (Items or "").strip()
                            if not items_val.isdigit():
                                leftover = re.sub(r"(?:¬£|£)?\s*\d+(?:\.\d{1,2})?","",(Paid or "")).strip()
                                m_int = re.search(r"\b(\d+)\b", leftover)
                                if m_int: items_val = m_int.group(1)
                            if not items_val.isdigit():
                                items_val = "1"

                            enh_value = format_amount(Enh)

                            if Cons:
                                yield {
                                    "Type":"Delivery","Status":Status,"Consignment Number":Cons,
                                    "Postcode":Postcode,"Service":Service,"Date":DateStr,"Size":size_text,
                                    "Items":items_val,"Pay":pay_value,"Enhancement":enh_value,"Account":"",
                                    "Collected From":"","Collection Postcode":"","Location":location,
                                    "Driver Name":name,"Driver ID":did
                                }
                            continue

                    # -------- COLLECTION rows WITH a date in the row --------
                    date_idx = next((i for i,v in enumerate(row) if is_date_ddmmyyyy(v)), None)
                    if sec in ("Collection","Unknown") and date_idx is not None:
                        pre = row[:date_idx]
                        date_val = row[date_idx]
                        seen_dates.append(date_val)

                        size_cell = row[date_idx+1] if date_idx+1 < len(row) else ""

                        postcode_val = find_postcode(pre)

                        # Resolve any earlier pending rows for this postcode
                        if postcode_val in pending_by_pc:
                            for sr in push_pending_for_pc(postcode_val, date_val):
                                yield sr

                        # Parse pay (rightmost money wins)
                        pay_val = ""
                        for c in reversed(row):
                            p,_ = parse_pay_from_text(c)
                            if p: pay_val = p; break

                        # Try to split a site code / money from size cell
                        tokens = size_cell.split()
                        site_code = ""
                        if tokens:
                            if SITE_CODE_RE.match(tokens[0]):
                                site_code = tokens[0]; tokens = tokens[1:]
                            elif SITE_CODE_RE.match(tokens[-1]):
                                site_code = tokens[-1]; tokens = tokens[:-1]
                        tmp2 = " ".join(tokens)
                        p2, remain = parse_pay_from_text(tmp2)
                        if p2: pay_val = p2
                        size_text = remain or tmp2 or size_cell

                        # Determine consignment (or Stop Rate)
                        cons_val = ""
                        for c in reversed(pre):
                            if STOP_RATE_RE.search(c):
                                cons_val = "Stop Rate"; break
                            if is_consignment(c):
                                cons_val = c; break

                        account_val = pre[0] if len(pre)>=1 else ""
                        collected_from_val = pre[1] if len(pre)>=2 else ""
                        if site_code:
                            collected_from_val = site_code

                        if cons_val:
                            yield {
                                "Type":"Collection","Status":"","Consignment Number":cons_val,
                                "Postcode":"","Service":"","Date":date_val,"Size":size_text,
                                "Items":"1","Pay":format_amount(pay_val),"Enhancement":"",
                                "Account":account_val,"Collected From":collected_from_val,
                                "Collection Postcode":postcode_val,"Location":location,
                                "Driver Name":name,"Driver ID":did
                            }
                        continue

                    # -------- UNDATED row that still looks like a Collection --------
                    if sec in ("Collection","Unknown") and looks_like_collection_row(row):
                        account_val = row[0] if len(row)>0 else ""
                        collected_from_val = row[1] if len(row)>1 else ""
                        postcode_val = find_postcode(row)

                        pay_val = ""
                        for c in reversed(row):
                            p,_ = parse_pay_from_text(c)
                            if p: pay_val = p; break

                        pending_row = {
                            "Type":"Collection","Status":"","Consignment Number":"",
                            "Postcode":"","Service":"","Date":"",          # to be filled later
                            "Size":"",
                            "Items":"1","Pay":format_amount(pay_val) or "0.00","Enhancement":"",
                            "Account":account_val,"Collected From":collected_from_val,
                            "Collection Postcode":postcode_val,"Location":location,
                            "Driver Name":name,"Driver ID":did
                        }
                        add_pending(pending_row, postcode_val)
                        continue

        # End of document: backfill any still-pending rows with most-common date
        fallback = most_common_date()
        if fallback:
            for pc, lst in list(pending_by_pc.items()):
                for sr in lst:
                    sr["Date"] = fallback
                    yield sr
                pending_by_pc.pop(pc, None)
        else:
            # If zero dates found at all, yield as-is (rare)
            for pc, lst in list(pending_by_pc.items()):
                for sr in lst:
                    yield sr
                pending_by_pc.pop(pc, None)

# =========================
# CSV streaming (lock held)
# =========================
def stream_csv(pdf_bytes: bytes, filename="cleaned.csv") -> StreamingResponse:
    def gen():
        with parse_lock:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=FINAL_COLUMNS, extrasaction="ignore")
            w.writeheader(); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for row in iter_parsed_rows(pdf_bytes):
                if not (row.get("Consignment Number") or "").strip():
                    continue
                # ensure Pay/Enhancement rendered as 2dp strings
                if "Pay" in row: row["Pay"] = format_amount(row.get("Pay"))
                if "Enhancement" in row: row["Enhancement"] = format_amount(row.get("Enhancement"))
                # Collections must always have Items=1
                if row.get("Type") == "Collection":
                    row["Items"] = "1"
                w.writerow({k: row.get(k, "") for k in FINAL_COLUMNS})
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)
    return StreamingResponse(gen(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})

# =========================
# API
# =========================
class UrlIn(BaseModel):
    file_url: str

@app.get("/")
def root():
    return {"status":"ok","endpoints":["/process/url","/process/file","/healthz"]}

@app.get("/healthz")
def healthz():
    return {"status":"healthy"}

@app.post("/process/url")
def process_url(body: UrlIn, request: Request):
    if not require_token(request.headers):
        return JSONResponse(status_code=401, content={"error": "unauthorized"})
    try:
        with requests.get(body.file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            cl = r.headers.get("content-length")
            if cl and int(cl) > MAX_BYTES:
                return JSONResponse(status_code=413, content={"error": "file too large"})
            pdf_bytes = r.content  # your files ~60–180 KB; safe to buffer
        return stream_csv(pdf_bytes, "cleaned.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...), request: Request = None):
    if request and not require_token(request.headers):
        return JSONResponse(status_code=401, content={"error": "unauthorized"})
    try:
        pdf_bytes = await file.read()
        if len(pdf_bytes) > MAX_BYTES:
            return JSONResponse(status_code=413, content={"error": "file too large"})
        name = (file.filename or "cleaned").replace(".pdf","") + ".csv"
        return stream_csv(pdf_bytes, name)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})



