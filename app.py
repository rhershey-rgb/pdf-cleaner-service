import io, re, csv
from typing import List, Tuple, Iterable, Dict

import pdfplumber
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import requests

app = FastAPI(title="CNR PDF → CSV (Streaming)", version="1.3.2")

# ---- Regex / constants ----
DRIVER_HDR_RE = re.compile(r"(Delivered|Collected)\s+By:\s*(.+?)\s*\((\d+)\)", re.I)
LOCATION_RE   = re.compile(r"Location:\s*(.+)", re.I)
DATE_RE       = re.compile(r"\d{2}/\d{2}/\d{4}")
ACCOUNT_RE    = re.compile(r"^[A-Z]\d{5,}$", re.I)        # L798133 / I782374 / C123460
SITE_CODE_RE  = re.compile(r"^[A-Z0-9]{2,4}$")
POSTCODE_RE   = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.I)
CONS_RE       = re.compile(r"^[A-Za-z]?\d[\d-]{5,}$")
STOP_RATE_RE  = re.compile(r"\bStop\s+Rate\b", re.I)

FINAL_COLUMNS = [
    "Type","Status","Consignment Number","Postcode","Service","Date","Size",
    "Items","Pay","Enhancement","Account","Collected From",
    "Collection Postcode","Location","Driver Name","Driver ID"
]

# ---- helpers ----
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
    txt = page.extract_text() or ""
    sec = "Collection" if "Collected By:" in txt else "Delivery" if "Delivered By:" in txt else "Unknown"
    name = did = ""
    m = DRIVER_HDR_RE.search(txt)
    if m: name, did = clean_ws(m.group(2)), clean_ws(m.group(3))
    return sec, name, did

# ---- core parser → yields dict rows ----
def iter_parsed_rows(pdf_bytes: bytes) -> Iterable[Dict[str,str]]:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        location = extract_location_from_first_page(pdf)
        last_sec = last_name = last_id = ""
        pending_stop_rows: List[dict] = []

        def flush_pending(found_date: str):
            nonlocal pending_stop_rows
            for sr in pending_stop_rows:
                sr["Date"] = found_date
                yield sr
            pending_stop_rows = []

        for page in pdf.pages:
            sec, name, did = page_context(page)
            sec  = sec  or last_sec  or "Unknown"
            name = name or last_name
            did  = did  or last_id
            last_sec, last_name, last_id = sec, name, did

            tables = page.extract_tables() or []
            for t in tables:
                if not t or len(t) < 2: continue
                for raw in t:
                    row = [clean_ws(x) for x in raw]
                    if not row or all(c == "" for c in row): continue

                    joined = " ".join(row)
                    first  = row[0] if row else ""
                    if ("Status" in first or "Consign" in joined or
                        ("Account" in first and "Collected" in joined)):
                        continue

                    # STOP RATE (date comes from next row)
                    if sec in ("Collection","Unknown") and any(STOP_RATE_RE.search(c or "") for c in row):
                        account_val = row[0] if len(row)>0 else ""
                        collected_from_val = row[1] if len(row)>1 else ""
                        postcode_val = find_postcode(row)
                        pay_val = ""
                        for c in reversed(row):
                            p,_ = parse_pay_from_text(c)
                            if p: pay_val = p; break
                        pending_stop_rows.append({
                            "Type":"Collection","Status":"","Consignment Number":"Stop Rate","Postcode":"",
                            "Service":"","Date":"","Size":"","Items":"1","Pay":format_amount(pay_val),
                            "Enhancement":"","Account":account_val if ACCOUNT_RE.match(account_val) else account_val,
                            "Collected From":collected_from_val,"Collection Postcode":postcode_val,
                            "Location":location,"Driver Name":name,"Driver ID":did
                        })
                        continue

                    # DELIVERY (9 cols): Status|Cons|Postcode|Service|Date|Size|Items|Paid|Enhancement
                    if any(is_date_ddmmyyyy(x) for x in row) and sec in ("Delivery","Unknown"):
                        r = (row + [""]*9)[:9]
                        Status, Cons, Postcode, Service, DateStr, Size, Items, Paid, Enh = r
                        if is_date_ddmmyyyy(DateStr):
                            _a, size_remain = parse_pay_from_text(Size)
                            size_text = size_remain or Size
                            pay_value = format_amount(Paid)
                            items_val = (Items or "").strip()
                            if not items_val.isdigit():
                                leftover = re.sub(r"(?:¬£|£)?\s*\d+(?:\.\d{1,2})?","",(Paid or "")).strip()
                                m_int = re.search(r"\b(\d+)\b", leftover)
                                if m_int: items_val = m_int.group(1)
                            if not items_val.isdigit(): items_val = "1"
                            enh_value = format_amount(Enh)
                            if Cons:
                                yield {"Type":"Delivery","Status":Status,"Consignment Number":Cons,
                                       "Postcode":Postcode,"Service":Service,"Date":DateStr,"Size":size_text,
                                       "Items":items_val,"Pay":pay_value,"Enhancement":enh_value,"Account":"",
                                       "Collected From":"","Collection Postcode":"","Location":location,
                                       "Driver Name":name,"Driver ID":did}
                            continue

                    # COLLECTION with date
                    date_idx = next((i for i,v in enumerate(row) if is_date_ddmmyyyy(v)), None)
                    if sec in ("Collection","Unknown") and date_idx is not None:
                        pre = row[:date_idx]
                        date_val = row[date_idx]
                        size_cell = row[date_idx+1] if date_idx+1 < len(row) else ""
                        if pending_stop_rows:
                            for sr in flush_pending(date_val): yield sr
                        pay_val = ""
                        for c in reversed(row):
                            p,_ = parse_pay_from_text(c)
                            if p: pay_val = p; break
                        postcode_val = find_postcode(pre)
                        cons_val = ""
                        for c in reversed(pre):
                            if STOP_RATE_RE.search(c): cons_val="Stop Rate"; break
                            if is_consignment(c): cons_val=c; break
                        account_val = pre[0] if len(pre)>=1 else ""
                        collected_from_val = pre[1] if len(pre)>=2 else ""
                        tokens = size_cell.split()
                        site_code = ""
                        if tokens:
                            if SITE_CODE_RE.match(tokens[0]): site_code=tokens[0]; tokens=tokens[1:]
                            elif SITE_CODE_RE.match(tokens[-1]): site_code=tokens[-1]; tokens=tokens[:-1]
                        tmp2 = " ".join(tokens)
                        p2, remain = parse_pay_from_text(tmp2)
                        if p2: pay_val = p2
                        size_text = remain or tmp2 or size_cell
                        if site_code: collected_from_val = site_code
                        if cons_val:
                            yield {"Type":"Collection","Status":"","Consignment Number":cons_val,
                                   "Postcode":"","Service":"","Date":date_val,"Size":size_text,
                                   "Items":"1","Pay":format_amount(pay_val),"Enhancement":"",
                                   "Account":account_val if ACCOUNT_RE.match(account_val) else account_val,
                                   "Collected From":collected_from_val,"Collection Postcode":postcode_val,
                                   "Location":location,"Driver Name":name,"Driver ID":did}

        # flush any orphan Stop Rate (no following dated row)
        for sr in pending_stop_rows:
            yield sr

# ---- CSV streaming ----
def stream_csv(pdf_bytes: bytes, filename="cleaned.csv") -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=FINAL_COLUMNS, extrasaction="ignore")
        w.writeheader(); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for row in iter_parsed_rows(pdf_bytes):
            if not (row.get("Consignment Number") or "").strip():  # keep only real rows
                continue
            w.writerow({k: row.get(k, "") for k in FINAL_COLUMNS})
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
    return StreamingResponse(gen(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})

# ---- API ----
class UrlIn(BaseModel):
    file_url: str

@app.get("/")
def root(): return {"status":"ok","endpoints":["/process/url","/process/file","/healthz"]}

@app.get("/healthz")
def healthz(): return {"status":"healthy"}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        return stream_csv(r.content, "cleaned.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        name = (file.filename or "cleaned").replace(".pdf","") + ".csv"
        return stream_csv(pdf_bytes, name)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

