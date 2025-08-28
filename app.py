import io, re, csv, urllib.request
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import pdfplumber
import pandas as pd

app = FastAPI(title="CNR PDF → CSV")

# --- Helpers ---------------------------------------------------------------

UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d[A-Z]{2})\b", re.I)
CONS_RE = re.compile(r"^[A-Za-z]?\d[\d-]{5,}$")
ACCOUNT_RE = re.compile(r"^C\d{3,}$", re.I)
MONEY_RE = re.compile(r"£?\s*(\d+(?:\.\d{2})?)")
SITE_CODE_RE = re.compile(r"^[A-Z]{2,4}$")

FINAL_COLUMNS = [
    "Type","Status","Consignment Number","Postcode","Service","Date","Size",
    "Items","Pay","Enhancement","Account","Collected From","Collection Postcode",
    "Location","Driver Name","Driver ID",
]

def clean_ws(s):
    if s is None: return ""
    return re.sub(r"\s+"," ",str(s)).strip()

def is_date_ddmmyyyy(s:str)->bool:
    try:
        datetime.strptime(s, "%d/%m/%Y")
        return True
    except Exception:
        return False

def format_amount(val:str)->str:
    try:
        f = float(str(val).replace("¬£","£").replace("£","").replace(",","").strip())
        return f"{f:.2f}"
    except Exception:
        return ""

def parse_pay_from_text(text:str):
    if not text: return "", text
    m = MONEY_RE.search(text.replace("¬£","£"))
    if not m: return "", text
    amount = format_amount(m.group(1))
    start, end = m.span()
    remain = (text[:start] + text[end:]).strip()
    remain = re.sub(r"\s{2,}"," ",remain)
    return amount, remain

def find_postcode(cells:List[str])->str:
    for c in cells:
        m = UK_POSTCODE_RE.search(c or "")
        if m: return clean_ws(m.group(0).upper())
    return ""

def is_consignment(s:str)->bool:
    return bool(CONS_RE.match(str(s).strip())) if s else False

def detect_section(page)->str:
    """
    Try to detect whether a page is a Delivery or Collection section by heading text.
    """
    txt = page.extract_text() or ""
    if "Collected By:" in txt: return "Collection"
    if "Delivered By:" in txt: return "Delivery"
    # fallback: unknown -> will infer per-table later
    return "Unknown"

# --- Core parsing ----------------------------------------------------------

def parse_pdf_to_rows(pdf_bytes: bytes,
                      default_location: str = "Leeds",
                      default_driver_name: str = "",
                      default_driver_id: str = "") -> pd.DataFrame:
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pno, page in enumerate(pdf.pages):
            section_hint = detect_section(page)
            tables = page.extract_tables() or []
            for t in tables:
                # skip tiny / header-only tables
                if not t or len(t) < 2: 
                    continue
                for raw in t:
                    row = [clean_ws(x) for x in raw]
                    if not row or all(c == "" for c in row):
                        continue

                    joined = " ".join(row)
                    first = row[0] if row else ""
                    # Heuristic: detect header rows & skip
                    if ("Status" in first or "Consign" in joined or "Account" in first and "Collected" in joined):
                        continue

                    # Try delivery-like (10 cols: Status,Consignment,Postcode,Service,Date,Size,Paid,Items,EffortPay,Enh)
                    if len(row) >= 5 and any(is_date_ddmmyyyy(x) for x in row):
                        # Work with a bounded copy to avoid index errors
                        r = (row + [""]*10)[:10]
                        Status, ConsNum, Postcode, Service, DateStr, Size, Paid, Items, EffortPay, Enhancement = r

                        if is_date_ddmmyyyy(DateStr) and section_hint in ("Delivery","Unknown"):
                            # DELIVERY row
                            # Realign mis-shifts we saw earlier:
                            # Size Paid was holding Items -> fix downstream; here compute clean "Size Paid" parts
                            # Final model wants: merge Size+Paid into Size text, then Items, Pay, Enhancement
                            pay_from_paid = format_amount(Paid)
                            # Some PDFs bundle "Small £0.09" in Size: split if needed
                            pay_from_size, size_remain = parse_pay_from_text(Size)
                            size_text = size_remain or Size
                            pay_value = pay_from_size or pay_from_paid

                            # Items sometimes includes duplicated money — remove if equals Pay
                            items_val = Items
                            if items_val and format_amount(items_val) == pay_value:
                                items_val = ""

                            rows.append({
                                "Type": "Delivery",
                                "Status": Status,
                                "Consignment Number": ConsNum,
                                "Postcode": Postcode,
                                "Service": Service,
                                "Date": DateStr,
                                "Size": size_text,          # (merged later already OK)
                                "Items": items_val,
                                "Pay": pay_value,
                                "Enhancement": format_amount(Enhancement),
                                "Account": "",
                                "Collected From": "",
                                "Collection Postcode": "",
                                "Location": default_location,
                                "Driver Name": default_driver_name,
                                "Driver ID": default_driver_id,
                            })
                            continue

                    # COLLECTION parsing (variable shapes). Find date first.
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
                        # Postcode + Consignment + Account + Site code (Collected From)
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

                        # Some files embed site code and money in size cell, e.g. "TPC Small £0.09"
                        # Extract site code & money from size; keep size text only
                        site_code = ""
                        tmp = size_cell
                        # site code at ends
                        tokens = tmp.split()
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
                            "Postcode": "",                # collections usually leave this empty
                            "Service": "",
                            "Date": date_val,
                            "Size": size_text,
                            "Items": "",                   # not used for collections in your format
                            "Pay": pay_val,
                            "Enhancement": "",
                            "Account": account_val if ACCOUNT_RE.match(account_val) else "",
                            "Collected From": collected_from_val,
                            "Collection Postcode": postcode_val,
                            "Location": default_location,
                            "Driver Name": default_driver_name,
                            "Driver ID": default_driver_id,
                        })
                        continue
                    # If we reach here, row didn’t fit either pattern — skip silently.

    df = pd.DataFrame(rows)
    # Ensure all columns exist & order
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[FINAL_COLUMNS]

    # Final normalizations
    # Pay/Enhancement -> 2dp numeric
    df["Pay"] = df["Pay"].apply(format_amount)
    df["Enhancement"] = df["Enhancement"].apply(format_amount)

    # Only keep rows with Consignment Number for the user’s preference
    df = df[df["Consignment Number"].astype(str).str.strip()!=""].reset_index(drop=True)

    return df

def df_to_csv_stream(df: pd.DataFrame, filename="output.csv"):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# --- API -------------------------------------------------------------------

class URLPayload(BaseModel):
    file_url: str
    location: Optional[str] = "Leeds"
    driver_name: Optional[str] = ""
    driver_id: Optional[str] = ""

@app.post("/process/url")
def process_by_url(payload: URLPayload):
    try:
        with urllib.request.urlopen(payload.file_url) as r:
            pdf_bytes = r.read()
        df = parse_pdf_to_rows(
            pdf_bytes,
            default_location=payload.location or "Leeds",
            default_driver_name=payload.driver_name or "",
            default_driver_id=payload.driver_id or "",
        )
        return df_to_csv_stream(df, filename="cleaned.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_by_file(
    file: UploadFile = File(...),
    location: str = Form("Leeds"),
    driver_name: str = Form(""),
    driver_id: str = Form("")
):
    try:
        pdf_bytes = await file.read()
        df = parse_pdf_to_rows(
            pdf_bytes,
            default_location=location or "Leeds",
            default_driver_name=driver_name or "",
            default_driver_id=driver_id or "",
        )
        return df_to_csv_stream(df, filename=f"{file.filename or 'cleaned'}.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
