# app.py
from __future__ import annotations
import io
import csv
import re
import time
import typing as t
from collections import Counter
from datetime import datetime
from threading import Lock

import requests
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

# ----------------------------
# Service setup / constants
# ----------------------------
app = FastAPI(title="PDF → CSV service", version="1.0.0")
parse_lock = Lock()

MAX_BYTES = 25 * 1024 * 1024  # 25MB safety (adjust if you need)

# Final CSV column order (edit if you want to add/remove fields)
CSV_COLUMNS = [
    "Type",
    "Status",
    "Consignment Number",
    "Postcode",
    "Service",
    "Date",
    "Size",
    "Items",
    "Pay",
    "Enhancement",
    "Account",
    "Collected From",
    "Collection Postcode",
    "Location",
    "Driver Name",
    "Driver ID",
]

MONEY_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

# ----------------------------
# Helpers – parsing & coercion
# ----------------------------

def _to_iso(d: str | None) -> str:
    """Return YYYY-MM-DD or '' if not parseable."""
    s = (d or "").strip()
    if not s:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""  # leave empty if we can't be sure

def _to_int(v: t.Any) -> int | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def _to_money(v: t.Any) -> float | None:
    """Accept '£2.11', 'Stop Rate £2.11', '2.11', etc."""
    if v is None:
        return None
    s = str(v).strip()
    if "£" in s:
        s = s.split("£", 1)[-1]
    m = MONEY_RE.search(s)
    return float(m.group(0)) if m else None

def _most_common_date(rows: list[dict]) -> str | None:
    dates = [(r.get("Date") or "").strip() for r in rows if (r.get("Date") or "").strip()]
    return Counter(dates).most_common(1)[0][0] if dates else None

def _looks_like_collection(row: dict) -> bool:
    """
    Heuristic for collection lines:
      - Account has a value
      - Collected From has a value
      - Pay has something (we'll parse it)
    """
    return bool(
        (row.get("Account") or "").strip()
        and (row.get("Collected From") or "").strip()
        and str(row.get("Pay") or "").strip()
    )

# ----------------------------
# Collection post-processing
# ----------------------------

def fix_collections(rows: list[dict], default_date: str | None = None) -> list[dict]:
    """
    Enforce collection rules:
      - Type='Collection' for rows that look like collections
      - Pay parsed to a float (Stop Rate supported)
      - Date backfilled from the *next* row with same Postcode
      - If still missing, use default_date (most-common file date)
      - If Items missing on collection, set Items=1
    Mutates and returns rows.
    """
    pending_by_pc: dict[str, list[dict]] = {}  # postcode -> [row, ...]

    for r in rows:
        if _looks_like_collection(r):
            r["Type"] = "Collection"

            # Pay → numeric
            pay = _to_money(r.get("Pay"))
            r["Pay"] = pay if pay is not None else 0.0

            # Items default to 1 on Collection if missing
            if _to_int(r.get("Items")) is None:
                r["Items"] = 1

            # queue for date backfill if missing
            if not (r.get("Date") or "").strip():
                pc = (r.get("Postcode") or "").strip()
                if pc:
                    pending_by_pc.setdefault(pc, []).append(r)

        # When a dated row appears, satisfy any earlier undated pending rows for same postcode
        pc = (r.get("Postcode") or "").strip()
        if pc in pending_by_pc and (r.get("Date") or "").strip():
            for prev_row in pending_by_pc.pop(pc):
                prev_row["Date"] = r["Date"]

    # Any leftovers → use default_date if available
    if default_date:
        for lst in pending_by_pc.values():
            for pr in lst:
                pr["Date"] = default_date

    return rows

# ----------------------------
# CSV writer
# ----------------------------

def rows_to_csv_bytes(rows: list[dict]) -> bytes:
    """
    Write CSV in the fixed column order.
    Formats Pay/Enhancement to 2dp; Items to int if possible.
    """
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CSV_COLUMNS)

    for r in rows:
        # coerce visible types
        items = _to_int(r.get("Items"))
        pay = _to_money(r.get("Pay"))
        enh = _to_money(r.get("Enhancement"))

        row_out = {
            **{k: "" for k in CSV_COLUMNS},  # defaults
            **{k: (r.get(k) or "") for k in r.keys()},  # whatever parser produced
        }

        # normalize fields
        if items is not None:
            row_out["Items"] = items
        if pay is not None:
            row_out["Pay"] = f"{pay:.2f}"
        if enh is not None:
            row_out["Enhancement"] = f"{enh:.2f}"

        # ensure Date normalized (if present)
        if row_out.get("Date"):
            row_out["Date"] = _to_iso(row_out["Date"])

        w.writerow([row_out.get(col, "") for col in CSV_COLUMNS])

    return buf.getvalue().encode("utf-8")

# ----------------------------
# YOUR parser hook
# ----------------------------

def parse_pdf_to_rows(pdf_bytes: bytes) -> list[dict]:
    """
    <<< plug YOUR existing PDF parsing & table-normalisation here >>>
    Must return a list of dicts using the CSV column names above
    (keys missing in some rows are fine).
    """
    # EXAMPLE ONLY (empty output) – replace with your real parser call.
    # e.g.:
    #   from your_module import parse_all_rows
    #   return parse_all_rows(pdf_bytes)
    return []  # <-- replace with real rows

# ----------------------------
# Endpoints
# ----------------------------

class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/process/url")
def process_from_url(body: UrlIn):
    try:
        with requests.get(body.file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            cl = r.headers.get("content-length")
            if cl and int(cl) > MAX_BYTES:
                return JSONResponse(status_code=413, content={"error": "file too large"})
            pdf_bytes = r.content
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"download_error: {e}"})

    with parse_lock:
        rows = parse_pdf_to_rows(pdf_bytes)

        # apply collection rules
        fallback_date = _most_common_date(rows)
        rows = fix_collections(rows, default_date=fallback_date)

        csv_bytes = rows_to_csv_bytes(rows)

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="output.csv"'},
    )

@app.post("/process/file")
async def process_from_upload(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        if len(pdf_bytes) > MAX_BYTES:
            return JSONResponse(status_code=413, content={"error": "file too large"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    with parse_lock:
        rows = parse_pdf_to_rows(pdf_bytes)

        # apply collection rules
        fallback_date = _most_common_date(rows)
        rows = fix_collections(rows, default_date=fallback_date)

        csv_bytes = rows_to_csv_bytes(rows)

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{file.filename or "output"}.csv"'},
    )


