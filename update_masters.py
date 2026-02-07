
#!/usr/bin/env python3
"""
USMS Masters NQT Scraper v6.1 (PDF-first, SCY + LCM)
- Fix: Ensure Women (F) tables are parsed for LCM PDF (and SCY).
- Fix: Delete only the refreshed keys (season_year+curso+genero) to avoid wiping missing gender.
- Fix: ciclo populated (set to season_year for Masters).
- Output schema matches standards_usa: (ciclo, genero, edad, estilo, distancia_m, curso, nivel, tiempo_s, season_year, standard_type)

Designed to run in GitHub Actions.
"""

import os
import io
import re
import sys
import datetime
import smtplib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import pdfplumber
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from supabase import create_client

# ----------------------------
# Config
# ----------------------------
DEFAULT_PDF_SCY = "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/2026%20spring%20nationals/2026%20usms%20spring%20nationals%20nqts.pdf"
DEFAULT_PDF_LCM = "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/2025%20summer%20nationals/2025%20usms%20summer%20nationals%20nqts%20v1.pdf"

# Email
EMAIL_RECEIVER_DEFAULT = "vorrabermauro@gmail.com"

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("FATAL: Missing SUPABASE_URL or SUPABASE_KEY env vars.")
    sys.exit(2)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

EMAIL_SENDER = os.environ.get("MAIL_USERNAME") or os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("MAIL_PASSWORD") or os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER") or EMAIL_RECEIVER_DEFAULT

REQUIRE_BOTH_GENDERS = (os.environ.get("REQUIRE_BOTH_GENDERS") or "").strip().lower() in ("1","true","yes","y")

@dataclass
class Stats:
    extracted: int = 0
    inserted: int = 0
    deleted: int = 0
    errors: int = 0
    by_course_gender: Dict[Tuple[str,str], int] = None  # (curso, genero) -> n

    def __post_init__(self):
        if self.by_course_gender is None:
            self.by_course_gender = {}

STATS = Stats()

# ----------------------------
# Helpers
# ----------------------------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Connection": "keep-alive",
}

AGE_RE = re.compile(r"\b(18\s*-\s*24|25\s*-\s*29|30\s*-\s*34|35\s*-\s*39|40\s*-\s*44|45\s*-\s*49|50\s*-\s*54|55\s*-\s*59|60\s*-\s*64|65\s*-\s*69|70\s*-\s*74|75\s*-\s*79|80\s*-\s*84|85\s*-\s*89|90\s*-\s*94|95\+)\b")

def normalize_age(s: str) -> str:
    s = (s or "").strip().replace("–","-").replace("—","-")
    s = re.sub(r"\s+", "", s)  # "40 - 44" -> "40-44"
    return s

def clean_time(time_str: str) -> Optional[float]:
    if time_str is None:
        return None
    t = str(time_str).strip()
    if not t or t.upper() in ("NO","NO TIME","NT","—","-"):
        return None
    t = t.replace("*","").replace("+","").strip()
    # common: 1:08.00 or 29.35
    try:
        if ":" in t:
            parts = t.split(":")
            if len(parts) == 2:
                return float(parts[0]) * 60.0 + float(parts[1])
            if len(parts) == 3:
                return float(parts[0]) * 3600.0 + float(parts[1]) * 60.0 + float(parts[2])
        return float(t)
    except Exception:
        return None

def detect_gender_from_text(page_text_upper: str) -> Optional[str]:
    """Return 'F' if page is clearly Women, 'M' if clearly Men, else None."""
    if not page_text_upper:
        return None
    has_w = "WOMEN" in page_text_upper
    has_m = "MEN" in page_text_upper
    if has_w and not has_m:
        return "F"
    if has_m and not has_w:
        return "M"
    return None

def gender_order_from_text(page_text_upper: str) -> Optional[List[str]]:
    """
    If both WOMEN and MEN appear, decide which comes first in the page text
    to map table indices deterministically when velocity test is ambiguous.
    """
    if not page_text_upper:
        return None
    iw = page_text_upper.find("WOMEN")
    im = page_text_upper.find("MEN")
    if iw == -1 or im == -1:
        return None
    return ["F","M"] if iw < im else ["M","F"]

def extract_probe_50_free_seconds(table: List[List[str]]) -> float:
    """
    Try to extract an indicative time from the table to compare genders
    (Men should generally have faster 50 Free than Women).
    Return large number on failure.
    """
    try:
        header_idx = -1
        for i, row in enumerate(table):
            row_str = " ".join([str(c) for c in row if c]).upper()
            if "18-24" in row_str or "18 - 24" in row_str:
                header_idx = i
                break
        if header_idx == -1:
            return 9999.0

        # Scan for 50 Free row
        for row in table[header_idx+1:]:
            row_clean = [str(c).replace("\n"," ").strip().upper() for c in row if c]
            if not row_clean:
                continue
            evt = row_clean[0]
            if evt.startswith("50") and ("FREE" in evt or "FREESTYLE" in evt):
                # first age group time
                t = clean_time(row[1] if len(row) > 1 else None)
                return t if t is not None else 9999.0
    except Exception:
        pass
    return 9999.0

def parse_table_rows(table: List[List[str]], curso: str, season_year: str, genero: str) -> List[dict]:
    out: List[dict] = []
    header_idx = -1
    age_groups: List[str] = []
    # find header row with age groups
    for idx_row, row in enumerate(table):
        row_str = " ".join([str(c) for c in row if c])
        if "18-24" in row_str or "18 - 24" in row_str:
            header_idx = idx_row
            age_groups = [normalize_age(str(c)) for c in row]
            break

    if header_idx == -1:
        return out

    for row in table[header_idx+1:]:
        if not row:
            continue
        # normalize to strings
        row = [("" if c is None else str(c).replace("\n"," ").strip()) for c in row]
        if len(row) < 2:
            continue

        event_name = row[0]
        if not event_name:
            continue
        up = event_name.upper()
        if "RELAY" in up:
            continue

        # Expect: "50 Free", "100 Back", etc.
        parts = event_name.split()
        if not parts or not parts[0].isdigit():
            continue

        distancia = int(parts[0])
        estilo_raw = " ".join(parts[1:]).upper()

        # Canonical style names in DB (match TS normStyle output):
        #   Freestyle / Backstroke / Breaststroke / Butterfly / IM
        estilo = None
        if "FREE" in estilo_raw:
            estilo = "Freestyle"
        elif "BACK" in estilo_raw:
            estilo = "Backstroke"
        elif "BREAST" in estilo_raw:
            estilo = "Breaststroke"
        elif "FLY" in estilo_raw:
            estilo = "Butterfly"
        elif "IM" in estilo_raw:
            estilo = "IM"
        else:
            # skip unknown
            continue
        for col_idx in range(1, min(len(row), len(age_groups))):
            age_range = age_groups[col_idx]
            if not age_range or not AGE_RE.search(age_range.replace("","")):
                # still allow common "40-44" etc; age_range is already normalized.
                pass

            t_seg = clean_time(row[col_idx])
            if t_seg is None:
                continue

            out.append({
                "ciclo": season_year,              # keep consistent (Masters)
                "genero": genero,                  # 'M' / 'F'
                "edad": normalize_age(age_range),
                "estilo": estilo,
                "distancia_m": distancia,
                "curso": curso,                    # 'SCY' / 'LCM'
                "nivel": "NQT",
                "tiempo_s": t_seg,
                "standard_type": "MASTERS",
                "season_year": season_year
            })
    return out

def parse_pdf(pdf_bytes: bytes, curso: str, season_year: str) -> List[dict]:
    rows: List[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"   📄 Analizando PDF ({curso}, season_year={season_year}) pages={len(pdf.pages)}")
        for pageno, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            page_text_upper = page_text.upper()

            tables = page.extract_tables() or []
            if not tables:
                continue

            # Determine mapping of table index -> gender
            table_gender: Dict[int, str] = {}

            if len(tables) == 1:
                g = detect_gender_from_text(page_text_upper) or "X"
                table_gender[0] = g
            else:
                # If both genders appear on page, try to map by probe times
                t0 = extract_probe_50_free_seconds(tables[0])
                t1 = extract_probe_50_free_seconds(tables[1])
                # debug
                # print(f"      🧪 p{pageno}: probe t0={t0} t1={t1}")
                if t0 != 9999.0 and t1 != 9999.0 and t0 != t1:
                    # faster == Men
                    if t0 < t1:
                        table_gender[0], table_gender[1] = "M", "F"
                    else:
                        table_gender[0], table_gender[1] = "F", "M"
                else:
                    # fallback by text ordering if possible
                    order = gender_order_from_text(page_text_upper)
                    if order:
                        table_gender[0], table_gender[1] = order[0], order[1]
                    else:
                        # last resort: assume first Women then Men (common formatting)
                        table_gender[0], table_gender[1] = "F", "M"

                # any extra tables: mark unknown (they will be skipped if header missing)
                for i in range(2, len(tables)):
                    table_gender[i] = detect_gender_from_text(page_text_upper) or "X"

            for i, table in enumerate(tables):
                genero = table_gender.get(i, "X")
                if genero not in ("M","F"):
                    # If header says WOMEN or MEN clearly, use it; else skip unknown
                    g = detect_gender_from_text(page_text_upper)
                    if g in ("M","F"):
                        genero = g
                    else:
                        continue

                parsed = parse_table_rows(table, curso=curso, season_year=season_year, genero=genero)
                if parsed:
                    rows.extend(parsed)
    return rows

def send_email(subject: str, body: str):
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        print("⚠️ Email creds missing; skipping email.")
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = f"Bot de Natación <{EMAIL_SENDER}>"
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        print("📧 Email de reporte enviado correctamente.")
    except Exception as e:
        print(f"❌ Error enviando email: {e}")

def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    r.raise_for_status()
    return r.content

def delete_existing(keys: List[Tuple[str,str,str]]) -> int:
    """
    Delete existing rows only for the extracted keys:
      (season_year, curso, genero)
    This prevents wiping Women if Women extraction fails.
    """
    deleted_total = 0
    # de-dup keys
    keys = sorted(set(keys))
    for season_year, curso, genero in keys:
        resp = sb.table("standards_usa") \
            .delete() \
            .eq("standard_type", "MASTERS") \
            .eq("nivel", "NQT") \
            .eq("season_year", season_year) \
            .eq("curso", curso) \
            .eq("genero", genero) \
            .execute()
        # supabase-py returns .data with deleted rows (may be None depending)
        n = len(resp.data) if getattr(resp, "data", None) else 0
        deleted_total += n
    return deleted_total

def insert_batches(rows: List[dict], batch_size: int = 250) -> int:
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        sb.table("standards_usa").insert(batch).execute()
        inserted += len(batch)
        print(f"   💉 Insert batch: +{len(batch)} (total={inserted})")
    return inserted

def summarize(rows: List[dict]) -> str:
    by = {}
    for r in rows:
        key = (r["curso"], r["genero"])
        by[key] = by.get(key, 0) + 1
    lines = []
    for (curso, genero), n in sorted(by.items()):
        lines.append(f"{curso}/{genero}={n}")
    return ", ".join(lines)

def main():
    print("🦈 USMS Masters NQT Scraper v6.1 (PDF-first, SCY + LCM, gender-safe)")
    pdf_scy = os.environ.get("PDF_URL_SCY") or DEFAULT_PDF_SCY
    pdf_lcm = os.environ.get("PDF_URL_LCM") or DEFAULT_PDF_LCM
    # Allow explicit season years; else infer from URL path (first 4-digit year found)
    season_scy = os.environ.get("SEASON_YEAR_SCY") or (re.search(r"(20\d{2})", pdf_scy) or [None])[0]
    season_lcm = os.environ.get("SEASON_YEAR_LCM") or (re.search(r"(20\d{2})", pdf_lcm) or [None])[0]
    # If inference failed, fallback to current year
    now_year = str(datetime.datetime.now().year)
    season_scy = season_scy or now_year
    season_lcm = season_lcm or now_year

    print(f"   SCY PDF: {pdf_scy}")
    print(f"   LCM PDF: {pdf_lcm}")

    all_rows: List[dict] = []
    try:
        scy_bytes = fetch_pdf(pdf_scy)
        scy_rows = parse_pdf(scy_bytes, curso="SCY", season_year=season_scy)
        all_rows.extend(scy_rows)
        print(f"   ✅ SCY extracted={len(scy_rows)} | {summarize(scy_rows)}")
    except Exception as e:
        STATS.errors += 1
        print(f"❌ SCY scrape failed: {e}")

    try:
        lcm_bytes = fetch_pdf(pdf_lcm)
        lcm_rows = parse_pdf(lcm_bytes, curso="LCM", season_year=season_lcm)
        all_rows.extend(lcm_rows)
        print(f"   ✅ LCM extracted={len(lcm_rows)} | {summarize(lcm_rows)}")
    except Exception as e:
        STATS.errors += 1
        print(f"❌ LCM scrape failed: {e}")

    # Basic sanity check
    if not all_rows:
        msg = "FAILURE: 0 rows extracted."
        print(msg)
        send_email("🔴 USMS Masters NQT v6.1 FAILURE (0 rows)", msg)
        sys.exit(1)

    # Optional: enforce both genders presence per course
    if REQUIRE_BOTH_GENDERS:
        for curso in ("SCY","LCM"):
            has_m = any(r["curso"] == curso and r["genero"] == "M" for r in all_rows)
            has_f = any(r["curso"] == curso and r["genero"] == "F" for r in all_rows)
            if not (has_m and has_f):
                msg = f"FAILURE: Missing gender in {curso}: has_m={has_m} has_f={has_f}"
                print(msg)
                send_email("🔴 USMS Masters NQT v6.1 FAILURE (missing gender)", msg)
                sys.exit(1)

    # Update stats
    STATS.extracted = len(all_rows)
    for r in all_rows:
        STATS.by_course_gender[(r["curso"], r["genero"])] = STATS.by_course_gender.get((r["curso"], r["genero"]), 0) + 1

    # Delete only the refreshed keys (season_year, curso, genero)
    keys = sorted(set((r["season_year"], r["curso"], r["genero"]) for r in all_rows))
    deleted = delete_existing(keys)
    STATS.deleted = deleted

    inserted = insert_batches(all_rows, batch_size=250)
    STATS.inserted = inserted

    summary = [
        f"[USMS_MASTERS_NQT v6.1] SUCCESS",
        f"Extracted={STATS.extracted} Inserted={STATS.inserted} Deleted={STATS.deleted} Errors={STATS.errors}",
        f"Breakdown: {', '.join([f'{k[0]}/{k[1]}={v}' for k,v in sorted(STATS.by_course_gender.items())])}",
        f"Keys refreshed: {keys}",
    ]
    msg = "\n".join(summary)
    print(msg)
    send_email("🟢 USMS Masters NQT v6.1 SUCCESS", msg)

if __name__ == "__main__":
    main()
