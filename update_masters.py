#!/usr/bin/env python3
"""
USMS Masters NQT scraper → Supabase standards_usa

v5.0  (SCY + LCM)  - parses the official USMS NQT page (HTML tables)
- Inserts BOTH SCY (Spring Nationals) and LCM (Summer Nationals) NQTs when present.
- Avoids wiping all Masters rows: deletes only the season+course being refreshed.
- Keeps existing PDFPlumber-based functions as optional fallback.

Table destination: standards_usa
Key fields inserted:
  ciclo, season_year, standard_type="MASTERS", nivel="NQT",
  genero (M/F), edad (e.g., 40-44), estilo (Libre/Espalda/Pecho/Mariposa/Combinado),
  distancia_m, curso (SCY/LCM), tiempo_s

Env:
  SUPABASE_URL, SUPABASE_KEY
  (optional for email reports) MAIL_USERNAME/EMAIL_USER, MAIL_PASSWORD/EMAIL_PASSWORD
"""
import os
import re
import io
import datetime
import requests
import pdfplumber
import smtplib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from supabase import create_client

# Optional but available in your environment; used for robust HTML table extraction
from bs4 import BeautifulSoup
import pandas as pd

# ----------------------------
# CONFIG
# ----------------------------
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL / SUPABASE_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

EMAIL_SENDER = os.environ.get("MAIL_USERNAME") or os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("MAIL_PASSWORD") or os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER") or "vorrabermauro@gmail.com"

USMS_NQT_URL = os.environ.get(
    "USMS_NQT_URL",
    "https://www.usms.org/events/national-championships/pool-national-championships/national-qualifying-times"
)

# Optional PDF fallback URLs (if you want to pin per year / in case HTML changes)
PDF_URL_SCY = os.environ.get("USMS_NQT_PDF_SCY")  # e.g. "...spring nationals nqts.pdf"
PDF_URL_LCM = os.environ.get("USMS_NQT_PDF_LCM")  # e.g. "...summer nationals nqts.pdf"

# Stats
STATS = {
    "extracted": 0,
    "inserted": 0,
    "male": 0,
    "female": 0,
    "courses": {},  # course -> count
    "seasons": {},  # season_year -> count
    "errors": 0,
}

# ----------------------------
# Helpers
# ----------------------------
def enviar_reporte_email(log_body: str, status: str = "SUCCESS") -> None:
    """Send final report via email (optional)."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        print("⚠️  Email creds not found; skipping email report.")
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = f"Bot de Natación <{EMAIL_SENDER}>"
        msg["To"] = EMAIL_RECEIVER

        icon = "🟢" if status == "SUCCESS" else "🔴"
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg["Subject"] = f"{icon} Reporte USMS Masters NQT - {timestamp}"

        body = f"""Hola Mauro,

Resultado de la ejecución automática de estándares Masters (USMS NQT).

--------------------------------------------------
REPORTE DE EJECUCIÓN
--------------------------------------------------
Version: USMS_MASTERS_NQT_v5.0_MULTI_COURSE
Timestamp: {timestamp}

{log_body}

--------------------------------------------------
Detalles Técnicos:
- Origen: USMS NQT (HTML tables) + PDF fallback opcional
- Destino: Supabase (Tabla: standards_usa)

Saludos,
Tu Ferrari de Datos 🏎️
"""
        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        print("📧 Email de reporte enviado correctamente.")
    except Exception as e:
        print(f"❌ Error enviando email: {e}")

def clean_time(time_str) -> Optional[float]:
    """Convert 'mm:ss.xx' or 'ss.xx' to seconds (float)."""
    try:
        if time_str is None:
            return None
        s = str(time_str).strip().replace("*", "").replace("+", "")
        if s == "" or s.upper() in {"NO TIME", "NT"}:
            return None
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            if len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(s)
    except Exception:
        return None

def map_stroke_usms_to_es(stroke_raw: str) -> str:
    r = (stroke_raw or "").strip().upper()
    if "FREE" in r:
        return "Libre"
    if "BACK" in r:
        return "Espalda"
    if "BREAST" in r:
        return "Pecho"
    if "FLY" in r:
        return "Mariposa"
    # USMS uses 200 IM / 400 IM
    if r.endswith("IM") or " IM" in r:
        return "Combinado"
    return "Unknown"

def parse_event(event: str) -> Optional[Tuple[int, str]]:
    """
    USMS events look like: '50 Free', '100 Back', '400 IM', '1500 Free'
    Returns (distance_m, estilo_es)
    """
    if not event:
        return None
    event = str(event).replace("\n", " ").strip()
    m = re.match(r"^\s*(\d+)\s+(.*)\s*$", event)
    if not m:
        return None
    dist = int(m.group(1))
    stroke_part = m.group(2).strip()
    estilo = map_stroke_usms_to_es(stroke_part)
    return dist, estilo

@dataclass
class SectionInfo:
    season_year: str
    course: str  # SCY / LCM
    gender: str  # M / F

def iter_sections_with_tables(html: str) -> List[Tuple[SectionInfo, str]]:
    """
    Find H3 sections like:
      '2026 USMS Spring Nationals (SCY) NQTs - Women'
      '2025 USMS Summer Nationals (LCM) NQTs - Men'
    and return the adjacent table HTML.
    """
    soup = BeautifulSoup(html, "lxml")
    out: List[Tuple[SectionInfo, str]] = []

    for h3 in soup.find_all(["h3", "h2"]):
        title = h3.get_text(" ", strip=True)
        m = re.search(r"(\d{4}).*\((SCY|LCM)\).*-\s*(Women|Men)\s*$", title, re.IGNORECASE)
        if not m:
            continue
        season_year = m.group(1)
        course = m.group(2).upper()
        gender = "F" if m.group(3).lower() == "women" else "M"

        # find next <table>
        table = h3.find_next("table")
        if not table:
            continue
        out.append((SectionInfo(season_year=season_year, course=course, gender=gender), str(table)))
    return out

def parse_table_to_rows(info: SectionInfo, table_html: str) -> List[Dict]:
    """
    Convert an NQT table to standards_usa rows.
    Table shape: rows=events, columns=age groups.
    """
    rows: List[Dict] = []
    # pandas parses html table → DataFrame
    dfs = pd.read_html(table_html)
    if not dfs:
        return rows
    df = dfs[0].copy()

    # Normalize column names
    # Typically first column is "Event"
    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    # Ensure first col is Event-like
    event_col = cols[0]
    age_cols = cols[1:]

    for _, r in df.iterrows():
        event_val = r.get(event_col)
        parsed = parse_event(event_val)
        if not parsed:
            continue
        distancia, estilo = parsed
        for age in age_cols:
            age_range = str(age).strip().replace("\n", "").replace(" ", "")
            time_val = r.get(age)
            t_seg = clean_time(time_val)
            if t_seg is None:
                continue

            rows.append({
                "ciclo": info.season_year,
                "season_year": info.season_year,
                "standard_type": "MASTERS",
                "nivel": "NQT",
                "genero": info.gender,
                "edad": age_range,          # e.g. 40-44
                "estilo": estilo,           # Libre/Espalda/...
                "distancia_m": distancia,   # keep same numeric (yards/meters handled by course)
                "curso": info.course,       # SCY or LCM
                "tiempo_s": t_seg,
            })
    return rows

# ----------------------------
# PDF fallback (kept from v4)
# ----------------------------
def extraer_tiempo_testigo(table) -> float:
    """
    Used only for PDF gender inference (legacy). Returns a time for '50 Free' @ 18-24
    """
    try:
        header_idx = -1
        for i, row in enumerate(table):
            row_str = " ".join([str(c) for c in row if c])
            if "18-24" in row_str:
                header_idx = i
                break
        if header_idx == -1:
            return 9999.0

        for row in table[header_idx+1:]:
            row_clean = [str(c).replace("\n", " ").strip().upper() for c in row if c]
            if not row_clean:
                continue
            evt = row_clean[0]
            if "50" in evt and "FREE" in evt:
                time_val = row[1]
                t_seg = clean_time(time_val)
                if t_seg:
                    return t_seg
    except Exception:
        pass
    return 9999.0

def procesar_tablas_pdf(pdf_bytes: bytes, curso: str, season_year: str) -> List[Dict]:
    """
    Legacy PDF extractor (kept). Infers gender by comparing 50 Free 18-24.
    """
    data_to_insert: List[Dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            mapa_generos: Dict[int, str] = {}

            if len(tables) >= 2:
                t0 = extraer_tiempo_testigo(tables[0])
                t1 = extraer_tiempo_testigo(tables[1])
                if 0 < t0 < t1:
                    mapa_generos[0], mapa_generos[1] = "M", "F"
                elif 0 < t1 < t0:
                    mapa_generos[0], mapa_generos[1] = "F", "M"
                else:
                    mapa_generos[0], mapa_generos[1] = "F", "M"
            else:
                page_text = (page.extract_text() or "").upper()
                if "WOMEN" in page_text and "MEN" not in page_text:
                    mapa_generos[0] = "F"
                elif "MEN" in page_text and "WOMEN" not in page_text:
                    mapa_generos[0] = "M"
                else:
                    mapa_generos[0] = "F"

            for i, table in enumerate(tables):
                genero = mapa_generos.get(i, "X")

                header_idx = -1
                age_groups = []
                for idx_row, row in enumerate(table):
                    row_str = " ".join([str(c) for c in row if c])
                    if "18-24" in row_str:
                        header_idx = idx_row
                        age_groups = row
                        break
                if header_idx == -1:
                    continue

                for row in table[header_idx+1:]:
                    row = [col if col else "" for col in row]
                    if len(row) < 2:
                        continue

                    event_name = str(row[0]).replace("\n", " ").strip()
                    if not event_name or "RELAY" in event_name.upper():
                        continue

                    parsed = parse_event(event_name)
                    if not parsed:
                        continue
                    distancia, estilo = parsed

                    for col_idx, time_val in enumerate(row):
                        if col_idx == 0:
                            continue
                        if col_idx >= len(age_groups):
                            break
                        age_range = str(age_groups[col_idx]).replace("\n", "").strip().replace(" ", "")
                        t_seg = clean_time(time_val)
                        if t_seg is None:
                            continue

                        data_to_insert.append({
                            "ciclo": season_year,
                            "season_year": season_year,
                            "standard_type": "MASTERS",
                            "nivel": "NQT",
                            "genero": genero,
                            "edad": age_range,
                            "estilo": estilo,
                            "distancia_m": distancia,
                            "curso": curso,
                            "tiempo_s": t_seg,
                        })
    return data_to_insert

# ----------------------------
# Supabase write
# ----------------------------
def delete_existing_for(season_year: str, curso: str) -> None:
    # delete only NQT Masters for that season+course
    supabase.table("standards_usa") \
        .delete() \
        .eq("standard_type", "MASTERS") \
        .eq("nivel", "NQT") \
        .eq("season_year", season_year) \
        .eq("curso", curso) \
        .execute()

def insert_batches(rows: List[Dict], batch_size: int = 500) -> None:
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]
        supabase.table("standards_usa").insert(batch).execute()
        print(f"   💉 Insert lote {i} a {min(i+batch_size, total)}")

# ----------------------------
# Main
# ----------------------------
def ejecutar_cazador() -> None:
    print("🦈 USMS Masters NQT Scraper v5.0 (SCY + LCM)")

    all_rows: List[Dict] = []
    try:
        r = requests.get(USMS_NQT_URL, timeout=30)
        r.raise_for_status()
        sections = iter_sections_with_tables(r.text)

        if not sections:
            raise RuntimeError("No sections/tables found on USMS NQT page (structure changed?)")

        for info, table_html in sections:
            rows = parse_table_to_rows(info, table_html)
            if not rows:
                continue
            all_rows.extend(rows)

            # stats
            STATS["courses"][info.course] = STATS["courses"].get(info.course, 0) + len(rows)
            STATS["seasons"][info.season_year] = STATS["seasons"].get(info.season_year, 0) + len(rows)
            for x in rows:
                if x["genero"] == "M":
                    STATS["male"] += 1
                elif x["genero"] == "F":
                    STATS["female"] += 1
            STATS["extracted"] += len(rows)

        if not all_rows:
            raise RuntimeError("Parsed 0 rows from USMS NQT page")

        # Delete+Insert per (season_year, curso) to avoid wiping everything
        keys = sorted({(x["season_year"], x["curso"]) for x in all_rows})
        print(f"   🔑 Refresh keys: {keys}")
        for season_year, curso in keys:
            delete_existing_for(season_year, curso)
            subset = [x for x in all_rows if x["season_year"] == season_year and x["curso"] == curso]
            insert_batches(subset, batch_size=500)

        STATS["inserted"] = len(all_rows)

        log_final = (
            f"[USMS_MASTERS_NQT] Extracted={STATS['extracted']} Inserted={STATS['inserted']} "
            f"Male={STATS['male']} Female={STATS['female']} "
            f"Courses={STATS['courses']} Seasons={STATS['seasons']}"
        )
        print("\n" + log_final)
        enviar_reporte_email(log_final, "SUCCESS")
        return

    except Exception as e:
        print(f"⚠️  HTML scrape failed: {e}")
        STATS["errors"] += 1

    # Optional PDF fallback if URLs are provided
    try:
        fallback_rows: List[Dict] = []
        if PDF_URL_SCY:
            rr = requests.get(PDF_URL_SCY, timeout=30)
            rr.raise_for_status()
            # try to infer season year from URL; fallback current year
            y = re.search(r"(20\d{2})", PDF_URL_SCY)
            season_year = y.group(1) if y else str(datetime.datetime.now().year)
            fallback_rows.extend(procesar_tablas_pdf(rr.content, "SCY", season_year))
        if PDF_URL_LCM:
            rr = requests.get(PDF_URL_LCM, timeout=30)
            rr.raise_for_status()
            y = re.search(r"(20\d{2})", PDF_URL_LCM)
            season_year = y.group(1) if y else str(datetime.datetime.now().year)
            fallback_rows.extend(procesar_tablas_pdf(rr.content, "LCM", season_year))

        if not fallback_rows:
            raise RuntimeError("No PDF fallback URLs configured or 0 rows extracted")

        keys = sorted({(x["season_year"], x["curso"]) for x in fallback_rows})
        print(f"   🔑 Refresh keys (PDF): {keys}")
        for season_year, curso in keys:
            delete_existing_for(season_year, curso)
            subset = [x for x in fallback_rows if x["season_year"] == season_year and x["curso"] == curso]
            insert_batches(subset, batch_size=500)

        log_final = f"[USMS_MASTERS_NQT_PDF] Inserted={len(fallback_rows)} keys={keys}"
        print("\n" + log_final)
        enviar_reporte_email(log_final, "SUCCESS")
        return

    except Exception as e:
        STATS["errors"] += 1
        msg = f"FAILURE: {e}"
        print(msg)
        enviar_reporte_email(msg, "FAILURE")


if __name__ == "__main__":
    ejecutar_cazador()
