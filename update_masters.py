#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🦈 USMS Masters NQT Scraper v6.0 (PDF-first, SCY + LCM)

Motivo: USMS suele bloquear scraping HTML desde GitHub Actions (403).
Solución: consumir PDFs oficiales (CDN azurefd) y poblar standards_usa como standard_type=MASTERS.

Tabla destino: standards_usa
Columnas usadas (mínimas):
- standard_type (str)  -> "MASTERS"
- season_year   (str)  -> "2026" / "2025" (año del meet/NQT)
- genero        (str)  -> "M" / "F"
- edad          (str)  -> "18-24", "25-29", ... "80-84" (sin prefijo MASTER)
- estilo        (str)  -> "FREE","BACK","BREAST","FLY","IM"
- distancia_m   (int)  -> 50, 100, 200, 400, 500, 800, 1000, 1500, 1650
- nivel         (str)  -> "NQT"
- tiempo_s      (float)-> segundos
- curso         (str)  -> "SCY" o "LCM"

ENV opcional:
- FETCH_MODE: "pdf" (default) | "auto" (idéntico en v6; siempre usa PDFs)
- USMS_SCY_PDF_URL: override URL SCY
- USMS_LCM_PDF_URL: override URL LCM
- SUPABASE_URL, SUPABASE_KEY (service role recomendado)
- MAIL_USERNAME, MAIL_PASSWORD (opcional para reporte email)
- MAIL_TO (opcional; si no, usa MAIL_USERNAME)
"""

import os
import re
import io
import sys
import datetime
import requests
import pdfplumber
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from supabase import create_client

VERSION = "6.0"

# PDFs oficiales (CDN azurefd) – default robusto
DEFAULT_SCY_PDF_URL = (
    "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/"
    "2026%20spring%20nationals/2026%20usms%20spring%20nationals%20nqts.pdf"
)
DEFAULT_LCM_PDF_URL = (
    "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/"
    "2025%20summer%20nationals/2025%20usms%20summer%20nationals%20nqts%20v1.pdf"
)

AGE_COLS = [
    "18-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60-64","65-69","70-74","75-79","80-84"
]

STYLE_MAP = {
    "FREE": "FREE", "FREESTYLE": "FREE", "LIBRE": "FREE",
    "BACK": "BACK", "BACKSTROKE": "BACK", "ESPALDA": "BACK",
    "BREAST": "BREAST", "BREASTSTROKE": "BREAST", "PECHO": "BREAST",
    "FLY": "FLY", "BUTTERFLY": "FLY", "MARIPOSA": "FLY",
    "IM": "IM", "MEDLEY": "IM", "COMBINADO": "IM",
}

def _env(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else default

def send_report_email(subject: str, body: str, status: str) -> None:
    user = _env("MAIL_USERNAME")
    pwd = _env("MAIL_PASSWORD")
    to = _env("MAIL_TO", user)
    if not user or not pwd or not to:
        print("ℹ️ Email not configured (MAIL_USERNAME/MAIL_PASSWORD/MAIL_TO). Skipping email.")
        return

    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = f"[USMS_MASTERS_NQT v{VERSION}] {status} | {subject}"
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(user, pwd)
            s.sendmail(user, [to], msg.as_string())
        print("📧 Email de reporte enviado correctamente.")
    except Exception as e:
        print(f"⚠️ No se pudo enviar email: {e}")

def time_to_seconds(t: str) -> float | None:
    s = (t or "").strip()
    if not s or s.upper() == "NO TIME":
        return None
    # Normalizamos separadores raros
    s = s.replace("’", "'").replace("−", "-")
    # "1:04.07" or "28.81"
    m = re.match(r"^(\d+):(\d+(?:\.\d+)?)$", s)
    if m:
        mm = int(m.group(1))
        ss = float(m.group(2))
        return mm * 60.0 + ss
    # "27.59"
    m2 = re.match(r"^(\d+(?:\.\d+)?)$", s)
    if m2:
        return float(m2.group(1))
    return None

def parse_pdf_bytes(pdf_bytes: bytes, course: str) -> list[dict]:
    """
    Extrae filas de NQT desde el PDF.
    """
    text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            # extraer texto plano (suficiente: el PDF es tabular simple)
            text += (p.extract_text() or "") + "\n"

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []

    rows: list[dict] = []
    gender: str | None = None

    for ln in lines:
        up = ln.upper().strip()

        if up == "WOMEN":
            gender = "F"
            continue
        if up == "MEN":
            gender = "M"
            continue

        # Saltar headers de tabla
        if up.startswith("EVENT "):
            continue
        if up.startswith("USMS NATIONAL QUALIFYING TIMES"):
            continue
        if up.startswith("FORMULA:") or up.startswith("NOTE:"):
            continue

        # Lineas de eventos: "50 Free 28.81 28.18 ... 53.25"
        m = re.match(r"^(\d+)\s+([A-Za-z]+)\s+(.*)$", ln)
        if not m or not gender:
            continue

        dist = int(m.group(1))
        style_word = m.group(2).strip().upper()
        tail = m.group(3).strip()

        style = STYLE_MAP.get(style_word, None)
        if not style:
            # por si viene "FRE" o algo raro:
            if style_word.startswith("FRE"):
                style = "FREE"
            else:
                continue

        # tokens de tiempos (con manejo de "NO TIME")
        toks = tail.split()
        collapsed: list[str] = []
        i = 0
        while i < len(toks):
            if i + 1 < len(toks) and toks[i].upper() == "NO" and toks[i + 1].upper() == "TIME":
                collapsed.append("NO TIME")
                i += 2
                continue
            collapsed.append(toks[i])
            i += 1

        # En algunos PDFs puede aparecer una nota al final; intentamos quedarnos solo con la cantidad correcta
        if len(collapsed) < len(AGE_COLS):
            # No hay suficientes columnas -> descartamos
            continue
        if len(collapsed) > len(AGE_COLS):
            collapsed = collapsed[:len(AGE_COLS)]

        for age_label, t in zip(AGE_COLS, collapsed):
            secs = time_to_seconds(t)
            if secs is None:
                continue
            rows.append({
                "genero": gender,
                "edad": age_label,
                "estilo": style,
                "distancia_m": dist,
                "tiempo_s": secs,
                "curso": course,
            })

    return rows

def fetch_pdf(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content

def derive_season_year_from_url(url: str) -> str:
    # intenta detectar 4 dígitos del path (ej: .../2026%20spring%20nationals/...)
    m = re.search(r"(20\d{2})", url)
    return m.group(1) if m else ""

def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def upsert_rows_to_supabase(sb, rows: list[dict], season_year: str) -> tuple[int,int]:
    """
    Inserta (y normaliza) en standards_usa con standard_type=MASTERS y nivel=NQT.
    Devuelve (inserted, deleted)
    """
    if not rows:
        return (0, 0)

    # pre-delete por (season_year, curso) para evitar acumulación
    deleted = 0
    by_course = {}
    for r in rows:
        by_course.setdefault(r["curso"], 0)
        by_course[r["curso"]] += 1

    for course in sorted(by_course.keys()):
        try:
            # delete solo del conjunto MASTERS + NQT para ese año/curso
            resp = (
                sb.table("standards_usa")
                .delete()
                .match({"standard_type": "MASTERS", "season_year": season_year, "curso": course, "nivel": "NQT"})
                .execute()
            )
            # supabase-py: resp.data puede venir con filas borradas
            if getattr(resp, "data", None):
                deleted += len(resp.data)
        except Exception as e:
            print(f"⚠️ Delete pre-run failed for {season_year} {course}: {e}")

    payload = []
    for r in rows:
        payload.append({
            "standard_type": "MASTERS",
            "season_year": season_year,
            "genero": r["genero"],
            "edad": r["edad"],
            "estilo": r["estilo"],
            "distancia_m": int(r["distancia_m"]),
            "nivel": "NQT",
            "tiempo_s": float(r["tiempo_s"]),
            "curso": r["curso"],
        })

    inserted = 0
    for batch in chunked(payload, 500):
        sb.table("standards_usa").insert(batch).execute()
        inserted += len(batch)
        print(f"   💉 Insert batch: +{len(batch)} (total={inserted})")

    return (inserted, deleted)

def run():
    load_dotenv()

    sb_url = _env("SUPABASE_URL")
    sb_key = _env("SUPABASE_KEY")
    if not sb_url or not sb_key:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY")

    supabase = create_client(sb_url, sb_key)

    scy_url = _env("USMS_SCY_PDF_URL", DEFAULT_SCY_PDF_URL)
    lcm_url = _env("USMS_LCM_PDF_URL", DEFAULT_LCM_PDF_URL)

    print(f"\n🦈 USMS Masters NQT Scraper v{VERSION} (PDF-first, SCY + LCM)")
    print(f"   SCY PDF: {scy_url}")
    print(f"   LCM PDF: {lcm_url}\n")

    stats = {
        "extracted": 0,
        "inserted": 0,
        "deleted": 0,
        "errors": 0,
        "scy_rows": 0,
        "lcm_rows": 0,
    }

    log_lines = []
    started = datetime.datetime.utcnow().isoformat() + "Z"
    log_lines.append(f"Started: {started}")

    # --- SCY
    try:
        scy_bytes = fetch_pdf(scy_url)
        scy_rows = parse_pdf_bytes(scy_bytes, "SCY")
        stats["scy_rows"] = len(scy_rows)
        stats["extracted"] += len(scy_rows)
        scy_year = derive_season_year_from_url(scy_url)
        if not scy_year:
            scy_year = str(datetime.datetime.utcnow().year)
        ins, dele = upsert_rows_to_supabase(supabase, scy_rows, scy_year)
        stats["inserted"] += ins
        stats["deleted"] += dele
        log_lines.append(f"SCY: year={scy_year} extracted={len(scy_rows)} inserted={ins} deleted={dele}")
    except Exception as e:
        stats["errors"] += 1
        log_lines.append(f"SCY: ERROR {e}")
        print(f"⚠️ SCY failed: {e}")

    # --- LCM
    try:
        lcm_bytes = fetch_pdf(lcm_url)
        lcm_rows = parse_pdf_bytes(lcm_bytes, "LCM")
        stats["lcm_rows"] = len(lcm_rows)
        stats["extracted"] += len(lcm_rows)
        lcm_year = derive_season_year_from_url(lcm_url)
        if not lcm_year:
            lcm_year = str(datetime.datetime.utcnow().year)
        ins, dele = upsert_rows_to_supabase(supabase, lcm_rows, lcm_year)
        stats["inserted"] += ins
        stats["deleted"] += dele
        log_lines.append(f"LCM: year={lcm_year} extracted={len(lcm_rows)} inserted={ins} deleted={dele}")
    except Exception as e:
        stats["errors"] += 1
        log_lines.append(f"LCM: ERROR {e}")
        print(f"⚠️ LCM failed: {e}")

    finished = datetime.datetime.utcnow().isoformat() + "Z"
    log_lines.append(f"Finished: {finished}")
    log_lines.append(f"STATS: {stats}")

    ok = stats["inserted"] > 0 and stats["errors"] == 0
    status = "SUCCESS" if ok else ("PARTIAL" if stats["inserted"] > 0 else "FAILURE")

    summary = (
        f"[USMS_MASTERS_NQT v{VERSION}] {status} | "
        f"Inserted={stats['inserted']} Deleted={stats['deleted']} Extracted={stats['extracted']} "
        f"(SCY={stats['scy_rows']}, LCM={stats['lcm_rows']}) Errors={stats['errors']}"
    )
    print("\n" + summary + "\n")

    send_report_email("Run summary", "\n".join(log_lines) + "\n\n" + summary, status)

    if not ok:
        # exit non-zero si no insertó nada (para que el workflow falle visible)
        if stats["inserted"] == 0:
            sys.exit(1)

if __name__ == "__main__":
    run()
