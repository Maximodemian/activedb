import os
import re
import time
import json
import uuid
import smtplib
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from supabase import create_client


# ============================================================
# MDV Records Updater (World Aquatics)
# - Scrapea World Aquatics (WR/OR/PAN/SAM) para 50m/25m
# - Actualiza records_standards SOLO si el tiempo web es mejor
# - NO pisa pseudo-records: match por record_type obligatorio
# - Escribe logs en scraper_logs (modo tolerante a esquemas)
# - Envía email resumen si EMAIL_USER/PASS están presentes
# ============================================================

# --------------- Config base ---------------
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")  # app-password 16 dígitos (Google)

HEADLESS = os.environ.get("HEADLESS", "true").strip().lower() not in ("0", "false", "no")
SLOWMO_MS = int(os.environ.get("SLOWMO_MS", "0") or "0")
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "60000") or "60000")

# Si querés que inserte filas nuevas cuando no exista el registro base en records_standards:
ALLOW_INSERT_NEW = os.environ.get("ALLOW_INSERT_NEW", "false").strip().lower() in ("1", "true", "yes")

# Receptor del reporte (por defecto Mauro)
EMAIL_TO = os.environ.get("EMAIL_TO", "vorrabermauro@gmail.com")

# --------------- Normalización / mapeos ---------------
TRADUCCION_ESTILOS = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "MEDLEY": "Combinado",
    "IM": "Combinado",
}

# Debe coincidir con records_standards.record_scope de tu BD
MAPEO_SCOPE_DB = {
    "WR": "Mundial",
    "OR": "Olímpico",
    "PAN": "Panamericano",
    "SAM": "Sudamericano",
}

# WA usa piscina=50m/25m -> en tu BD pool_length suele ser LCM/SCM
MAPEO_PISCINA = {"50m": "LCM", "25m": "SCM"}

# Match obligatorio para NO pisar pseudo-records (mínimas, selección, etc.)
MAPEO_RECORD_TYPE = {
    "WR": "Récord Mundial",
    "OR": "Récord Olímpico",
    "PAN": "Récord Panamericano",
    "SAM": "Récord Sudamericano",
}

DEFAULT_CATEGORY = os.environ.get("DEFAULT_CATEGORY", "Open")  # WA = Open


# --------------- Utilidades ---------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_print(msg: str) -> None:
    print(msg, flush=True)


_TIME_RE = re.compile(
    r"^\s*(\d{1,2}:)?\d{1,2}:\d{2}([.,]\d{1,2})?\s*$|^\s*\d{1,3}([.,]\d{1,2})\s*$"
)


def time_str_to_ms(t_str: str) -> Optional[int]:
    """
    Acepta:
      - 1:52.34
      - 52.34
      - 00:01:52.34
      - 00:00:46.3
      - 1.54.50 (m.ss.cc típico de PDFs) -> 1:54.50
      - 46,30 (coma decimal)
    Retorna ms o None.
    """
    if not t_str:
        return None
    s = str(t_str).strip()
    if not s:
        return None

    s = s.replace(",", ".")

    # Caso PDF: "1.54.50" => 1:54.50
    if ":" not in s and s.count(".") == 2:
        a, b, c = s.split(".")
        if a.isdigit() and b.isdigit() and c.isdigit() and len(b) == 2:
            s = f"{int(a)}:{int(b):02d}.{int(c):02d}"

    if not _TIME_RE.match(s):
        return None

    parts = s.split(":")
    try:
        if len(parts) == 3:
            hh = int(parts[0])
            mm = int(parts[1])
            sec_part = parts[2]
        elif len(parts) == 2:
            hh = 0
            mm = int(parts[0])
            sec_part = parts[1]
        else:
            hh = 0
            mm = 0
            sec_part = parts[0]

        if "." in sec_part:
            ss_str, cc_str = sec_part.split(".", 1)
            ss = int(ss_str)
            if len(cc_str) == 1:
                cent = int(cc_str) * 10
            else:
                cent = int(cc_str[:2])
        else:
            ss = int(sec_part)
            cent = 0

        return (hh * 3600 + mm * 60 + ss) * 1000 + cent * 10
    except Exception:
        return None


def ms_to_clock_2dp(ms: int) -> str:
    """Formato HH:MM:SS.cc"""
    if ms is None:
        return ""
    if ms < 0:
        ms = 0
    total_seconds, ms_rem = divmod(ms, 1000)
    cent = round(ms_rem / 10)
    if cent == 100:
        cent = 0
        total_seconds += 1
    hh, rem = divmod(total_seconds, 3600)
    mm, ss = divmod(rem, 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{cent:02d}"


def extract_first_time(lines: List[str]) -> Optional[str]:
    for ln in lines:
        if time_str_to_ms(ln) is not None:
            return ln.strip()
    return None


def infer_gender(header: str) -> Optional[str]:
    h = (header or "").upper()
    # WOMEN primero (porque WOMEN contiene MEN)
    if "WOMEN" in h or "WOMAN" in h:
        return "F"
    if "MEN" in h or "MAN" in h:
        return "M"
    return None


def extract_distance(header: str) -> Optional[int]:
    h = header or ""
    m = re.search(r"(\d{2,4})\s*M", h.upper())
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def extract_stroke_db(header: str) -> Optional[str]:
    h = (header or "").upper()
    for k, v in TRADUCCION_ESTILOS.items():
        if k in h:
            return v
    return None


def compact_exc(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"


# --------------- Modelos ---------------
@dataclass
class ScrapedRecord:
    source: str
    source_url: str

    record_type_code: str  # WR/OR/PAN/SAM
    record_scope_db: str   # Mundial/Olímpico/...

    pool_length: str       # LCM/SCM
    category: str          # Open

    gender: str            # M/F
    distance: int
    stroke: str            # Libre/Espalda/...

    athlete_name: str
    time_clock_raw: str
    time_ms: int


# --------------- Logger tolerante a esquema ---------------
class SupaLogger:
    """
    Inserta logs en scraper_logs intentando varias formas:
    - extended: run_id, level, event, message, payload, created_at
    - legacy+: scope/prueba/atleta/tiempo_anterior/tiempo_nuevo (+ run_id, created_at)
    - legacy: scope/prueba/atleta/tiempo_anterior/tiempo_nuevo
    """
    def __init__(self, supa, run_id: str):
        self.supa = supa
        self.run_id = run_id

    def insert(self, data: Dict[str, Any]) -> None:
        extended = {
            "run_id": self.run_id,
            "level": data.get("level", "INFO"),
            "event": data.get("event", "log"),
            "message": data.get("message", ""),
            "payload": data.get("payload", None),
            "created_at": utc_now().isoformat(),
        }
        if self._try_insert(extended):
            return

        legacy_plus = {
            "scope": data.get("scope", data.get("record_scope", "")),
            "prueba": data.get("prueba", data.get("event_name", "")),
            "atleta": data.get("atleta", data.get("actor", "")),
            "tiempo_anterior": data.get("tiempo_anterior", data.get("old", "")),
            "tiempo_nuevo": data.get("tiempo_nuevo", data.get("new", "")),
            "run_id": self.run_id,
            "created_at": utc_now().isoformat(),
        }
        if self._try_insert(legacy_plus):
            return

        legacy = {
            "scope": legacy_plus["scope"],
            "prueba": legacy_plus["prueba"],
            "atleta": legacy_plus["atleta"],
            "tiempo_anterior": legacy_plus["tiempo_anterior"],
            "tiempo_nuevo": legacy_plus["tiempo_nuevo"],
        }
        self._try_insert(legacy)

    def _try_insert(self, row: Dict[str, Any]) -> bool:
        try:
            self.supa.table("scraper_logs").insert(row).execute()
            return True
        except Exception:
            return False


# --------------- Supabase helpers ---------------
def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if missing:
        raise RuntimeError(f"Faltan variables de entorno requeridas: {', '.join(missing)}")


def get_supabase():
    require_env()
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def find_records_rows(
    supa,
    rec: ScrapedRecord,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Match seguro: scope + record_type + pool + category + gender + distance + stroke
    (evita pisar pseudo-records).
    """
    record_type = MAPEO_RECORD_TYPE.get(rec.record_type_code)
    if not record_type:
        return [], "no_record_type_map"

    q = supa.table("records_standards").select("*") \
        .eq("gender", rec.gender) \
        .eq("distance", rec.distance) \
        .eq("stroke", rec.stroke) \
        .eq("pool_length", rec.pool_length) \
        .eq("record_scope", rec.record_scope_db) \
        .eq("record_type", record_type)

    if rec.category:
        q = q.eq("category", rec.category)

    res = q.execute()
    return (res.data or []), "scope+record_type"


def update_record_row(supa, row_id: Any, updates: Dict[str, Any]) -> None:
    supa.table("records_standards").update(updates).eq("id", row_id).execute()


def insert_new_record_row(supa, rec: ScrapedRecord) -> None:
    record_type = MAPEO_RECORD_TYPE.get(rec.record_type_code) or ""
    payload = {
        "gender": rec.gender,
        "category": rec.category,
        "pool_length": rec.pool_length,
        "stroke": rec.stroke,
        "distance": rec.distance,
        "time_clock": ms_to_clock_2dp(rec.time_ms),
        "time_clock_2dp": ms_to_clock_2dp(rec.time_ms),
        "time_ms": rec.time_ms,
        "record_scope": rec.record_scope_db,
        "record_type": record_type,
        "athlete_name": rec.athlete_name,
        "last_updated": utc_now().date().isoformat(),
        "source_url": rec.source_url,
        "source_name": rec.source,
        "source_note": "scraped",
        "verified": True,
        "is_active": True,
    }
    supa.table("records_standards").insert(payload).execute()


# --------------- Scraper World Aquatics ---------------
def scrape_world_aquatics(page, record_type_code: str, piscina_web: str) -> List[ScrapedRecord]:
    pool_length = MAPEO_PISCINA.get(piscina_web)
    record_scope_db = MAPEO_SCOPE_DB.get(record_type_code)

    if not pool_length or not record_scope_db:
        return []

    url_wa = f"https://www.worldaquatics.com/swimming/records?recordType={record_type_code}&piscina={piscina_web}"
    safe_print(f"🔍 WA | {record_scope_db} | {pool_length} | URL: {url_wa}")

    try:
        page.goto(url_wa, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
        time.sleep(2)
    except PlaywrightTimeoutError:
        page.goto(url_wa, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        time.sleep(2)

    palabras_clave = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY"]
    processed_headers = set()
    results: List[ScrapedRecord] = []

    for clave in palabras_clave:
        items = page.get_by_text(clave).all()
        for item in items:
            try:
                card_text = item.locator("xpath=./..").inner_text()
                lines = [p.strip() for p in card_text.split("\n") if p and p.strip()]
                if not lines:
                    continue

                header = lines[0]
                if header in processed_headers:
                    continue
                processed_headers.add(header)

                gender = infer_gender(header)
                distance = extract_distance(header)
                stroke = extract_stroke_db(header)
                if not gender or not distance or not stroke:
                    continue

                time_raw = None
                athlete = None

                # Método 1 (compatible con tu script original)
                if len(lines) >= 4 and time_str_to_ms(lines[3]) is not None:
                    athlete = lines[2]
                    time_raw = lines[3]
                else:
                    # Método 2: detectar tiempo y usar la línea previa como atleta
                    time_raw = extract_first_time(lines)
                    if time_raw:
                        idx = lines.index(time_raw)
                        athlete = lines[idx - 1] if idx > 0 else None

                if not time_raw or not athlete:
                    continue

                ms = time_str_to_ms(time_raw)
                if ms is None:
                    continue

                results.append(
                    ScrapedRecord(
                        source="World Aquatics",
                        source_url=url_wa,
                        record_type_code=record_type_code,
                        record_scope_db=record_scope_db,
                        pool_length=pool_length,
                        category=DEFAULT_CATEGORY,
                        gender=gender,
                        distance=distance,
                        stroke=stroke,
                        athlete_name=athlete,
                        time_clock_raw=time_raw,
                        time_ms=ms,
                    )
                )
            except Exception:
                continue

    return results


# --------------- Email ---------------
def send_email_report(
    run_id: str,
    duration_s: float,
    updates: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    if not EMAIL_USER or not EMAIL_PASS:
        safe_print("⚠️ No se enviará mail: faltan EMAIL_USER/EMAIL_PASS en el entorno.")
        return

    msg = EmailMessage()
    subject = f"🏁 MDV Scraper WA | run {run_id[:8]} | {len(updates)} updates | {stats.get('errors', 0)} errors"
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO

    body = []
    body.append("Hola Coach,")
    body.append("")
    body.append(f"Run ID: {run_id}")
    body.append(f"Duración: {duration_s:.2f}s")
    body.append(
        f"Resumen: seen={stats.get('seen', 0)} | updated={stats.get('updated', 0)} | missing={stats.get('missing', 0)} | skipped={stats.get('skipped', 0)} | errors={stats.get('errors', 0)}"
    )
    body.append("")
    if updates:
        body.append("DETALLE DE ACTUALIZACIONES:")
        for u in updates[:200]:
            body.append(
                f"✅ [{u.get('scope')}] {u.get('prueba')}: {u.get('atleta')} {u.get('tiempo_anterior')} → {u.get('tiempo_nuevo')}"
            )
        if len(updates) > 200:
            body.append(f"... ({len(updates) - 200} más)")
    else:
        body.append("No hubo cambios hoy.")

    body.append("")
    body.append("Logs: scraper_logs (Supabase)")
    body.append("Atentamente,")
    body.append("Tu Ferrari de Récords 🏎️💨")

    msg.set_content("\n".join(body))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        safe_print("📧 Mail enviado con éxito.")
    except Exception as e:
        safe_print(f"❌ Error enviando mail: {compact_exc(e)}")


# --------------- Orquestación ---------------
def run() -> int:
    run_id = str(uuid.uuid4())
    started = utc_now()
    supa = get_supabase()
    slog = SupaLogger(supa, run_id)

    stats = {"seen": 0, "updated": 0, "missing": 0, "skipped": 0, "errors": 0}
    updates_for_email: List[Dict[str, Any]] = []

    # Log START
    try:
        slog.insert(
            {
                "level": "INFO",
                "event": "RUN_START",
                "message": "Inicio de ejecución",
                "payload": {"run_id": run_id, "started_at": started.isoformat(), "headless": HEADLESS},
                "scope": "RUN",
                "prueba": "START",
                "atleta": run_id,
                "tiempo_anterior": "",
                "tiempo_nuevo": "",
            }
        )
    except Exception:
        pass

    safe_print(f"🚀 RUN {run_id} | headless={HEADLESS} slowmo={SLOWMO_MS}ms")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
            page = browser.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)

            tareas = [
                ("WR", "50m"),
                ("WR", "25m"),
                ("OR", "50m"),
                ("PAN", "50m"),
                ("PAN", "25m"),
                ("SAM", "50m"),
                ("SAM", "25m"),
            ]

            for record_type_code, piscina_web in tareas:
                try:
                    scraped = scrape_world_aquatics(page, record_type_code, piscina_web)
                    stats["seen"] += len(scraped)

                    for rec in scraped:
                        try:
                            rows, strategy = find_records_rows(supa, rec)

                            if not rows:
                                stats["missing"] += 1
                                slog.insert(
                                    {
                                        "level": "WARN",
                                        "event": "MISSING_ROW",
                                        "message": f"No match en record
