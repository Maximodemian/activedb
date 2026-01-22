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
# MDV Records Updater (World Aquatics) - SAFE VERSION
# Version: WA_V2_2026-01-22a
# ============================================================

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")  # Google App Password (16)

EMAIL_TO = os.environ.get("EMAIL_TO", "vorrabermauro@gmail.com")

HEADLESS = os.environ.get("HEADLESS", "true").strip().lower() not in ("0", "false", "no")
SLOWMO_MS = int(os.environ.get("SLOWMO_MS", "0") or "0")
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "60000") or "60000")

ALLOW_INSERT_NEW = os.environ.get("ALLOW_INSERT_NEW", "false").strip().lower() in ("1", "true", "yes")
DEFAULT_CATEGORY = os.environ.get("DEFAULT_CATEGORY", "Open")

# World Aquatics
WA_BASE = "https://www.worldaquatics.com/swimming/records"
MAPEO_PISCINA = {"50m": "LCM", "25m": "SCM"}

TRADUCCION_ESTILOS = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "MEDLEY": "Combinado",
    "IM": "Combinado",
}

def safe_print(msg: str) -> None:
    print(msg, flush=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def compact_exc(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"

_TIME_RE = re.compile(
    r"^\s*(\d{1,2}:)?\d{1,2}:\d{2}([.,]\d{1,2})?\s*$|^\s*\d{1,3}([.,]\d{1,2})\s*$"
)

def time_str_to_ms(t_str: str) -> Optional[int]:
    """
    Acepta:
      - 1:52.34
      - 52.34
      - 00:01:52.34
      - 46,30
      - 1.54.50 (PDF) -> 1:54.50
    """
    if not t_str:
        return None
    s = str(t_str).strip()
    if not s:
        return None

    s = s.replace(",", ".")

    # PDF style: "1.54.50" => "1:54.50"
    if ":" not in s and s.count(".") == 2:
        a, b, c = s.split(".")
        if a.isdigit() and b.isdigit() and c.isdigit() and len(b) == 2:
            s = f"{int(a)}:{int(b):02d}.{int(c):02d}"

    if not _TIME_RE.match(s):
        return None

    parts = s.split(":")
    try:
        if len(parts) == 3:
            hh = int(parts[0]); mm = int(parts[1]); sec_part = parts[2]
        elif len(parts) == 2:
            hh = 0; mm = int(parts[0]); sec_part = parts[1]
        else:
            hh = 0; mm = 0; sec_part = parts[0]

        if "." in sec_part:
            ss_str, cc_str = sec_part.split(".", 1)
            ss = int(ss_str)
            cent = int(cc_str[:2]) if len(cc_str) >= 2 else int(cc_str) * 10
        else:
            ss = int(sec_part)
            cent = 0

        return (hh * 3600 + mm * 60 + ss) * 1000 + cent * 10
    except Exception:
        return None

def ms_to_clock_2dp(ms: int) -> str:
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

def infer_gender(header: str) -> Optional[str]:
    h = (header or "").upper()
    if "WOMEN" in h or "WOMAN" in h:
        return "F"
    if "MEN" in h or "MAN" in h:
        return "M"
    return None

def extract_distance(header: str) -> Optional[int]:
    m = re.search(r"(\d{2,4})\s*M", (header or "").upper())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def extract_stroke_db(header: str) -> Optional[str]:
    h = (header or "").upper()
    for k, v in TRADUCCION_ESTILOS.items():
        if k in h:
            return v
    return None

def extract_first_time(lines: List[str]) -> Optional[str]:
    for ln in lines:
        if time_str_to_ms(ln) is not None:
            return ln.strip()
    return None

def get_record_type_db(code: str, pool_length: str) -> Optional[str]:
    # Ajustado a tu BD: WR SCM => "Récord Mundial SC"
    if code == "WR":
        return "Récord Mundial SC" if pool_length == "SCM" else "Récord Mundial"
    if code == "OR":
        return "Récord Olímpico"
    if code == "PAN":
        return "Récord Panamericano"
    if code == "SAM":
        return "Récord Sudamericano"
    return None

def get_scope_candidates(code: str) -> List[str]:
    # Ajuste por inconsistencias históricas en tu tabla: OR puede estar bajo "Olímpico" o "Mundial"
    if code == "WR":
        return ["Mundial"]
    if code == "OR":
        return ["Olímpico", "Mundial"]
    if code == "PAN":
        return ["Panamericano"]
    if code == "SAM":
        return ["Sudamericano"]
    return []

@dataclass
class ScrapedRecord:
    source: str
    source_url: str
    record_type_code: str
    pool_length: str
    category: str
    gender: str
    distance: int
    stroke: str
    athlete_name: str
    time_clock_raw: str
    time_ms: int

class SupaLogger:
    """
    Inserta logs en scraper_logs intentando:
    (1) formato extendido: run_id, level, event, message, payload, created_at
    (2) formato legacy: scope, prueba, atleta, tiempo_anterior, tiempo_nuevo
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

        legacy = {
            "scope": data.get("scope", ""),
            "prueba": data.get("prueba", ""),
            "atleta": data.get("atleta", ""),
            "tiempo_anterior": data.get("tiempo_anterior", ""),
            "tiempo_nuevo": data.get("tiempo_nuevo", ""),
        }
        self._try_insert(legacy)

    def _try_insert(self, row: Dict[str, Any]) -> bool:
        try:
            self.supa.table("scraper_logs").insert(row).execute()
            return True
        except Exception:
            return False

def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if missing:
        raise RuntimeError("Faltan variables: " + ", ".join(missing))

def get_supabase():
    require_env()
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def wa_url(record_type: str, piscina: str) -> str:
    return f"{WA_BASE}?recordType={record_type}&piscina={piscina}"

def scrape_world_aquatics(page, record_type_code: str, piscina_web: str) -> List[ScrapedRecord]:
    pool_length = MAPEO_PISCINA.get(piscina_web)
    if not pool_length:
        return []

    url = wa_url(record_type_code, piscina_web)
    safe_print(f"🔍 WA | {record_type_code} | {pool_length} | {url}")

    try:
        page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
        time.sleep(1.5)
    except PlaywrightTimeoutError:
        page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        time.sleep(1.5)

    keywords = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY"]
    processed_headers = set()
    out: List[ScrapedRecord] = []

    for kw in keywords:
        items = page.get_by_text(kw).all()
        for item in items:
            try:
                card_text = item.locator("xpath=./..").inner_text()
                lines = [x.strip() for x in card_text.split("\n") if x and x.strip()]
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

                # Método 1 (como tu versión original)
                if len(lines) >= 4 and time_str_to_ms(lines[3]) is not None:
                    athlete = lines[2]
                    time_raw = lines[3]
                else:
                    # Método 2: buscar el primer "time-like" y usar la línea previa como atleta
                    time_raw = extract_first_time(lines)
                    if time_raw:
                        idx = lines.index(time_raw)
                        athlete = lines[idx - 1] if idx > 0 else None

                if not time_raw or not athlete:
                    continue

                ms = time_str_to_ms(time_raw)
                if ms is None:
                    continue

                out.append(ScrapedRecord(
                    source="World Aquatics",
                    source_url=url,
                    record_type_code=record_type_code,
                    pool_length=pool_length,
                    category=DEFAULT_CATEGORY,
                    gender=gender,
                    distance=distance,
                    stroke=stroke,
                    athlete_name=athlete,
                    time_clock_raw=time_raw,
                    time_ms=ms,
                ))
            except Exception:
                continue

    return out

def find_records_rows(supa, rec: ScrapedRecord) -> Tuple[List[Dict[str, Any]], str]:
    record_type = get_record_type_db(rec.record_type_code, rec.pool_length)
    scopes = get_scope_candidates(rec.record_type_code)
    if not record_type or not scopes:
        return [], "no_map"

    for scope in scopes:
        q = (
            supa.table("records_standards")
            .select("*")
            .eq("gender", rec.gender)
            .eq("distance", rec.distance)
            .eq("stroke", rec.stroke)
            .eq("pool_length", rec.pool_length)
            .eq("record_type", record_type)
            .eq("record_scope", scope)
            .eq("category", rec.category)
        )
        res = q.execute()
        rows = res.data or []
        if rows:
            return rows, f"scope={scope}"
    return [], "no_match"

def update_record_row(supa, row_id: Any, updates: Dict[str, Any]) -> None:
    supa.table("records_standards").update(updates).eq("id", row_id).execute()

def insert_new_record_row(supa, rec: ScrapedRecord, scope_for_insert: str) -> None:
    record_type = get_record_type_db(rec.record_type_code, rec.pool_length) or ""
    payload = {
        "gender": rec.gender,
        "category": rec.category,
        "pool_length": rec.pool_length,
        "stroke": rec.stroke,
        "distance": rec.distance,
        "time_clock": ms_to_clock_2dp(rec.time_ms),
        "time_clock_2dp": ms_to_clock_2dp(rec.time_ms),
        "time_ms": rec.time_ms,
        "record_scope": scope_for_insert,
        "record_type": record_type,
        "athlete_name": rec.athlete_name,
        "last_updated": utc_now().date().isoformat(),
        "source_name": rec.source,
        "source_url": rec.source_url,
        "source_note": "scraped",
        "verified": True,
        "is_active": True,
    }
    supa.table("records_standards").insert(payload).execute()

def send_email_report(run_id: str, duration_s: float, updates: List[Dict[str, Any]], stats: Dict[str, Any]) -> None:
    if not EMAIL_USER or not EMAIL_PASS:
        safe_print("⚠️ No se enviará mail: faltan EMAIL_USER/EMAIL_PASS.")
        return

    msg = EmailMessage()
    msg["Subject"] = f"🏁 MDV Scraper WA | run {run_id[:8]} | {len(updates)} updates | {stats.get('errors', 0)} errors"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO

    lines = []
    lines.append(f"Version: WA_V2_2026-01-22a")
    lines.append(f"Run ID: {run_id}")
    lines.append(f"Duración: {duration_s:.2f}s")
    lines.append(
        f"seen={stats.get('seen', 0)} | updated={stats.get('updated', 0)} | missing={stats.get('missing', 0)} | skipped={stats.get('skipped', 0)} | errors={stats.get('errors', 0)}"
    )
    lines.append("")
    if updates:
        lines.append("ACTUALIZACIONES:")
        for u in updates[:200]:
            lines.append(f"✅ [{u.get('scope')}] {u.get('prueba')}: {u.get('atleta')} {u.get('tiempo_anterior')} → {u.get('tiempo_nuevo')}")
        if len(updates) > 200:
            lines.append(f"... ({len(updates) - 200} más)")
    else:
        lines.append("No hubo cambios hoy.")

    msg.set_content("\n".join(lines))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        safe_print("📧 Mail enviado con éxito.")
    except Exception as e:
        safe_print(f"❌ Error enviando mail: {compact_exc(e)}")

def run() -> int:
    safe_print("MDV_UPDATER_VERSION=WA_V2_2026-01-22a")

    run_id = str(uuid.uuid4())
    started = utc_now()

    supa = get_supabase()
    slog = SupaLogger(supa, run_id)

    stats = {"seen": 0, "updated": 0, "missing": 0, "skipped": 0, "errors": 0}
    updates_for_email: List[Dict[str, Any]] = []

    slog.insert({
        "level": "INFO",
        "event": "RUN_START",
        "message": "Inicio ejecución",
        "payload": {"run_id": run_id, "started_at": started.isoformat()},
        "scope": "RUN",
        "prueba": "START",
        "atleta": run_id,
    })

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

            for code, piscina in tareas:
                scraped = scrape_world_aquatics(page, code, piscina)
                stats["seen"] += len(scraped)

                for rec in scraped:
                    try:
                        rows, strategy = find_records_rows(supa, rec)

                        if not rows:
                            stats["missing"] += 1

                            # IMPORTANTE: mensaje en una sola línea (sin cortes)
                            msg = f"No match en records_standards ({strategy})"

                            slog.insert({
                                "level": "WARN",
                                "event": "MISSING_ROW",
                                "message": msg,
                                "payload": asdict(rec),
                                "scope": ",".join(get_scope_candidates(code)),
                                "prueba": f"{rec.gender} {rec.distance}m {rec.stroke} ({rec.pool_length})",
                                "atleta": rec.athlete_name,
                            })

                            if ALLOW_INSERT_NEW:
                                try:
                                    insert_scope = get_scope_candidates(code)[0]
                                    insert_new_record_row(supa, rec, insert_scope)
                                    slog.insert({
                                        "level": "INFO",
                                        "event": "INSERT_NEW",
                                        "message": "Insert nuevo OK",
                                        "payload": asdict(rec),
                                        "scope": insert_scope,
                                        "prueba": f"{rec.gender} {rec.distance}m {rec.stroke} ({rec.pool_length})",
                                        "atleta": rec.athlete_name,
                                        "tiempo_anterior": "",
                                        "tiempo_nuevo": ms_to_clock_2dp(rec.time_ms),
                                    })
                                except Exception as e:
                                    stats["errors"] += 1
                                    slog.insert({
                                        "level": "ERROR",
                                        "event": "INSERT_FAILED",
                                        "message": compact_exc(e),
                                        "payload": {"trace": traceback.format_exc()[:4000]},
                                    })
                            continue

                        for row in rows:
                            db_ms = row.get("time_ms")
                            try:
                                db_ms = int(db_ms) if db_ms is not None else None
                            except Exception:
                                db_ms = None

                            if db_ms is not None and rec.time_ms >= db_ms:
                                stats["skipped"] += 1
                                continue

                            old_clock = row.get("time_clock") or row.get("time_clock_2dp") or ""
                            new_clock = ms_to_clock_2dp(rec.time_ms)
                            scope_used = row.get("record_scope") or ",".join(get_scope_candidates(code))

                            update_record_row(supa, row["id"], {
                                "athlete_name": rec.athlete_name,
                                "time_ms": rec.time_ms,
                                "time_clock": new_clock,
                                "time_clock_2dp": new_clock,
                                "last_updated": utc_now().date().isoformat(),
                                "source_name": rec.source,
                                "source_url": rec.source_url,
                                "source_note": "scraped",
                                "verified": True,
                            })

                            log_data = {
                                "scope": scope_used,
                                "prueba": f"{rec.gender} {rec.distance}m {rec.stroke} ({rec.pool_length})",
                                "atleta": rec.athlete_name,
                                "tiempo_anterior": old_clock,
                                "tiempo_nuevo": new_clock,
                            }
                            updates_for_email.append(log_data)
                            stats["updated"] += 1

                            slog.insert({
                                "level": "INFO",
                                "event": "UPDATED",
                                "message": f"Updated ({strategy})",
                                "payload": {"db_id": row.get("id"), "strategy": strategy},
                                **log_data,
                            })

                    except Exception as e:
                        stats["errors"] += 1
                        slog.insert({
                            "level": "ERROR",
                            "event": "RECORD_ERROR",
                            "message": compact_exc(e),
                            "payload": {"trace": traceback.format_exc()[:4000]},
                        })

            browser.close()

    except Exception as e:
        stats["errors"] += 1
        slog.insert({
            "level": "ERROR",
            "event": "FATAL",
            "message": compact_exc(e),
            "payload": {"trace": traceback.format_exc()[:4000]},
            "scope": "RUN",
            "prueba": "FATAL",
            "atleta": run_id,
        })
        safe_print(f"💥 FATAL: {compact_exc(e)}")
        return 2

    ended = utc_now()
    duration_s = (ended - started).total_seconds()

    slog.insert({
        "level": "INFO",
        "event": "RUN_END",
        "message": "Fin ejecución",
        "payload": {"run_id": run_id, "duration_s": duration_s, "stats": stats},
        "scope": "RUN",
        "prueba": "END",
        "atleta": run_id,
    })

    safe_print(f"✅ DONE | run={run_id[:8]} | dur={duration_s:.2f}s | {json.dumps(stats, ensure_ascii=False)}")
    send_email_report(run_id, duration_s, updates_for_email, stats)
    return 0

if __name__ == "__main__":
    raise SystemExit(run())
