#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MDV Records Updater (World Aquatics + Sudamérica + PanAm Games)

Qué hace
- World Aquatics (oficial): baja XLSX desde la web y hace UPSERT en Supabase.
- Sudamérica:
  1) Intenta CONSANAT (oficial) (HTML). Si falla por timeout/bloqueo, hace fallback a Wikipedia ES.
- Panamericano (récords de Juegos Panamericanos, NO "Continental Americas"):
  - Scrapea Wikipedia EN (Pan American Games records in swimming) (LCM).
- UPSERT en Supabase (records_standards):
  * actualiza tiempos si cambiaron
  * inserta récords faltantes
  * completa campos incompletos aunque el tiempo no cambie
- Registra un log editable en scraper_logs.
- Envía email resumen (opcional).

ENV requeridas:
- SUPABASE_URL
- SUPABASE_KEY (service_role o key con permisos de escritura)

Opcionales:
- EMAIL_USER / EMAIL_PASS
- MDV_UPDATER_VERSION (default: WA+CONS+PANAM_v10_WIKI_FALLBACK)
"""

from __future__ import annotations

import os
import re
import sys
import json
import uuid
import time
import shutil
import smtplib
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from playwright.sync_api import sync_playwright
from supabase import create_client

# (Opcional) .env para local; en GitHub Actions no hace falta
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ModuleNotFoundError:
    pass

# -------------------------- Config --------------------------

MDV_UPDATER_VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+CONS+PANAM_v10_WIKI_FALLBACK")
RUN_ID = os.getenv("GITHUB_RUN_ID") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASS = os.getenv("EMAIL_PASS", "").strip()

# Fuentes
CONS_NATACION_URL = "https://consanat.com/records/136/natacion"
WIKI_SAM_URL = "https://es.wikipedia.org/wiki/Anexo:Plusmarcas_de_Sudam%C3%A9rica_de_nataci%C3%B3n"
WIKI_PANAM_GAMES_URL = "https://en.wikipedia.org/wiki/List_of_Pan_American_Games_records_in_swimming"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Preferimos nombres en español, compatibles con tu tabla.
STROKE_MAP = {
    "freestyle": "Libre",
    "backstroke": "Espalda",
    "breaststroke": "Pecho",
    "butterfly": "Mariposa",
    "medley": "Combinado",
    "individual medley": "Combinado",

    "libre": "Libre",
    "espalda": "Espalda",
    "pecho": "Pecho",
    "mariposa": "Mariposa",
    "combinado": "Combinado",
    "medley": "Combinado",
    "im": "Combinado",
}

# -------------------------- Helpers: time/date --------------------------

def parse_time_to_ms(raw: str) -> Optional[int]:
    """Convierte varios formatos a ms.

    Admite:
    - "20.91" (seg.centésimas)
    - "1.41.32" (min.seg.cent)
    - "00:01:41.32" (h:m:s.cent)
    - "1:41.32" (m:s.cent)
    """
    if not raw:
        return None
    s = str(raw).strip()
    s = s.replace(",", ".")
    s = re.sub(r"\s+", "", s)

    if not s:
        return None

    # quitar caracteres extra típicos
    s = re.sub(r"[^0-9\.:]", "", s)

    # Formato con ':'
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 3:
                hh = int(parts[0]); mm = int(parts[1]); sec = float(parts[2])
            elif len(parts) == 2:
                hh = 0; mm = int(parts[0]); sec = float(parts[1])
            else:
                return None
            return int(round(((hh * 3600 + mm * 60) + sec) * 1000))
        except Exception:
            return None

    # Formato con '.' (seg.cent o min.seg.cent o h.min.seg.cent)
    dot_parts = s.split(".")
    try:
        if len(dot_parts) == 2:
            sec = float(s)
            return int(round(sec * 1000))
        if len(dot_parts) == 3:
            mm = int(dot_parts[0]); ss = int(dot_parts[1]); cc = int(dot_parts[2])
            return (mm * 60 + ss) * 1000 + int(round(cc * 10))
        if len(dot_parts) == 4:
            hh = int(dot_parts[0]); mm = int(dot_parts[1]); ss = int(dot_parts[2]); cc = int(dot_parts[3])
            return (hh * 3600 + mm * 60 + ss) * 1000 + int(round(cc * 10))
    except Exception:
        return None
    return None


def format_ms_to_hms(ms: int) -> str:
    """Formato estándar para tu tabla: HH:MM:SS.xx"""
    if ms < 0:
        ms = 0
    total_seconds = ms // 1000
    cent = (ms % 1000) // 10
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = total_seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{cent:02d}"


def parse_date(raw: str) -> Optional[str]:
    """Devuelve YYYY-MM-DD si puede."""
    if not raw:
        return None
    s = str(raw).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        if y < 100:
            y = 2000 + y
        try:
            return datetime(y, mo, d).date().isoformat()
        except Exception:
            return None

    # formatos tipo "August 7, 2015"
    try:
        from dateutil import parser as dateparser  # type: ignore
        dt = dateparser.parse(s, dayfirst=False, fuzzy=True)
        if dt:
            return dt.date().isoformat()
    except Exception:
        pass

    return None


# -------------------------- Helpers: event parsing --------------------------

# Soporta EN y ES básicos. (No relays todavía)
EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.IGNORECASE,
)

def parse_event(event_raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Devuelve (distance_m, stroke_es)"""
    if not event_raw:
        return None, None
    s = str(event_raw).strip().lower()
    s = s.replace("individual medley", "medley")

    m = EVENT_RE.search(s)
    if not m:
        return None, None

    dist = int(m.group("dist"))
    stroke_key = m.group("stroke").lower()
    stroke = STROKE_MAP.get(stroke_key)
    return dist, stroke


def gender_label(g: str) -> str:
    return "M" if str(g).upper().startswith("M") else "F"


def pool_label(pool: str) -> str:
    p = str(pool).upper()
    if p in ("LCM", "50M", "50", "L"):
        return "LCM"
    if p in ("SCM", "25M", "25", "S"):
        return "SCM"
    return p


# -------------------------- Supabase helpers --------------------------

class SB:
    def __init__(self, url: str, key: str):
        if not url or not key:
            raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")
        self.client = create_client(url, key)

    def upsert_record(self, payload: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Devuelve (status, db_row). status in: inserted|updated|filled|unchanged"""
        key_fields = [
            "record_scope",
            "record_type",
            "category",
            "pool_length",
            "gender",
            "stroke",
            "distance",
        ]
        for k in key_fields:
            if payload.get(k) is None:
                raise ValueError(f"payload missing {k}")

        q = self.client.table("records_standards").select("*")
        for k in key_fields:
            q = q.eq(k, payload[k])
        existing_resp = q.limit(1).execute()
        existing = (existing_resp.data or [None])[0]

        new_ms = parse_time_to_ms(payload.get("record_time", ""))

        if not existing:
            insert_payload = dict(payload)
            insert_payload["last_updated"] = RUN_TS
            resp = self.client.table("records_standards").insert(insert_payload).execute()
            row = (resp.data or [None])[0]
            return "inserted", row

        old_ms = parse_time_to_ms(existing.get("record_time") or "")
        time_changed = (new_ms is not None and old_ms is not None and new_ms != old_ms)

        # Campos que completamos aunque no cambie el tiempo
        fill_fields = [
            "athlete_name",
            "country",
            "record_date",
            "competition_name",
            "competition_city",
            "competition_country",
            "source_name",
            "source_url",
            "notes",
        ]
        updates: Dict[str, Any] = {}

        for f in fill_fields:
            newv = payload.get(f)
            oldv = existing.get(f)
            if newv is None or newv == "":
                continue
            if oldv is None or str(oldv).strip() == "":
                updates[f] = newv

        if time_changed:
            updates["record_time"] = payload.get("record_time")

        if updates:
            updates["last_updated"] = RUN_TS
            resp = (
                self.client.table("records_standards")
                .update(updates)
                .eq("id", existing["id"])
                .execute()
            )
            row = (resp.data or [None])[0]
            return ("updated" if time_changed else "filled"), row

        return "unchanged", existing

    def log(self, scope: str, prueba: str, atleta: str, t_old: str = "", t_new: str = "", message: str = "") -> None:
        """Inserta en scraper_logs (tolerante). No frena el run si falla."""
        base = {
            "fecha": RUN_TS,
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "tiempo_anterior": t_old,
            "tiempo_nuevo": t_new,
            "message": message,
            "run_id": RUN_ID,
            "version": MDV_UPDATER_VERSION,
        }
        try:
            self.client.table("scraper_logs").insert(base).execute()
        except Exception:
            # no matamos el run por logs
            return


# -------------------------- World Aquatics (XLSX) --------------------------

@dataclass
class WASpec:
    code: str  # WR, OR, WJ, CR_AMERICAS
    pool: str  # LCM/SCM
    gender: str  # M/F

def wa_url(spec: WASpec) -> str:
    base = "https://www.worldaquatics.com/swimming/records"
    if spec.code == "WR":
        return f"{base}?recordType=WR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "OR":
        return f"{base}?recordType=OR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "WJ":
        return f"{base}?recordCode=WJ&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "CR_AMERICAS":
        return f"{base}?recordType=PAN&recordCode=CR&eventTypeId=&region=AMERICAS&countryId=&gender={spec.gender}&pool={spec.pool}"
    raise ValueError(f"Unknown WA code {spec.code}")

def wa_specs() -> List[WASpec]:
    out: List[WASpec] = []
    for gender in ("M", "F"):
        for pool in ("LCM", "SCM"):
            out.append(WASpec("WR", pool, gender))
            out.append(WASpec("WJ", pool, gender))
            out.append(WASpec("CR_AMERICAS", pool, gender))
        out.append(WASpec("OR", "LCM", gender))
    return out

def wa_scope_and_type(code: str, pool: str) -> Tuple[str, str]:
    pool = pool_label(pool)
    is_scm = pool == "SCM"
    if code == "WR":
        return "Mundial", "Récord Mundial" + (" SC" if is_scm else "")
    if code == "OR":
        return "Olímpico", "Récord Olímpico"
    if code == "WJ":
        return "Mundial", "Récord Mundial Junior" + (" SC" if is_scm else "")
    if code == "CR_AMERICAS":
        return "Américas", "Récord Continental Américas" + (" SC" if is_scm else "")
    raise ValueError(code)

def wa_download_xlsx(page, url: str, out_dir: str) -> str:
    page.goto(url, wait_until="networkidle", timeout=120_000)

    try:
        page.get_by_role("button", name=re.compile(r"Accept Cookies", re.I)).click(timeout=2500)
    except Exception:
        pass

    with page.expect_download(timeout=120_000) as dl_info:
        try:
            page.get_by_role("link", name=re.compile(r"XLSX", re.I)).click(timeout=10_000)
        except Exception:
            page.get_by_role("link", name=re.compile(r"Download Records", re.I)).click(timeout=10_000)

    download = dl_info.value
    filename = download.suggested_filename or f"wa_{uuid.uuid4().hex}.xlsx"
    path = os.path.join(out_dir, filename)
    download.save_as(path)
    return path

def wa_parse_xlsx(xlsx_path: str) -> List[Dict[str, Any]]:
    wb = load_workbook(xlsx_path, data_only=True)
    rows_out: List[Dict[str, Any]] = []

    def norm(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        values = list(ws.values)
        if not values:
            continue

        header_idx = None
        header_map: Dict[str, int] = {}

        for i, row in enumerate(values[:60]):
            row_norm = [norm(x).lower() for x in row]
            if any("event" in c for c in row_norm) and any("time" in c for c in row_norm):
                header_idx = i
                for j, c in enumerate(row_norm):
                    if "event" in c:
                        header_map["event"] = j
                    elif "time" in c:
                        header_map["time"] = j
                    elif "athlete" in c or "swimmer" in c or "record holder" in c:
                        header_map["athlete"] = j
                    elif "country" in c or "nation" in c:
                        header_map["country"] = j
                    elif "date" in c:
                        header_map["date"] = j
                    elif "place" in c or "location" in c or "venue" in c:
                        header_map["location"] = j
                    elif "competition" in c or "meet" in c:
                        header_map["competition"] = j
                break

        if header_idx is None or "event" not in header_map or "time" not in header_map:
            continue

        for row in values[header_idx + 1 :]:
            event = norm(row[header_map["event"]])
            t = norm(row[header_map["time"]])
            if not event or not t:
                continue

            athlete = norm(row[header_map.get("athlete", -1)]) if "athlete" in header_map else ""
            country = norm(row[header_map.get("country", -1)]) if "country" in header_map else ""
            date = norm(row[header_map.get("date", -1)]) if "date" in header_map else ""
            location = norm(row[header_map.get("location", -1)]) if "location" in header_map else ""
            competition = norm(row[header_map.get("competition", -1)]) if "competition" in header_map else ""

            rows_out.append(
                {"event": event, "time": t, "athlete": athlete, "country": country, "date": date, "location": location, "competition": competition}
            )
    return rows_out

# -------------------------- Sudamérica: CONSANAT + fallback WIKI --------------------------

def fetch_with_retries(url: str, tries: int = 3, connect_timeout: int = 20, read_timeout: int = 60, tag: str = "FETCH") -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, timeout=(connect_timeout, read_timeout), headers=DEFAULT_HEADERS)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            wait_s = 3 * attempt
            print(f"⚠️ {tag} intento {attempt}/{tries} falló: {e} (reintento en {wait_s}s)")
            time.sleep(wait_s)
    raise last_err or RuntimeError(f"{tag} falló sin excepción? url={url}")

def consanat_fetch() -> str:
    return fetch_with_retries(CONS_NATACION_URL, tries=3, connect_timeout=20, read_timeout=60, tag="CONSANAT fetch")

def consanat_parse(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def find_idx(pattern: str) -> Optional[int]:
        for i, ln in enumerate(lines):
            if pattern.lower() in ln.lower():
                return i
        return None

    out: List[Dict[str, Any]] = []

    idx_scm = find_idx("RECORDS SUDAMERICANOS DE PISCINA CORTA")
    idx_lcm = find_idx("RECORDS SUDAMERICANOS DE PISCINA LARGA")

    sections: List[Tuple[str, int, int]] = []
    if idx_scm is not None:
        sections.append(("SCM", idx_scm, idx_lcm if idx_lcm is not None else len(lines)))
    if idx_lcm is not None:
        sections.append(("LCM", idx_lcm, len(lines)))

    for pool, start, end in sections:
        chunk = lines[start:end]
        for gender_word, gender in (("FEMININO", "F"), ("MASCULINO", "M")):
            try:
                g_start = next(i for i, ln in enumerate(chunk) if ln.upper() == gender_word)
            except StopIteration:
                continue
            g_end = next((i for i in range(g_start + 1, len(chunk)) if chunk[i].upper() in ("FEMININO", "MASCULINO")), len(chunk))
            g_lines = chunk[g_start:g_end]
            try:
                h = next(i for i, ln in enumerate(g_lines) if ln.upper() == "PRUEBAS")
            except StopIteration:
                continue
            data = g_lines[h:]
            try:
                header_end = next(i for i, ln in enumerate(data) if "COMPET" in ln.upper())
            except StopIteration:
                continue
            rows = data[header_end + 1 :]

            for i in range(0, len(rows) - 6, 7):
                event = rows[i]; t = rows[i + 1]; athlete = rows[i + 2]; country = rows[i + 3]; date = rows[i + 4]; location = rows[i + 5]; comp = rows[i + 6]
                dist, stroke = parse_event(event)
                ms = parse_time_to_ms(t)
                if dist is None or stroke is None or ms is None:
                    continue
                out.append({
                    "pool": pool, "gender": gender, "event": event, "distance": dist, "stroke": stroke, "time_ms": ms,
                    "athlete": athlete, "country": country, "date": parse_date(date) or date,
                    "city": location, "comp": comp, "source_name": "CONSANAT", "source_url": CONS_NATACION_URL
                })
    return out

def wiki_sam_parse(html: str) -> List[Dict[str, Any]]:
    """Parsea la página ES de Wikipedia (Plusmarcas Sudamérica). Muy estable para scraping."""
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    # En Wikipedia, las tablas suelen tener class="wikitable"
    tables = soup.select("table.wikitable")
    if not tables:
        return out

    # Heurística:
    # - Detectar contexto (LCM/SCM y género) por el heading previo (h2/h3)
    def detect_ctx(table) -> Tuple[str, str]:
        pool = ""
        gender = ""
        # busca encabezados cercanos
        heading = table.find_previous(["h2", "h3", "h4"])
        ctx = heading.get_text(" ", strip=True).lower() if heading else ""
        if "piscina larga" in ctx or "50" in ctx:
            pool = "LCM"
        if "piscina corta" in ctx or "25" in ctx:
            pool = "SCM"
        if "hombres" in ctx or "mascul" in ctx:
            gender = "M"
        if "mujeres" in ctx or "femen" in ctx:
            gender = "F"
        return pool, gender

    for t in tables:
        pool, gender = detect_ctx(t)
        # header
        header = [th.get_text(" ", strip=True).lower() for th in t.select("tr th")]
        if not header:
            continue

        # buscamos columnas típicas
        # (La estructura puede variar. Vamos robustos.)
        rows = t.select("tr")[1:]
        for tr in rows:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th","td"])]
            if len(cells) < 3:
                continue

            event = cells[0]
            # columnas comunes: Marca / Tiempo suele estar en 1
            t_raw = cells[1]
            athlete = cells[2] if len(cells) > 2 else ""
            country = cells[3] if len(cells) > 3 else ""
            date = cells[4] if len(cells) > 4 else ""
            location = cells[5] if len(cells) > 5 else ""
            competition = cells[6] if len(cells) > 6 else ""

            dist, stroke = parse_event(event)
            ms = parse_time_to_ms(t_raw)

            if dist is None or stroke is None or ms is None:
                continue

            out.append({
                "pool": pool or "LCM",
                "gender": gender or "M",
                "event": event,
                "distance": dist,
                "stroke": stroke,
                "time_ms": ms,
                "athlete": athlete,
                "country": country,
                "date": parse_date(date) or date,
                "city": location,
                "comp": competition,
                "source_name": "Wikipedia (Sudamérica)",
                "source_url": WIKI_SAM_URL,
            })

    return out

# -------------------------- PanAm Games (Wikipedia EN) --------------------------

def wiki_panam_games_fetch() -> str:
    return fetch_with_retries(WIKI_PANAM_GAMES_URL, tries=3, connect_timeout=20, read_timeout=60, tag="PANAM_GAMES(WIKI) fetch")

def wiki_panam_games_parse(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []
    tables = soup.select("table.wikitable")
    if not tables:
        return out

    # El artículo suele tener secciones "Men" y "Women" y tablas por stroke.
    # Detectamos género mirando el heading anterior.
    def ctx_gender(table) -> str:
        h = table.find_previous(["h2", "h3", "h4"])
        ctx = h.get_text(" ", strip=True).lower() if h else ""
        if "women" in ctx:
            return "F"
        if "men" in ctx:
            return "M"
        return "M"

    for t in tables:
        gender = ctx_gender(t)
        rows = t.select("tr")
        if len(rows) < 2:
            continue
        header_cells = [c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th","td"])]

        def col_idx(keys: Iterable[str]) -> Optional[int]:
            for k in keys:
                for i, c in enumerate(header_cells):
                    if k in c:
                        return i
            return None

        c_event = col_idx(["event", "distance"])  # a veces "event"
        c_time = col_idx(["time"])
        c_ath = col_idx(["athlete", "swimmer"])
        c_country = col_idx(["nation", "country", "noc"])
        c_date = col_idx(["date"])
        c_loc = col_idx(["venue", "location"])
        c_comp = col_idx(["games", "competition", "event"])  # no siempre

        # si no detectamos time/event, no es tabla de records
        if c_time is None or c_event is None:
            continue

        for tr in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
            if len(cells) <= max(c_time, c_event):
                continue
            event = cells[c_event]
            t_raw = cells[c_time]

            dist, stroke = parse_event(event)
            ms = parse_time_to_ms(t_raw)
            if dist is None or stroke is None or ms is None:
                continue

            athlete = cells[c_ath] if (c_ath is not None and c_ath < len(cells)) else ""
            country = cells[c_country] if (c_country is not None and c_country < len(cells)) else ""
            date = cells[c_date] if (c_date is not None and c_date < len(cells)) else ""
            loc = cells[c_loc] if (c_loc is not None and c_loc < len(cells)) else ""
            comp = cells[c_comp] if (c_comp is not None and c_comp < len(cells)) else "Pan American Games"

            out.append({
                "pool": "LCM",  # PanAm Games records en piscina larga
                "gender": gender,
                "event": event,
                "distance": dist,
                "stroke": stroke,
                "time_ms": ms,
                "athlete": athlete,
                "country": country,
                "date": parse_date(date) or date,
                "city": loc,
                "comp": comp,
                "source_name": "Wikipedia (PanAm Games)",
                "source_url": WIKI_PANAM_GAMES_URL,
            })
    return out

# -------------------------- Build payloads --------------------------

def split_location(loc: str) -> Tuple[str, str]:
    """Intenta partir 'City, Country' -> (city, country)."""
    if not loc:
        return "", ""
    s = str(loc).strip()
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return s, ""

def build_payload(
    record_scope: str,
    record_type: str,
    pool: str,
    gender: str,
    distance: int,
    stroke: str,
    time_ms: int,
    athlete: str,
    country: str,
    record_date: str,
    comp_name: str,
    comp_location: str,
    source_name: str,
    source_url: str,
) -> Dict[str, Any]:
    city, comp_country = split_location(comp_location)
    # si country del atleta viene vacío, intentamos usar el del lugar (no siempre coincide)
    return {
        "record_scope": record_scope,
        "record_type": record_type,
        "category": "Open",
        "pool_length": pool_label(pool),
        "gender": gender_label(gender),
        "distance": int(distance),
        "stroke": stroke,
        "record_time": format_ms_to_hms(int(time_ms)),
        "athlete_name": athlete or "",
        "country": country or "",
        "record_date": record_date or "",
        "competition_name": comp_name or "",
        "competition_city": city or "",
        "competition_country": comp_country or "",
        "source_name": source_name or "",
        "source_url": source_url or "",
        "notes": "",
    }

# -------------------------- Runs --------------------------

def run_wa(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}
    tmp_dir = f"/tmp/mdv_wa_{RUN_ID}"
    os.makedirs(tmp_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for spec in wa_specs():
            try:
                url = wa_url(spec)
                print(f"🔎 WA | {spec.code} | {spec.pool} | {spec.gender} | {url}")

                xlsx_path = wa_download_xlsx(page, url, tmp_dir)
                rows = wa_parse_xlsx(xlsx_path)

                record_scope, record_type = wa_scope_and_type(spec.code, spec.pool)

                for r in rows:
                    stats["seen"] += 1
                    dist, stroke = parse_event(r.get("event", ""))
                    ms = parse_time_to_ms(r.get("time", ""))
                    if dist is None or stroke is None or ms is None:
                        stats["skipped"] += 1
                        continue

                    payload = build_payload(
                        record_scope=record_scope,
                        record_type=record_type,
                        pool=spec.pool,
                        gender=spec.gender,
                        distance=dist,
                        stroke=stroke,
                        time_ms=ms,
                        athlete=r.get("athlete", ""),
                        country=r.get("country", ""),
                        record_date=parse_date(r.get("date", "")) or r.get("date", ""),
                        comp_name=r.get("competition", ""),
                        comp_location=r.get("location", ""),
                        source_name="World Aquatics",
                        source_url=url,
                    )

                    status, _row = sb.upsert_record(payload)
                    if status == "inserted":
                        stats["inserted"] += 1
                    elif status == "updated":
                        stats["updated"] += 1
                    elif status == "filled":
                        stats["filled"] += 1
                    else:
                        stats["unchanged"] += 1

            except Exception as e:
                stats["errors"] += 1
                msg = f"WA {spec.code} {spec.pool} {spec.gender} error: {e}"
                print("❌", msg)
                sb.log("ERROR", f"WA {spec.code} {spec.pool} {spec.gender}", "", message=msg + "\n" + traceback.format_exc())
                continue

        browser.close()

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return stats

def run_sudamerica(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    rows: List[Dict[str, Any]] = []
    source_used = ""

    # 1) CONSANAT
    try:
        html = consanat_fetch()
        rows = consanat_parse(html)
        source_used = "CONSANAT"
        if not rows:
            print("⚠️ CONSANAT respondió pero parse=0; activo fallback Wikipedia.")
            raise RuntimeError("CONSANAT parse=0")
    except Exception as e:
        stats["errors"] += 1
        sb.log("ERROR", "CONSANAT", "", message=str(e) + "\n" + traceback.format_exc())

        # 2) Fallback Wikipedia
        try:
            html = fetch_with_retries(WIKI_SAM_URL, tries=3, connect_timeout=20, read_timeout=60, tag="WIKI_SAM fetch")
            rows = wiki_sam_parse(html)
            source_used = "WIKI_SAM"
            if not rows:
                raise RuntimeError("Wikipedia Sudamérica parse=0")
        except Exception as e2:
            stats["errors"] += 1
            sb.log("ERROR", "WIKI_SAM", "", message=str(e2) + "\n" + traceback.format_exc())
            return stats

    for r in rows:
        stats["seen"] += 1
        record_scope = "Sudamericano"
        record_type = "Récord Sudamericano" + (" SC" if r["pool"] == "SCM" else "")

        payload = build_payload(
            record_scope=record_scope,
            record_type=record_type,
            pool=r["pool"],
            gender=r["gender"],
            distance=r["distance"],
            stroke=r["stroke"],
            time_ms=r["time_ms"],
            athlete=r.get("athlete", ""),
            country=r.get("country", ""),
            record_date=r.get("date", ""),
            comp_name=r.get("comp", ""),
            comp_location=r.get("city", ""),
            source_name=("CONSANAT" if source_used == "CONSANAT" else r.get("source_name", "Wikipedia (Sudamérica)")),
            source_url=(CONS_NATACION_URL if source_used == "CONSANAT" else r.get("source_url", WIKI_SAM_URL)),
        )

        try:
            status, _ = sb.upsert_record(payload)
            if status == "inserted":
                stats["inserted"] += 1
            elif status == "updated":
                stats["updated"] += 1
            elif status == "filled":
                stats["filled"] += 1
            else:
                stats["unchanged"] += 1
        except Exception as e:
            stats["errors"] += 1
            sb.log("ERROR", "UPSERT_SUDAM", payload.get("athlete_name",""), message=str(e) + "\n" + traceback.format_exc())

    return stats

def run_panam_games(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    try:
        html = wiki_panam_games_fetch()
        rows = wiki_panam_games_parse(html)
        if not rows:
            raise RuntimeError("PanAm Games (Wikipedia) parse=0")
    except Exception as e:
        stats["errors"] += 1
        sb.log("ERROR", "PANAM_GAMES_WIKI", "", message=str(e) + "\n" + traceback.format_exc())
        return stats

    for r in rows:
        stats["seen"] += 1
        record_scope = "Panamericano"
        record_type = "Récord Juegos Panamericanos"  # distinto de Continental Américas

        payload = build_payload(
            record_scope=record_scope,
            record_type=record_type,
            pool=r["pool"],
            gender=r["gender"],
            distance=r["distance"],
            stroke=r["stroke"],
            time_ms=r["time_ms"],
            athlete=r.get("athlete", ""),
            country=r.get("country", ""),
            record_date=r.get("date", ""),
            comp_name=r.get("comp", "Pan American Games"),
            comp_location=r.get("city", ""),
            source_name=r.get("source_name", "Wikipedia (PanAm Games)"),
            source_url=r.get("source_url", WIKI_PANAM_GAMES_URL),
        )

        try:
            status, _ = sb.upsert_record(payload)
            if status == "inserted":
                stats["inserted"] += 1
            elif status == "updated":
                stats["updated"] += 1
            elif status == "filled":
                stats["filled"] += 1
            else:
                stats["unchanged"] += 1
        except Exception as e:
            stats["errors"] += 1
            sb.log("ERROR", "UPSERT_PANAM_GAMES", payload.get("athlete_name",""), message=str(e) + "\n" + traceback.format_exc())

    return stats

# -------------------------- Email --------------------------

def send_email(subject: str, body: str) -> None:
    if not EMAIL_USER or not EMAIL_PASS:
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_USER
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(EMAIL_USER, EMAIL_PASS)
        s.send_message(msg)

# -------------------------- Main --------------------------

def main() -> int:
    try:
        sb = SB(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase init error: {e}")
        return 2

    print(f"MDV_UPDATER_VERSION={MDV_UPDATER_VERSION}")
    print(f"RUN_ID={RUN_ID}")

    all_stats: Dict[str, Dict[str, int]] = {}

    all_stats["WA"] = run_wa(sb)
    all_stats["SUDAM"] = run_sudamerica(sb)
    all_stats["PANAM_GAMES"] = run_panam_games(sb)

    lines = [
        f"Version: {MDV_UPDATER_VERSION}",
        f"Run ID: {RUN_ID}",
        f"Timestamp (UTC): {RUN_TS}",
        "",
    ]
    for k, st in all_stats.items():
        lines.append(
            f"[{k}] seen={st['seen']} | inserted={st['inserted']} | updated={st['updated']} | filled={st['filled']} | unchanged={st['unchanged']} | skipped={st['skipped']} | errors={st['errors']}"
        )
    body = "\n".join(lines)
    print(body)

    try:
        sb.log("RUN", "SUMMARY", json.dumps(all_stats, ensure_ascii=False), message=body)
    except Exception:
        pass

    send_email(f"🏁 MDV Scraper | {RUN_ID} | {MDV_UPDATER_VERSION}", body)

    total_errors = sum(st["errors"] for st in all_stats.values())
    # Opcional: si querés que el workflow NO falle cuando solo fallan fuentes secundarias,
    # podés cambiar esta línea a: return 0
    return 1 if total_errors else 0

if __name__ == "__main__":
    sys.exit(main())
