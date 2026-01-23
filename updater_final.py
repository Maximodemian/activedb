#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MDV Records Updater v11 (WA + Sudamérica + Juegos Panamericanos + DataHub opcional)

Qué cambia vs v10
- Detecta automáticamente columnas reales de la tabla (evita errores tipo "Could not find column ...").
- Sudamérica: intenta CONSANAT; si no responde, usa Wikipedia ES (HTML estable).
- Juegos Panamericanos: usa Wikipedia EN (tablas estables).
- Normaliza país a ISO3 (BRA/ARG/USA...) cuando viene como nombre (Brasil/Argentina/United States).
- Log de errores “hablable”: imprime el motivo real (PostgREST / constraint / columna inexistente) y lo guarda en scraper_logs.
- Data Hub (USA Swimming) opcional: se intenta por Playwright (download/intercept). Si falla, NO rompe el run.

Variables de entorno requeridas
- SUPABASE_URL
- SUPABASE_KEY  (service_role o key con permisos de escritura)

Opcionales
- MDV_UPDATER_VERSION
- EMAIL_USER / EMAIL_PASS
- MDV_DEBUG=1  (más detalle en stdout)

Notas
- WA usa Playwright para descargar XLSX oficial.
- Si CONSANAT timeoutea (común en runners), Wikipedia cubre el gap.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import uuid
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

# (Opcional) Carga variables desde .env cuando corrés local.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ModuleNotFoundError:
    pass

try:
    import pycountry  # type: ignore
except ModuleNotFoundError:
    pycountry = None  # type: ignore


# -------------------------- Config --------------------------

MDV_UPDATER_VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+SUDAM+PANAMG+HUB_v11_SCHEMA_DETECT")
RUN_ID = os.getenv("GITHUB_RUN_ID") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

DEBUG = os.getenv("MDV_DEBUG", "").strip() in ("1", "true", "TRUE", "yes", "YES")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASS = os.getenv("EMAIL_PASS", "").strip()

# Fuentes
WA_BASE = "https://www.worldaquatics.com/swimming/records"
CONS_NATACION_URL = "https://consanat.com/records/136/natacion"
WIKI_SUDAM_URL = "https://es.wikipedia.org/wiki/Anexo:Plusmarcas_de_Sudam%C3%A9rica_de_nataci%C3%B3n"
WIKI_PANAM_GAMES_URL = "https://en.wikipedia.org/wiki/List_of_Pan_American_Games_records_in_swimming"
USA_DATAHUB_URL = "https://data.usaswimming.org/datahub/continentalrecordlists"

# -------------------------- Helpers: stdout --------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"⚠️ {msg}", flush=True)

def err(msg: str) -> None:
    print(f"❌ {msg}", flush=True)

# -------------------------- Helpers: time/date --------------------------

def parse_time_to_ms(raw: str) -> Optional[int]:
    """Convierte varios formatos a ms.
    Admite: "20.91", "1.41.32", "00:01:41.32", "1:41.32"
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"\s+", "", s)

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


def format_ms_to_clock(ms: int) -> str:
    """Formato estándar HH:MM:SS.xx"""
    if ms < 0:
        ms = 0
    total_seconds = ms // 1000
    cent = (ms % 1000) // 10
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = total_seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{cent:02d}"


def parse_date(raw: str) -> str:
    """Devuelve YYYY-MM-DD si puede; sino devuelve el string limpio."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$", s)
    if m:
        d = int(m.group(1)); mo = int(m.group(2)); y = int(m.group(3))
        if y < 100:
            y = 2000 + y
        try:
            return datetime(y, mo, d).date().isoformat()
        except Exception:
            return s
    return s


# -------------------------- Helpers: event parsing --------------------------

STROKE_MAP = {
    "freestyle": "Libre",
    "backstroke": "Espalda",
    "breaststroke": "Pecho",
    "butterfly": "Mariposa",
    "medley": "Combinado",
    "individual medley": "Combinado",
    "im": "Combinado",
    # ES
    "libre": "Libre",
    "espalda": "Espalda",
    "pecho": "Pecho",
    "mariposa": "Mariposa",
    "combinado": "Combinado",
}

EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*(?:m|mts|metros)?\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|individual medley|im|libre|espalda|pecho|mariposa|combinado)",
    re.IGNORECASE,
)

RELAY_RE = re.compile(
    r"(?P<legs>\d)\s*[x×]\s*(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.IGNORECASE,
)

def parse_event(event_raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Devuelve (distance_m, stroke_es). En relevos, devuelve distancia del tramo (ej 4x100 -> 100)."""
    if event_raw is None:
        return None, None
    s = str(event_raw).strip()
    if not s:
        return None, None

    m = RELAY_RE.search(s)
    if m:
        dist = int(m.group("dist"))
        stroke_key = m.group("stroke").lower()
        return dist, STROKE_MAP.get(stroke_key, None)

    m = EVENT_RE.search(s)
    if not m:
        return None, None
    dist = int(m.group("dist"))
    stroke_key = m.group("stroke").lower()
    return dist, STROKE_MAP.get(stroke_key, None)


def gender_label(g: str) -> str:
    if not g:
        return ""
    g2 = str(g).strip().upper()
    if g2.startswith("M") or g2.startswith("H"):
        return "M"
    if g2.startswith("F") or g2.startswith("W") or g2.startswith("D"):
        return "F"
    return g2[:1]


def pool_label(pool: str) -> str:
    p = str(pool or "").upper().strip()
    if p in ("LCM", "50M", "50", "L", "LONG", "LONG COURSE"):
        return "LCM"
    if p in ("SCM", "25M", "25", "S", "SHORT", "SHORT COURSE"):
        return "SCM"
    return p


# -------------------------- Helpers: ISO3 country --------------------------

# Fallback mínimo (si pycountry no está)
ISO3_FALLBACK = {
    "argentina": "ARG",
    "brasil": "BRA",
    "brazil": "BRA",
    "uruguay": "URU",
    "chile": "CHI",
    "paraguay": "PAR",
    "perú": "PER",
    "peru": "PER",
    "colombia": "COL",
    "venezuela": "VEN",
    "ecuador": "ECU",
    "bolivia": "BOL",
    "méxico": "MEX",
    "mexico": "MEX",
    "estados unidos": "USA",
    "united states": "USA",
    "canadá": "CAN",
    "canada": "CAN",
    "cuba": "CUB",
    "república dominicana": "DOM",
    "dominican republic": "DOM",
    "puerto rico": "PUR",
    "panamá": "PAN",
    "panama": "PAN",
}

def to_iso3(raw: str) -> str:
    """Normaliza a ISO3 cuando es posible.
    - Si ya es 3 letras (BRA), devuelve tal cual.
    - Si viene como nombre, intenta pycountry y luego fallback.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    # si viene con banderita o cosas raras, limpialo
    s = re.sub(r"\[.*?\]", "", s).strip()
    s = re.sub(r"\(.*?\)", "", s).strip()

    if re.fullmatch(r"[A-Za-z]{3}", s):
        return s.upper()

    key = s.lower().strip()
    key = key.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ü","u").replace("ñ","n")
    if key in ISO3_FALLBACK:
        return ISO3_FALLBACK[key]

    if pycountry is not None:
        try:
            # lookup es bastante flexible
            c = pycountry.countries.lookup(s)
            return getattr(c, "alpha_3", "") or ""
        except Exception:
            pass

    # último intento: si viene "City, Country"
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if parts:
            return to_iso3(parts[-1])

    return ""


# -------------------------- Supabase helpers --------------------------

class SB:
    """Wrapper Supabase + auto-detección de columnas."""
    def __init__(self, url: str, key: str):
        if not url or not key:
            raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")
        self.client = create_client(url, key)
        self.columns = self._detect_columns()
        log(f"🧬 DB columns detectadas: {len(self.columns)}")

    def _detect_columns(self) -> List[str]:
        """Trae 1 fila y usa sus keys como lista de columnas disponibles."""
        try:
            resp = self.client.table("records_standards").select("*").limit(1).execute()
            data = resp.data or []
            if data:
                return sorted(list(data[0].keys()))
        except Exception:
            pass
        # fallback (tabla vacía o sin permisos de select)
        return sorted([
            "id",
            "record_scope","record_type","category","pool_length","gender","stroke","distance",
            "time_clock","time_ms",
            "athlete_name","country","record_date","competition_name","city",
            "source_name","source_url","notes","last_updated",
        ])

    def has(self, col: str) -> bool:
        return col in self.columns

    def _translate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """El payload se arma con nombres semánticos (los de tu modelo).
        Acá se filtran sólo las columnas existentes para evitar PGRST204.
        """
        out: Dict[str, Any] = {}
        for k, v in payload.items():
            if self.has(k):
                out[k] = v
        return out

    def upsert_record(self, payload: Dict[str, Any], source_priority: int = 50) -> Tuple[str, Optional[Dict[str, Any]]]:
        """status: inserted|updated|filled|unchanged|skipped_lower_priority"""
        required = ["record_scope","record_type","category","pool_length","gender","stroke","distance"]
        for k in required:
            if payload.get(k) in (None, ""):
                raise ValueError(f"payload missing {k}")

        # query existente por llave compuesta
        q = self.client.table("records_standards").select("*")
        for k in required:
            q = q.eq(k, payload[k])
        existing_resp = q.limit(1).execute()
        existing = (existing_resp.data or [None])[0]

        # normaliza tiempos
        new_ms = payload.get("time_ms")
        if new_ms is None:
            new_ms = parse_time_to_ms(payload.get("time_clock", ""))

        # si no existe: INSERT
        if not existing:
            insert_payload = dict(payload)
            insert_payload["last_updated"] = RUN_TS
            insert_payload = self._translate_payload(insert_payload)
            resp = self.client.table("records_standards").insert(insert_payload).execute()
            row = (resp.data or [None])[0]
            return "inserted", row

        old_ms = existing.get("time_ms")
        if old_ms is None:
            old_ms = parse_time_to_ms(existing.get("time_clock") or "")

        time_changed = (new_ms is not None and old_ms is not None and int(new_ms) != int(old_ms))

        # priority: si tenemos un campo de fuente anterior, lo inferimos por nombre
        old_source = (existing.get("source_name") or "").strip().lower()
        old_pri = 50
        if "world aquatics" in old_source:
            old_pri = 100
        elif "usa swimming" in old_source or "data hub" in old_source:
            old_pri = 90
        elif "consanat" in old_source:
            old_pri = 80
        elif "wikipedia" in old_source:
            old_pri = 10

        # Fill fields (solo si existen en DB)
        candidate_fill = ["athlete_name","country","record_date","competition_name","city","source_name","source_url","notes"]
        updates: Dict[str, Any] = {}

        for f in candidate_fill:
            if not self.has(f):
                continue
            newv = payload.get(f)
            oldv = existing.get(f)
            if newv is None or str(newv).strip() == "":
                continue
            if oldv is None or str(oldv).strip() == "":
                updates[f] = newv

        # Update time si cambió y la fuente no es de menor prioridad
        if time_changed:
            if source_priority < old_pri:
                # no pisamos con fuente "más débil"
                return "skipped_lower_priority", existing
            if self.has("time_ms") and new_ms is not None:
                updates["time_ms"] = int(new_ms)
            if self.has("time_clock") and new_ms is not None:
                updates["time_clock"] = format_ms_to_clock(int(new_ms))

        if updates:
            updates["last_updated"] = RUN_TS
            updates = self._translate_payload(updates)
            resp = (
                self.client.table("records_standards")
                .update(updates)
                .eq("id", existing["id"])
                .execute()
            )
            row = (resp.data or [None])[0]
            if time_changed:
                return "updated", row
            return "filled", row

        return "unchanged", existing

    def log_row(self, scope: str, prueba: str, atleta: str, status: str = "", detail: str = "") -> None:
        """Inserta en scraper_logs (tolerante a schema)."""
        base = {
            "fecha": RUN_TS,
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "status": status,
            "detail": detail,
            "run_id": str(RUN_ID),
        }
        try:
            self.client.table("scraper_logs").insert(base).execute()
        except Exception:
            # No matar el run por logs
            pass


# -------------------------- WA (World Aquatics) --------------------------

@dataclass
class WASpec:
    code: str  # WR, OR, WJ, CR_AMERICAS
    pool: str  # LCM/SCM
    gender: str  # M/F


def wa_url(spec: WASpec) -> str:
    if spec.code == "WR":
        return f"{WA_BASE}?recordType=WR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "OR":
        return f"{WA_BASE}?recordType=OR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "WJ":
        return f"{WA_BASE}?recordCode=WJ&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"
    if spec.code == "CR_AMERICAS":
        return f"{WA_BASE}?recordType=PAN&recordCode=CR&eventTypeId=&region=AMERICAS&countryId=&gender={spec.gender}&pool={spec.pool}"
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


def wa_scope_and_type(code: str, pool: str) -> Tuple[str, str, int]:
    """(record_scope, record_type, priority)"""
    pool = pool_label(pool)
    if code == "WR":
        return "Mundial", "Récord Mundial", 100
    if code == "OR":
        return "Olímpico", "Récord Olímpico", 100
    if code == "WJ":
        return "Mundial", "Récord Mundial Junior", 100
    if code == "CR_AMERICAS":
        # guardamos en scope "Panamericano" para mantener consistencia con tus scopes,
        # y distinguimos por record_type.
        return "Panamericano", "Récord Continental Américas", 100
    raise ValueError(code)


def wa_download_xlsx(page, url: str, out_dir: str) -> str:
    page.goto(url, wait_until="networkidle", timeout=120_000)
    try:
        page.get_by_role("button", name=re.compile(r"Accept Cookies", re.I)).click(timeout=3000)
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
        for i, row in enumerate(values[:50]):
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
                {"event": event, "time": t, "athlete": athlete, "country": country,
                 "date": date, "location": location, "competition": competition}
            )

    return rows_out


# -------------------------- CONSANAT --------------------------

def consanat_fetch(retries: int = 3) -> str:
    last_err = ""
    for i in range(retries):
        try:
            r = requests.get(CONS_NATACION_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = str(e)
            warn(f"CONSANAT fetch intento {i+1}/{retries} falló: {e} (reintento en {3*(i+1)}s)")
            time.sleep(3 * (i + 1))
    raise RuntimeError(last_err)


def consanat_parse(html: str) -> List[Dict[str, Any]]:
    """Parser conservador: extrae texto y arma bloques."""
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
            g_end = None
            for i in range(g_start + 1, len(chunk)):
                if chunk[i].upper() in ("FEMININO", "MASCULINO"):
                    g_end = i
                    break
            if g_end is None:
                g_end = len(chunk)
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
                event = rows[i]
                t = rows[i + 1]
                athlete = rows[i + 2]
                country = rows[i + 3]
                date = rows[i + 4]
                location = rows[i + 5]
                comp = rows[i + 6]

                dist, stroke = parse_event(event)
                ms = parse_time_to_ms(t)
                if dist is None or stroke is None or ms is None:
                    continue

                out.append({
                    "pool": pool, "gender": gender,
                    "event": event, "distance": dist, "stroke": stroke,
                    "time_ms": ms,
                    "athlete": athlete,
                    "country": to_iso3(country) or country,
                    "date": parse_date(date),
                    "city": location,
                    "competition": comp,
                    "source_name": "CONSANAT",
                    "source_url": CONS_NATACION_URL,
                    "priority": 80,
                })
    return out


# -------------------------- Wikipedia: Sudamérica --------------------------

def wiki_sudam_fetch() -> str:
    r = requests.get(WIKI_SUDAM_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def _wiki_context_text(tag) -> str:
    if not tag:
        return ""
    try:
        return tag.get_text(" ", strip=True)
    except Exception:
        return ""


def wiki_sudam_parse(html: str) -> List[Dict[str, Any]]:
    """Parsea tablas wikitable con contexto (piscina + género)."""
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    tables = soup.select("table.wikitable")
    if DEBUG:
        log(f"🔎 WIKI_SUDAM tables={len(tables)}")

    for table in tables:
        # contexto (miramos headings previos)
        pool = ""
        gender = ""
        # buscamos un heading previo cercano
        prev = table.find_previous(["h2", "h3", "h4"])
        ctx = _wiki_context_text(prev).lower()
        # subimos un poco más si hace falta (piscina suele estar en h2 y sexo en h3)
        prev2 = prev.find_previous(["h2", "h3", "h4"]) if prev else None
        ctx2 = _wiki_context_text(prev2).lower()

        ctx_all = f"{ctx2} {ctx}".lower()

        if "piscina corta" in ctx_all or "25" in ctx_all:
            pool = "SCM"
        if "piscina larga" in ctx_all or "50" in ctx_all:
            pool = "LCM"

        if "mascul" in ctx_all or "hombres" in ctx_all or "men" in ctx_all:
            gender = "M"
        if "femen" in ctx_all or "mujeres" in ctx_all or "women" in ctx_all:
            gender = "F"

        # Si no pudimos inferir, igual parseamos pero con defaults razonables
        if not pool:
            pool = "LCM"
        if not gender:
            # si la tabla tiene caption con "Masculino/Femenino", lo usamos
            cap = _wiki_context_text(table.find("caption")).lower()
            if "mascul" in cap or "hombres" in cap:
                gender = "M"
            elif "femen" in cap or "mujeres" in cap:
                gender = "F"
            else:
                gender = "M"

        # header → índices
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th", "td"])]

        def idx_of(keys: Iterable[str]) -> Optional[int]:
            for k in keys:
                for i, h in enumerate(headers):
                    if k in h:
                        return i
            return None

        i_event = idx_of(["prueba", "event"]) or 0
        i_time = idx_of(["marca", "tiempo", "time"]) or 1
        i_name = idx_of(["nadador", "atleta", "name", "recordista"]) or 2
        i_country = idx_of(["país", "pais", "country", "nation", "nacionalidad"]) or 3
        i_date = idx_of(["fecha", "date"]) or 4
        i_meet = idx_of(["competición", "competicion", "meet", "competition"])  # opcional
        i_loc = idx_of(["lugar", "location", "sede"])  # opcional

        for r in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in r.find_all(["th", "td"])]
            if len(cells) < 3:
                continue
            event = cells[i_event] if i_event < len(cells) else ""
            t_raw = cells[i_time] if i_time < len(cells) else ""
            name = cells[i_name] if i_name < len(cells) else ""
            country_raw = cells[i_country] if i_country < len(cells) else ""
            date_raw = cells[i_date] if i_date < len(cells) else ""
            meet = cells[i_meet] if (i_meet is not None and i_meet < len(cells)) else ""
            loc = cells[i_loc] if (i_loc is not None and i_loc < len(cells)) else ""

            dist, stroke = parse_event(event)
            ms = parse_time_to_ms(t_raw)
            if dist is None or stroke is None or ms is None:
                continue

            out.append({
                "pool": pool,
                "gender": gender,
                "event": event,
                "distance": dist,
                "stroke": stroke,
                "time_ms": ms,
                "athlete": name,
                "country": to_iso3(country_raw) or country_raw,
                "date": parse_date(date_raw),
                "city": loc,
                "competition": meet,
                "source_name": "Wikipedia (Sudamérica)",
                "source_url": WIKI_SUDAM_URL,
                "priority": 10,
            })

    return out


# -------------------------- Wikipedia: Juegos Panamericanos --------------------------

def wiki_panam_fetch() -> str:
    r = requests.get(WIKI_PANAM_GAMES_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def wiki_panam_games_parse(html: str) -> List[Dict[str, Any]]:
    """Parsea tablas con columnas: Event | Time | Name | Nationality | Date | Meet | Location."""
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    tables = soup.select("table.wikitable")
    if DEBUG:
        log(f"🔎 WIKI_PANAM tables={len(tables)}")

    for table in tables:
        # contexto de headings previos
        prev = table.find_previous(["h2", "h3", "h4"])
        ctx = _wiki_context_text(prev).lower()
        prev2 = prev.find_previous(["h2", "h3", "h4"]) if prev else None
        ctx2 = _wiki_context_text(prev2).lower()
        ctx_all = f"{ctx2} {ctx}".lower()

        gender = "M"
        if "women" in ctx_all or "female" in ctx_all:
            gender = "F"
        if "men" in ctx_all or "male" in ctx_all:
            gender = "M"

        pool = "LCM"
        if "short course" in ctx_all:
            pool = "SCM"
        if "long course" in ctx_all:
            pool = "LCM"

        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th","td"])]
        if not headers:
            continue

        # sólo tablas que parezcan de récords (tienen event y time)
        if not any("event" in h for h in headers) or not any("time" in h for h in headers):
            continue

        def idx_of(keys: Iterable[str]) -> Optional[int]:
            for k in keys:
                for i, h in enumerate(headers):
                    if k in h:
                        return i
            return None

        i_event = idx_of(["event"]) or 0
        i_time = idx_of(["time"]) or 1
        i_name = idx_of(["name"]) or 2
        i_nat = idx_of(["nationality", "nation", "noc"]) or 3
        i_date = idx_of(["date"]) or 4
        i_meet = idx_of(["meet", "competition"])  # opcional
        i_loc = idx_of(["location", "venue", "place"])  # opcional

        for r in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in r.find_all(["th","td"])]
            if len(cells) < 4:
                continue
            event = cells[i_event] if i_event < len(cells) else ""
            t_raw = cells[i_time] if i_time < len(cells) else ""
            name = cells[i_name] if i_name < len(cells) else ""
            nat = cells[i_nat] if i_nat < len(cells) else ""
            date_raw = cells[i_date] if i_date < len(cells) else ""
            meet = cells[i_meet] if (i_meet is not None and i_meet < len(cells)) else ""
            loc = cells[i_loc] if (i_loc is not None and i_loc < len(cells)) else ""

            dist, stroke = parse_event(event)
            ms = parse_time_to_ms(t_raw)
            if dist is None or stroke is None or ms is None:
                continue

            out.append({
                "pool": pool,
                "gender": gender,
                "event": event,
                "distance": dist,
                "stroke": stroke,
                "time_ms": ms,
                "athlete": name,
                "country": to_iso3(nat) or nat,
                "date": parse_date(date_raw),
                "city": loc,
                "competition": meet,
                "source_name": "Wikipedia (Juegos Panamericanos)",
                "source_url": WIKI_PANAM_GAMES_URL,
                "priority": 10,
            })

    return out


# -------------------------- USA Data Hub (opcional) --------------------------

def datahub_try_fetch_playwright() -> Optional[str]:
    """Intenta obtener un CSV o JSON desde DataHub vía Playwright.
    Devuelve el contenido crudo (texto) si encuentra algo descargable.
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            captured: Dict[str, str] = {}

            def on_response(resp):
                try:
                    url = resp.url
                    ct = (resp.headers.get("content-type") or "").lower()
                    if any(x in ct for x in ["text/csv", "application/json"]) or url.lower().endswith(".csv"):
                        # evitamos cosas chicas
                        body = resp.text()
                        if body and len(body) > 2000 and len(captured) < 3:
                            captured[url] = body
                except Exception:
                    pass

            page.on("response", on_response)

            page.goto(USA_DATAHUB_URL, wait_until="networkidle", timeout=120_000)

            # Intento 1: buscar un link/botón de descarga
            try:
                with page.expect_download(timeout=10_000) as dl_info:
                    page.get_by_role("button", name=re.compile(r"download|export|csv", re.I)).click(timeout=5000)
                dl = dl_info.value
                path = f"/tmp/datahub_{uuid.uuid4().hex}.csv"
                dl.save_as(path)
                browser.close()
                try:
                    return open(path, "r", encoding="utf-8", errors="replace").read()
                except Exception:
                    return None
            except Exception:
                pass

            # Intento 2: si capturamos algún JSON/CSV por XHR
            browser.close()
            if captured:
                # devolvemos el mayor
                best_url = max(captured.keys(), key=lambda u: len(captured[u]))
                if DEBUG:
                    log(f"🧩 DATAHUB captured from {best_url} len={len(captured[best_url])}")
                return captured[best_url]

    except Exception as e:
        if DEBUG:
            warn(f"DATAHUB playwright failed: {e}")

    return None


def datahub_parse(text: str) -> List[Dict[str, Any]]:
    """Parser muy conservador: busca CSV con columnas event/time/name/nation/pool/gender.
    Si no se reconoce, devuelve [].
    """
    if not text or len(text) < 50:
        return []

    # Si es JSON, intentamos parsearlo
    if text.lstrip().startswith("{") or text.lstrip().startswith("["):
        try:
            data = json.loads(text)
        except Exception:
            return []
        # Estructuras variables. No adivinamos: devolvemos [] por ahora.
        return []

    # CSV: split simple (sin pandas para evitar dependencia extra)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return []

    header = [h.strip().lower() for h in re.split(r",|\t|;", lines[0])]
    def idx(keys: Iterable[str]) -> Optional[int]:
        for k in keys:
            for i,h in enumerate(header):
                if k in h:
                    return i
        return None

    i_event = idx(["event"]) 
    i_time = idx(["time"])
    i_name = idx(["name","athlete","swimmer"])
    i_nat = idx(["nation","noc","country"])
    i_gender = idx(["gender","sex"])
    i_pool = idx(["pool","course"])

    if i_event is None or i_time is None:
        return []

    out: List[Dict[str, Any]] = []
    for ln in lines[1:]:
        parts = re.split(r",|\t|;", ln)
        if len(parts) < max(i_event, i_time) + 1:
            continue
        event = parts[i_event].strip()
        t_raw = parts[i_time].strip()
        name = parts[i_name].strip() if i_name is not None and i_name < len(parts) else ""
        nat = parts[i_nat].strip() if i_nat is not None and i_nat < len(parts) else ""
        gender = parts[i_gender].strip() if i_gender is not None and i_gender < len(parts) else "M"
        pool = parts[i_pool].strip() if i_pool is not None and i_pool < len(parts) else "LCM"

        dist, stroke = parse_event(event)
        ms = parse_time_to_ms(t_raw)
        if dist is None or stroke is None or ms is None:
            continue

        out.append({
            "pool": pool_label(pool),
            "gender": gender_label(gender),
            "event": event,
            "distance": dist,
            "stroke": stroke,
            "time_ms": ms,
            "athlete": name,
            "country": to_iso3(nat) or nat,
            "date": "",
            "city": "",
            "competition": "",
            "source_name": "USA Swimming Data Hub",
            "source_url": USA_DATAHUB_URL,
            "priority": 90,
        })
    return out


# -------------------------- Build payload --------------------------

def build_payload(
    record_scope: str,
    record_type: str,
    category: str = "Open",
    pool: str,
    gender: str,
    distance: int,
    stroke: str,
    time_ms: int,
    athlete: str,
    athlete_country: str,
    record_date: str,
    competition_name: str,
    city: str,
    source_name: str,
    source_url: str,
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "record_scope": record_scope,
        "record_type": record_type,
        "category": category,
        "pool_length": pool_label(pool),
        "gender": gender_label(gender),
        "distance": int(distance),
        "stroke": stroke,
        "time_ms": int(time_ms),
        "time_clock": format_ms_to_clock(int(time_ms)),
        "athlete_name": athlete or "",
        "country": to_iso3(athlete_country) or (athlete_country or ""),
        "record_date": record_date or "",
        "competition_name": competition_name or "",
        "city": city or "",
        "source_name": source_name or "",
        "source_url": source_url or "",
        "notes": notes or "",
    }


# -------------------------- Runners --------------------------

def run_wa(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "inserted": 0, "updated": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    tmp_dir = f"/tmp/mdv_wa_{RUN_ID}"
    os.makedirs(tmp_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for spec in wa_specs():
            url = wa_url(spec)
            log(f"🔎 WA | {spec.code} | {spec.pool} | {spec.gender} | {url}")
            try:
                xlsx_path = wa_download_xlsx(page, url, tmp_dir)
                rows = wa_parse_xlsx(xlsx_path)
                record_scope, record_type, pri = wa_scope_and_type(spec.code, spec.pool)

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
                        athlete_country=r.get("country", ""),
                        record_date=parse_date(r.get("date", "")),
                        competition_name=r.get("competition", ""),
                        city=r.get("location", ""),
                        source_name="World Aquatics",
                        source_url=url,
                        notes="",
                    )

                    try:
                        status, _ = sb.upsert_record(payload, source_priority=pri)
                        if status == "inserted":
                            stats["inserted"] += 1
                        elif status == "updated":
                            stats["updated"] += 1
                        elif status == "filled":
                            stats["filled"] += 1
                        elif status == "skipped_lower_priority":
                            stats["skipped"] += 1
                        else:
                            stats["unchanged"] += 1
                    except Exception as e:
                        stats["errors"] += 1
                        msg = f"WA upsert error {spec.gender} {dist} {stroke} {spec.pool}: {e}"
                        err(msg)
                        if DEBUG:
                            traceback.print_exc()
                        sb.log_row("ERROR", "WA", msg, status="error", detail=str(e))
                        continue

            except Exception as e:
                stats["errors"] += 1
                msg = f"WA {spec.code} {spec.pool} {spec.gender} error: {e}"
                err(msg)
                if DEBUG:
                    traceback.print_exc()
                sb.log_row("ERROR", "WA", msg, status="error", detail=str(e))
                continue

        browser.close()

    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    return stats


def run_sudam(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "inserted": 0, "updated": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    rows: List[Dict[str, Any]] = []
    used = ""

    # 1) CONSANAT (si responde)
    try:
        html = consanat_fetch(retries=3)
        rows = consanat_parse(html)
        used = "CONSANAT"
    except Exception as e:
        warn(f"CONSANAT no disponible: {e}. Fallback a Wikipedia…")

    # 2) Wikipedia fallback
    if not rows:
        try:
            html = wiki_sudam_fetch()
            rows = wiki_sudam_parse(html)
            used = "WIKI"
        except Exception as e:
            stats["errors"] += 1
            msg = f"WIKI_SUDAM fetch/parse error: {e}"
            err(msg)
            if DEBUG:
                traceback.print_exc()
            sb.log_row("ERROR", "SUDAM", msg, status="error", detail=str(e))
            return stats

    log(f"🌎 SUDAM source={used} filas={len(rows)}")

    # UPSERT
    for r in rows:
        stats["seen"] += 1
        record_scope = "Sudamericano"
        record_type = "Récord Sudamericano"
        pri = int(r.get("priority") or (80 if used == "CONSANAT" else 10))

        payload = build_payload(
            record_scope=record_scope,
            record_type=record_type,
            pool=r.get("pool", "LCM"),
            gender=r.get("gender", "M"),
            distance=int(r["distance"]),
            stroke=r["stroke"],
            time_ms=int(r["time_ms"]),
            athlete=r.get("athlete", ""),
            athlete_country=r.get("country", ""),
            record_date=r.get("date", ""),
            competition_name=r.get("competition", ""),
            city=r.get("city", ""),
            source_name=r.get("source_name", "Sudamérica"),
            source_url=r.get("source_url", ""),
            notes="",
        )

        try:
            status, _ = sb.upsert_record(payload, source_priority=pri)
            if status == "inserted":
                stats["inserted"] += 1
            elif status == "updated":
                stats["updated"] += 1
            elif status == "filled":
                stats["filled"] += 1
            elif status == "skipped_lower_priority":
                stats["skipped"] += 1
            else:
                stats["unchanged"] += 1

        except Exception as e:
            stats["errors"] += 1
            prueba = f"{r.get('gender')} {r.get('distance')}m {r.get('stroke')} ({r.get('pool')})"
            msg = f"SUDAM upsert error [{prueba}]: {e}"
            err(msg)
            if DEBUG:
                traceback.print_exc()
            sb.log_row("ERROR", "SUDAM", msg, status="error", detail=str(e))

    return stats


def run_panam_games(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "inserted": 0, "updated": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    try:
        html = wiki_panam_fetch()
        rows = wiki_panam_games_parse(html)
    except Exception as e:
        stats["errors"] += 1
        msg = f"WIKI_PANAM_GAMES fetch/parse error: {e}"
        err(msg)
        if DEBUG:
            traceback.print_exc()
        sb.log_row("ERROR", "PANAM_GAMES", msg, status="error", detail=str(e))
        return stats

    log(f"🏟️ PANAM_GAMES filas={len(rows)}")

    for r in rows:
        stats["seen"] += 1
        # Scope: Panamericano. Type: distinguimos campeonato (Juegos) del continental (WA/DataHub)
        record_scope = "Panamericano"
        record_type = "Récord Juegos Panamericanos"
        pri = int(r.get("priority") or 10)

        payload = build_payload(
            record_scope=record_scope,
            record_type=record_type,
            category="Open",
            pool=r.get("pool", "LCM"),
            gender=r.get("gender", "M"),
            distance=int(r["distance"]),
            stroke=r["stroke"],
            time_ms=int(r["time_ms"]),
            athlete=r.get("athlete", ""),
            athlete_country=r.get("country", ""),
            record_date=r.get("date", ""),
            competition_name=r.get("competition", ""),
            city=r.get("city", ""),
            source_name=r.get("source_name", "Wikipedia (Juegos Panamericanos)"),
            source_url=r.get("source_url", WIKI_PANAM_GAMES_URL),
            notes="",
        )

        try:
            status, _ = sb.upsert_record(payload, source_priority=pri)
            if status == "inserted":
                stats["inserted"] += 1
            elif status == "updated":
                stats["updated"] += 1
            elif status == "filled":
                stats["filled"] += 1
            elif status == "skipped_lower_priority":
                stats["skipped"] += 1
            else:
                stats["unchanged"] += 1

        except Exception as e:
            # fallback: si el record_type nuevo no es aceptado por tu tabla, reintentamos
            # usando record_type="Récord Panamericano" y category="Juegos Panamericanos"
            try:
                payload_fb = dict(payload)
                payload_fb["record_type"] = "Récord Panamericano"
                payload_fb["category"] = "Juegos Panamericanos"
                payload_fb["notes"] = (payload_fb.get("notes","") + " | fallback record_type").strip(" |")
                status, _ = sb.upsert_record(payload_fb, source_priority=pri)
                if status == "inserted":
                    stats["inserted"] += 1
                elif status == "updated":
                    stats["updated"] += 1
                elif status == "filled":
                    stats["filled"] += 1
                elif status == "skipped_lower_priority":
                    stats["skipped"] += 1
                else:
                    stats["unchanged"] += 1
                continue
            except Exception:
                pass

            stats["errors"] += 1
            prueba = f"{r.get('gender')} {r.get('distance')}m {r.get('stroke')} ({r.get('pool')})"
            msg = f"PANAM_GAMES upsert error [{prueba}]: {e}"
            err(msg)
            if DEBUG:
                traceback.print_exc()
            sb.log_row("ERROR", "PANAM_GAMES", msg, status="error", detail=str(e))

    return stats


def run_datahub_optional(sb: SB) -> Dict[str, int]:
    """Opcional: intenta DataHub. Nunca rompe el run."""
    stats = {"seen": 0, "inserted": 0, "updated": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}
    raw = datahub_try_fetch_playwright()
    if not raw:
        warn("DATAHUB: no se pudo obtener dataset (ok, es opcional).")
        return stats

    rows = datahub_parse(raw)
    if not rows:
        warn("DATAHUB: dataset obtenido pero no reconocible (ok, opcional).")
        return stats

    log(f"🏁 DATAHUB filas parseadas={len(rows)}")

    # Guardamos como Continental Américas (mismo record_type que WA CR_AMERICAS), fill-only por prioridad 90.
    for r in rows:
        stats["seen"] += 1
        record_scope = "Panamericano"
        record_type = "Récord Continental Américas"

        payload = build_payload(
            record_scope=record_scope,
            record_type=record_type,
            pool=r.get("pool", "LCM"),
            gender=r.get("gender", "M"),
            distance=int(r["distance"]),
            stroke=r["stroke"],
            time_ms=int(r["time_ms"]),
            athlete=r.get("athlete", ""),
            athlete_country=r.get("country", ""),
            record_date=r.get("date", ""),
            competition_name=r.get("competition", ""),
            city=r.get("city", ""),
            source_name=r.get("source_name", "USA Swimming Data Hub"),
            source_url=r.get("source_url", USA_DATAHUB_URL),
            notes="(DataHub opcional)",
        )

        try:
            status, _ = sb.upsert_record(payload, source_priority=int(r.get("priority") or 90))
            if status == "inserted":
                stats["inserted"] += 1
            elif status == "updated":
                stats["updated"] += 1
            elif status == "filled":
                stats["filled"] += 1
            elif status == "skipped_lower_priority":
                stats["skipped"] += 1
            else:
                stats["unchanged"] += 1
        except Exception as e:
            stats["errors"] += 1
            msg = f"DATAHUB upsert error: {e}"
            warn(msg)
            if DEBUG:
                traceback.print_exc()
            sb.log_row("ERROR", "DATAHUB", msg, status="error", detail=str(e))

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
        err(f"Supabase init error: {e}")
        return 2

    log(f"MDV_UPDATER_VERSION={MDV_UPDATER_VERSION}")
    log(f"RUN_ID={RUN_ID}")
    log(f"Timestamp (UTC)={RUN_TS}")

    all_stats: Dict[str, Dict[str, int]] = {}

    all_stats["WA"] = run_wa(sb)
    all_stats["SUDAM"] = run_sudam(sb)
    all_stats["PANAM_GAMES"] = run_panam_games(sb)
    all_stats["DATAHUB_OPT"] = run_datahub_optional(sb)

    # Summary
    lines = [
        f"Version: {MDV_UPDATER_VERSION}",
        f"Run ID: {RUN_ID}",
        f"Timestamp (UTC): {RUN_TS}",
        "",
    ]
    for k, st in all_stats.items():
        lines.append(
            f"[{k}] seen={st['seen']} | inserted={st['inserted']} | updated={st['updated']} | "
            f"filled={st['filled']} | unchanged={st['unchanged']} | skipped={st['skipped']} | errors={st['errors']}"
        )

    body = "\n".join(lines)
    log(body)

    # Log summary row
    try:
        sb.log_row("RUN", "SUMMARY", json.dumps(all_stats, ensure_ascii=False), status="ok", detail="")
    except Exception:
        pass

    send_email(f"🏁 MDV Scraper | {RUN_ID} | {MDV_UPDATER_VERSION}", body)

    # Exit code: fallar solo si WA falla o si Sudam/Panam no pudieron procesar nada.
    fatal = 0
    if all_stats["WA"]["errors"] > 0:
        fatal += 1
    # Si SUDAM vio filas pero tuvo errores masivos
    if all_stats["SUDAM"]["seen"] > 0 and (all_stats["SUDAM"]["inserted"] + all_stats["SUDAM"]["updated"] + all_stats["SUDAM"]["filled"] + all_stats["SUDAM"]["unchanged"]) == 0:
        fatal += 1
    if all_stats["PANAM_GAMES"]["seen"] > 0 and (all_stats["PANAM_GAMES"]["inserted"] + all_stats["PANAM_GAMES"]["updated"] + all_stats["PANAM_GAMES"]["filled"] + all_stats["PANAM_GAMES"]["unchanged"]) == 0:
        fatal += 1

    return 1 if fatal else 0


if __name__ == "__main__":
    sys.exit(main())
