#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MDV Records Updater (WA + Sudamérica + Panamericano) — v13

Fixes v12 issues:
- country ahora = PAÍS DEL ATLETA (no el país de la sede). La sede se guarda como:
  * city = ciudad (si viene "Ciudad, País" se toma la parte izquierda)
  * source_note incluye "loc_raw" (texto completo de la sede) para auditoría
- build_payload consistente (incluye type_probe, comp_country opcional)
- parse_event ahora detecta relevos (type_probe=relay) sin romper callers
- WA "too many values to unpack" corregido (parse_event devuelve 3 valores)
- Errores en fuentes no críticas (CONSANAT / WIKI) NO cortan el run (exit 0) si WA ok.
- UPSERT:
  * inserta récords faltantes
  * actualiza tiempos si cambian
  * completa campos vacíos (incluye type_probe) aunque el tiempo no cambie
- Log en scraper_logs tolerante.

ENV requeridas:
- SUPABASE_URL
- SUPABASE_KEY  (service_role o key con permisos de escritura)

Opcionales:
- EMAIL_USER / EMAIL_PASS
- PANAM_AQUATICS_URL
- MDV_UPDATER_VERSION
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import uuid
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

# .env solo local. En Actions: secrets/env.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# -------------------------- Config --------------------------

MDV_UPDATER_VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+SUDAM+PANAM_v14_TYPEPROBEFIX")
RUN_ID = os.getenv("GITHUB_RUN_ID") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")
RUN_DATE = datetime.now(timezone.utc).date().isoformat()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASS = os.getenv("EMAIL_PASS", "").strip()

PANAM_AQUATICS_URL = os.getenv(
    "PANAM_AQUATICS_URL",
    "https://www.panamaquatics.com/customPage/ed0bf338-6fab-4b35-abc0-418e0db1749e",
).strip()

CONS_NATACION_URL = "https://consanat.com/records/136/natacion"
SUDAM_WIKI_URL = "https://es.wikipedia.org/wiki/Anexo:Plusmarcas_de_Sudam%C3%A9rica_de_nataci%C3%B3n"
PANAM_GAMES_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_Pan_American_Games_records_in_swimming"

# Preferimos nombres en español (tu DB)
STROKE_MAP = {
    "freestyle": "Libre",
    "backstroke": "Espalda",
    "breaststroke": "Pecho",
    "butterfly": "Mariposa",
    "medley": "Combinado",
    "individual medley": "Combinado",
    "im": "Combinado",
    "libre": "Libre",
    "espalda": "Espalda",
    "pecho": "Pecho",
    "mariposa": "Mariposa",
    "combinado": "Combinado",
}

# -------------------------- Helpers: time/date --------------------------

def parse_time_to_ms(raw: str) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"\s+", "", s)

    # HH:MM:SS.xx / MM:SS.xx
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 3:
                hh = int(parts[0])
                mm = int(parts[1])
                sec = float(parts[2])
            elif len(parts) == 2:
                hh = 0
                mm = int(parts[0])
                sec = float(parts[1])
            else:
                return None
            return int(round(((hh * 3600 + mm * 60) + sec) * 1000))
        except Exception:
            return None

    # 23.76 / 1.54.50 / 15.48.32
    dot_parts = s.split(".")
    try:
        if len(dot_parts) == 2:
            return int(round(float(s) * 1000))
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
    if ms is None:
        return ""
    if ms < 0:
        ms = 0
    total_seconds = ms // 1000
    cent = (ms % 1000) // 10
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = total_seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{cent:02d}"


def parse_date(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
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
            return None
    return None


# -------------------------- Helpers: event parsing --------------------------

EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|individual\s+medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.IGNORECASE,
)

RELAY_RE = re.compile(
    r"(?P<legs>[23456])\s*[x×]\s*(?P<legdist>\d{2,4})\s*m|relay|relevo",
    re.IGNORECASE,
)

def parse_event(event_raw: str) -> Tuple[Optional[int], Optional[str], bool]:
    """(distance_m, stroke_es, is_relay)

    distance_m:
      - individuales: distancia de la prueba
      - relevos: distancia de una posta (leg distance) para que puedas usar distance+stroke como la prueba base,
        y diferenciar con type_probe=relay.
    """
    if not event_raw:
        return None, None, False
    s = str(event_raw).strip().lower()
    s = s.replace("individual medley", "medley")

    is_relay = bool(RELAY_RE.search(s))

    # Si es relevo: preferimos capturar la distancia de la posta (ej 4x100 -> 100)
    if is_relay:
        mrel = RELAY_RE.search(s)
        if mrel and mrel.groupdict().get("legdist"):
            try:
                legdist = int(mrel.group("legdist"))
            except Exception:
                legdist = None
        else:
            legdist = None

        # stroke en relevos
        stroke_key = None
        for k in ("freestyle", "backstroke", "breaststroke", "butterfly", "medley", "libre", "espalda", "pecho", "mariposa", "combinado", "im"):
            if k in s:
                stroke_key = k
                break
        stroke = STROKE_MAP.get(stroke_key or "freestyle", STROKE_MAP["freestyle"])

        return legdist, stroke, True

    m = EVENT_RE.search(s)
    if not m:
        return None, None, False
    dist = int(m.group("dist"))
    stroke_key = m.group("stroke").lower()
    stroke = STROKE_MAP.get(stroke_key)
    return dist, stroke, False


def infer_type_probe(event: str, athlete: str, is_relay: bool) -> str:
    if is_relay:
        return "relay"
    # Heurística adicional: si hay muchos nombres en athlete, probablemente relevo
    a = (athlete or "").strip()
    if a and (a.count(",") >= 3 or a.count(";") >= 3 or a.count("/") >= 3):
        return "relay"
    return "individual"


def gender_label(g: str) -> str:
    return "M" if str(g).upper().startswith("M") else "F"


def pool_label(pool: str) -> str:
    p = str(pool).upper()
    if p in ("LCM", "50M", "50", "L"):
        return "LCM"
    if p in ("SCM", "25M", "25", "S"):
        return "SCM"
    if p in ("SCY", "25Y", "YARDS", "YARD", "Y"):
        return "SCY"
    return p


def split_city(loc_raw: str) -> str:
    """De 'Fukuoka, Japan' -> 'Fukuoka'. Si no hay coma, devuelve texto completo."""
    if not loc_raw:
        return ""
    s = str(loc_raw).strip()
    # separadores frecuentes
    for sep in (",", " - ", " – ", " — "):
        if sep in s:
            return s.split(sep, 1)[0].strip()
    return s


# -------------------------- Supabase helpers --------------------------

class SB:
    def __init__(self, url: str, key: str):
        if not url or not key:
            raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")
        self.client = create_client(url, key)
        self.columns = self._detect_columns()
        print(f"🧬 DB columns detectadas: {len(self.columns)}")

    def _detect_columns(self) -> List[str]:
        # Tomamos las claves de un row existente (ya tienes data). Si estuviera vacío, caemos a lista mínima.
        try:
            resp = self.client.table("records_standards").select("*").limit(1).execute()
            if resp.data and isinstance(resp.data, list) and resp.data[0]:
                return sorted(list(resp.data[0].keys()))
        except Exception:
            pass
        return [
            "record_scope","record_type","category","pool_length","gender","distance","stroke",
            "time_clock","time_ms","athlete_name","country","record_date","competition_name","city",
            "source_name","source_url","source_note","last_updated"
        ]

    def has(self, col: str) -> bool:
        return col in set(self.columns)

    def upsert_record(self, payload: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        status: inserted | updated | filled | unchanged
        """
        # Clave lógica
        key_fields = ["record_scope","record_type","category","pool_length","gender","stroke","distance"]
        if self.has("type_probe"):
            key_fields.append("type_probe")

        # Defaults
        if "category" not in payload or payload["category"] is None:
            payload["category"] = "Open"
        if self.has("type_probe") and not payload.get("type_probe"):
            payload["type_probe"] = "individual"

        for k in key_fields:
            if payload.get(k) in (None, ""):
                raise ValueError(f"payload missing {k}")

        # Query existing (COALESCE(record_scope,'') + type_probe NULL≈individual)
        base_match = {
            'gender': payload.get('gender'),
            'category': payload.get('category'),
            'pool_length': payload.get('pool_length'),
            'stroke': payload.get('stroke'),
            'distance': payload.get('distance'),
            'record_type': payload.get('record_type'),
        }
        cands = (
            self.client.table('records_standards')
            .select('*')
            .match(base_match)
            .limit(20)
            .execute()
            .data
        ) or []
        want_scope = (payload.get('record_scope') or '').strip()
        want_type = (payload.get('type_probe') or 'individual').strip() if self.has('type_probe') else None
        existing = None
        for r in cands:
            r_scope = (r.get('record_scope') or '').strip()
            if r_scope != want_scope:
                continue
            if self.has('type_probe'):
                r_type = (r.get('type_probe') or 'individual').strip()
                if want_type == 'individual':
                    if r_type not in ('', 'individual'):
                        continue
                else:
                    if r_type != want_type:
                        continue
            existing = r
            break

        new_ms = payload.get("time_ms")
        if new_ms is None:
            new_ms = parse_time_to_ms(payload.get("time_clock", ""))
        if new_ms is not None:
            payload["time_ms"] = int(new_ms)
            payload["time_clock"] = payload.get("time_clock") or format_ms_to_clock(int(new_ms))

        # INSERT
        if not existing:
            insert_payload = dict(payload)
            insert_payload["last_updated"] = RUN_DATE
            resp = self.client.table("records_standards").insert(insert_payload).execute()
            row = (resp.data or [None])[0]
            return "inserted", row

        # UPDATE / FILL
        old_ms = existing.get("time_ms")
        if old_ms is None:
            old_ms = parse_time_to_ms(existing.get("time_clock") or "")

        time_changed = (new_ms is not None and old_ms is not None and int(new_ms) != int(old_ms))

        updates: Dict[str, Any] = {}

        # si cambió tiempo: actualizamos ambos campos
        if time_changed:
            updates["time_ms"] = int(new_ms) if new_ms is not None else existing.get("time_ms")
            updates["time_clock"] = payload.get("time_clock")

        # completar campos incompletos (aunque no cambie tiempo)
        fill_fields = [
            "athlete_name",
            "country",          # PAÍS DEL ATLETA (nacionalidad)
            "record_date",
            "competition_name",
            "city",
            "source_name",
            "source_url",
            "source_note",
        ]
        if self.has("type_probe"):
            fill_fields.append("type_probe")

        for f in fill_fields:
            if f not in payload:
                continue
            newv = payload.get(f)
            oldv = existing.get(f)
            if newv is None or str(newv).strip() == "":
                continue
            if oldv is None or str(oldv).strip() == "":
                updates[f] = newv

        if updates:
            updates["last_updated"] = RUN_DATE
            resp = self.client.table("records_standards").update(updates).eq("id", existing["id"]).execute()
            row = (resp.data or [None])[0]
            if time_changed:
                return "updated", row
            return "filled", row

        return "unchanged", existing

    def log(self, scope: str, prueba: str, atleta: str, t_old: str = "", t_new: str = "", message: str = "") -> None:
        base = {
            "fecha": RUN_TS,
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "tiempo_anterior": t_old,
            "tiempo_nuevo": t_new,
        }
        if message:
            base["message"] = message

        try:
            self.client.table("scraper_logs").insert(base).execute()
        except Exception:
            # no rompemos el run por logs
            return


# -------------------------- Build payload --------------------------

def build_payload(
    *,
    record_scope: str,
    record_type: str,
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
    type_probe: str = "individual",
    source_note: str = "",
    category: str = "Open",
) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "record_scope": record_scope,
        "record_type": record_type,
        "category": category,
        "pool_length": pool_label(pool),
        "gender": gender_label(gender),
        "distance": int(distance),
        "stroke": stroke,
        "time_ms": int(time_ms),
        "time_clock": format_ms_to_clock(int(time_ms)),
        "time_clock_2dp": time_str if time_str else None,
        "athlete_name": athlete or "",
        "country": (athlete_country or "").strip(),   # <- clave: PAÍS del atleta
        "record_date": (record_date or "").strip(),
        "competition_name": competition_name or "",
        "city": city or "",
        "source_name": source_name or "",
        "source_url": source_url or "",
        "source_note": source_note or "",
        "type_probe": type_probe or "individual",
        "last_updated": RUN_DATE,
    }
    return p


# -------------------------- WA (World Aquatics) --------------------------

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


def wa_download_xlsx(page, url: str, out_dir: str) -> str:
    page.goto(url, wait_until="networkidle", timeout=120_000)
    try:
        page.get_by_role("button", name=re.compile(r"Accept", re.I)).click(timeout=2000)
    except Exception:
        pass

    with page.expect_download(timeout=120_000) as dl_info:
        try:
            page.get_by_role("link", name=re.compile(r"XLSX", re.I)).click(timeout=10_000)
        except Exception:
            page.get_by_role("link", name=re.compile(r"Download", re.I)).click(timeout=10_000)

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
                    elif ("noc" in c) or ("nationality" in c) or ("nation" in c) or (("country" in c) and ("location" not in c) and ("venue" not in c) and ("place" not in c) and ("meet" not in c)):
                        header_map["athlete_country"] = j
                    elif ("country" in c) and (("location" in c) or ("venue" in c) or ("place" in c)):
                        header_map["location_country"] = j
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
            athlete_country = norm(row[header_map.get("athlete_country", -1)]) if "athlete_country" in header_map else ""
            location_country = norm(row[header_map.get("location_country", -1)]) if "location_country" in header_map else ""
            # Heurística: si no hay país del atleta, intentar extraer NOC de "(USA)" en el nombre
            if not athlete_country and athlete:
                m = re.search(r"\((?P<noc>[A-Z]{2,3})\)", athlete)
                if m:
                    athlete_country = m.group("noc")
            # Si athlete_country parece ser el país de la sede (coincide con la cola de "City, Country"), moverlo a location_country cuando esté vacío
            if athlete_country and location and ("," in location):
                tail = location.split(",")[-1].strip()
                if tail and athlete_country.strip().lower() == tail.lower() and not location_country:
                    location_country = athlete_country
                    # volver a intentar NOC en athlete; si no, dejamos athlete_country tal cual
                    m = re.search(r"\((?P<noc>[A-Z]{2,3})\)", athlete)
                    if m:
                        athlete_country = m.group("noc")
            date = norm(row[header_map.get("date", -1)]) if "date" in header_map else ""
            location = norm(row[header_map.get("location", -1)]) if "location" in header_map else ""
            # Si no hay location pero sí location_country, al menos guardamos el país como location
            if not location and location_country:
                location = location_country
            competition = norm(row[header_map.get("competition", -1)]) if "competition" in header_map else ""

            rows_out.append(
                {
                    "event": event,
                    "time": t,
                    "athlete": athlete,
                    "athlete_country": athlete_country,
                    "date": date,
                    "location": location,
                    "competition": competition,
                }
            )

    return rows_out


def wa_specs() -> List[WASpec]:
    out: List[WASpec] = []
    for gender in ("M", "F"):
        for pool in ("LCM", "SCM"):
            out.append(WASpec("WR", pool, gender))
            out.append(WASpec("WJ", pool, gender))
            out.append(WASpec("CR_AMERICAS", pool, gender))
        out.append(WASpec("OR", "LCM", gender))  # OR solo LCM
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


# -------------------------- Sudamérica (CONSANAT + fallback WIKI) --------------------------

def http_get_with_retry(url: str, tries: int = 3, timeout: int = 20, backoff_base: int = 3, headers: Optional[Dict[str,str]] = None) -> str:
    last = None
    for i in range(1, tries + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers or {"User-Agent":"Mozilla/5.0"})
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            print(f"⚠️ fetch intento {i}/{tries} falló: {e} (reintento en {backoff_base*i}s)")
            time.sleep(backoff_base*i)
    raise RuntimeError(str(last))


def consanat_fetch() -> str:
    return http_get_with_retry(CONS_NATACION_URL, tries=3, timeout=20)


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
                athlete_country = rows[i + 3]  # PAÍS del atleta según CONSANAT
                date = rows[i + 4]
                location = rows[i + 5]
                comp = rows[i + 6]

                dist, stroke, is_relay = parse_event(event)
                ms = parse_time_to_ms(t)
                if dist is None or stroke is None or ms is None:
                    continue

                out.append(
                    {
                        "pool": pool,
                        "gender": gender,
                        "event": event,
                        "distance": dist,
                        "stroke": stroke,
                        "is_relay": is_relay,
                        "time_ms": ms,
                        "athlete": athlete,
                        "athlete_country": athlete_country,
                        "date": parse_date(date) or date,
                        "city": split_city(location),
                        "competition": comp,
                        "source_url": CONS_NATACION_URL,
                        "source_name": "CONSANAT",
                        "loc_raw": location,
                    }
                )

    return out


def wiki_parse_tables(url: str, *, default_scope: str) -> List[Dict[str, Any]]:
    html = http_get_with_retry(url, tries=3, timeout=25)
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": re.compile("wikitable")})
    out: List[Dict[str, Any]] = []

    def clean(s: str) -> str:
        return re.sub(r"\[[0-9]+\]", "", (s or "")).strip()

    # Intentamos inferir pool y gender desde captions / headings previos
    for t in tables:
        caption = clean(t.get_text(" ", strip=True))[:200].lower()
        # Filtramos solo tablas que parezcan de récords (tienen "m" y tiempos)
        if "m" not in caption and "récord" not in caption and "record" not in caption:
            # igual puede ser útil, no descartamos
            pass

        # Pool
        pool = "LCM"
        if any(k in caption for k in ["piscina corta", "25 m", "25m", "short course"]):
            pool = "SCM"
        if any(k in caption for k in ["piscina larga", "50 m", "50m", "long course"]):
            pool = "LCM"

        # Gender
        gender = "M"
        if any(k in caption for k in ["mujeres", "women", "femenino"]):
            gender = "F"
        if any(k in caption for k in ["hombres", "men", "masculino"]):
            gender = "M"

        rows = t.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = [clean(c.get_text(" ", strip=True)).lower() for c in rows[0].find_all(["th","td"])]

        def idx_any(keys: Iterable[str]) -> Optional[int]:
            for k in keys:
                for i, c in enumerate(header_cells):
                    if k in c:
                        return i
            return None

        c_event = idx_any(["prueba","event"]) or 0
        c_time = idx_any(["marca","tiempo","time"]) or 1
        c_ath  = idx_any(["nadador","atleta","swimmer","recordista"]) or 2
        c_country = idx_any(["país","pais","country","nación","nacion"])  # puede ser atleta
        c_date = idx_any(["fecha","date"])  # opcional
        c_loc = idx_any(["lugar","sede","location","place"])  # suele ser sede/ciudad

        # Salteamos tablas que no tengan prueba+marca
        if c_event is None or c_time is None:
            continue

        for r in rows[1:]:
            cells = [clean(c.get_text(" ", strip=True)) for c in r.find_all(["th","td"])]
            if len(cells) < 3:
                continue
            event = cells[c_event] if c_event < len(cells) else ""
            t_raw = cells[c_time] if c_time < len(cells) else ""
            athlete = cells[c_ath] if c_ath < len(cells) else ""

            ms = parse_time_to_ms(t_raw)
            dist, stroke, is_relay = parse_event(event)
            if ms is None or dist is None or stroke is None:
                continue

            athlete_country = ""
            if c_country is not None and c_country < len(cells):
                athlete_country = cells[c_country]

            loc_raw = ""
            if c_loc is not None and c_loc < len(cells):
                loc_raw = cells[c_loc]

            date = ""
            if c_date is not None and c_date < len(cells):
                date = parse_date(cells[c_date]) or cells[c_date]

            out.append({
                "pool": pool,
                "gender": gender,
                "event": event,
                "distance": dist,
                "stroke": stroke,
                "is_relay": is_relay,
                "time_ms": ms,
                "athlete": athlete,
                "athlete_country": athlete_country,
                "date": date,
                "city": split_city(loc_raw),
                "competition": "",
                "source_url": url,
                "source_name": "Wikipedia",
                "loc_raw": loc_raw,
                "scope": default_scope,
            })

    return out


# -------------------------- PanAm Games (Wikipedia) --------------------------

def panam_games_wiki() -> List[Dict[str, Any]]:
    rows = wiki_parse_tables(PANAM_GAMES_WIKI_URL, default_scope="Panamericano")
    # Algunas tablas en EN tienen event names distintos; parse_event ya tolera.
    return rows


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
                    dist, stroke, is_relay = parse_event(r.get("event", ""))
                    ms = parse_time_to_ms(r.get("time", ""))
                    if dist is None or stroke is None or ms is None:
                        stats["skipped"] += 1
                        continue

                    athlete = r.get("athlete", "")
                    athlete_country = r.get("athlete_country", "")  # <- WA: país del atleta
                    loc_raw = r.get("location", "")
                    city = split_city(loc_raw)

                    payload = build_payload(
                        record_scope=record_scope,
                        record_type=record_type,
                        pool=spec.pool,
                        gender=spec.gender,
                        distance=dist,
                        stroke=stroke,
                        time_ms=ms,
                        athlete=athlete,
                        athlete_country=athlete_country,
                        record_date=parse_date(r.get("date", "")) or r.get("date", ""),
                        competition_name=r.get("competition", ""),
                        city=city,
                        source_name="World Aquatics",
                        source_url=url,
                        type_probe=infer_type_probe(r.get("event",""), athlete, is_relay),
                        source_note=json.dumps({"loc_raw": loc_raw}, ensure_ascii=False) if loc_raw else "",
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
                err = f"WA {spec.code} {spec.pool} {spec.gender} error: {e}"
                print("❌", err)
                sb.log("ERROR", f"WA {spec.code} {spec.pool} {spec.gender}", "", message=err)
                continue

        browser.close()

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return stats


def run_sudam(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    rows: List[Dict[str, Any]] = []
    source = "CONSANAT"
    try:
        html = consanat_fetch()
        rows = consanat_parse(html)
    except Exception as e:
        print(f"⚠️ CONSANAT no disponible: {e}. Fallback a Wikipedia…")
        source = "WIKI"
        try:
            rows = wiki_parse_tables(SUDAM_WIKI_URL, default_scope="Sudamericano")
            print(f"🌎 SUDAM source=WIKI filas={len(rows)}")
        except Exception as ee:
            stats["errors"] += 1
            sb.log("ERROR", "SUDAM", "", message=str(ee))
            return stats

    for r in rows:
        stats["seen"] += 1
        try:
            record_scope = "Sudamericano"
            record_type = "Récord Sudamericano" + (" SC" if r["pool"] == "SCM" else "")

            loc_raw = r.get("loc_raw","")
            payload = build_payload(
                record_scope=record_scope,
                record_type=record_type,
                pool=r["pool"],
                gender=r["gender"],
                distance=r["distance"],
                stroke=r["stroke"],
                time_ms=r["time_ms"],
                athlete=r.get("athlete",""),
                athlete_country=r.get("athlete_country",""),
                record_date=r.get("date",""),
                competition_name=r.get("competition","") or "",
                city=r.get("city",""),
                source_name=r.get("source_name", "CONSANAT" if source=="CONSANAT" else "Wikipedia"),
                source_url=r.get("source_url", CONS_NATACION_URL if source=="CONSANAT" else SUDAM_WIKI_URL),
                type_probe=infer_type_probe(r.get("event",""), r.get("athlete",""), r.get("is_relay", False)),
                source_note=json.dumps({"loc_raw": loc_raw}, ensure_ascii=False) if loc_raw else "",
            )

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
            sb.log("ERROR", "SUDAM_ROW", r.get("athlete",""), message=str(e))

    return stats


def run_panam_games(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    try:
        rows = panam_games_wiki()
        print(f"🌎 PANAM_GAMES source=WIKI filas={len(rows)}")
    except Exception as e:
        stats["errors"] += 1
        sb.log("ERROR", "PANAM_GAMES", "", message=str(e))
        return stats

    for r in rows:
        stats["seen"] += 1
        try:
            record_scope = "Panamericano"
            record_type = "Récord Juegos Panamericanos" + (" SC" if r["pool"] == "SCM" else "")

            loc_raw = r.get("loc_raw","")
            payload = build_payload(
                record_scope=record_scope,
                record_type=record_type,
                pool=r["pool"],
                gender=r["gender"],
                distance=r["distance"],
                stroke=r["stroke"],
                time_ms=r["time_ms"],
                athlete=r.get("athlete",""),
                athlete_country=r.get("athlete_country",""),
                record_date=r.get("date",""),
                competition_name=r.get("competition","") or "Pan American Games",
                city=r.get("city",""),
                source_name=r.get("source_name","Wikipedia"),
                source_url=r.get("source_url", PANAM_GAMES_WIKI_URL),
                type_probe=infer_type_probe(r.get("event",""), r.get("athlete",""), r.get("is_relay", False)),
                source_note=json.dumps({"loc_raw": loc_raw}, ensure_ascii=False) if loc_raw else "",
            )

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
            sb.log("ERROR", "PANAM_ROW", r.get("athlete",""), message=str(e))

    return stats


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


def main() -> int:
    try:
        sb = SB(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase init error: {e}")
        return 2

    print(f"MDV_UPDATER_VERSION={MDV_UPDATER_VERSION}")
    print(f"RUN_ID={RUN_ID}")
    print(f"Timestamp (UTC)={RUN_TS}")

    all_stats: Dict[str, Dict[str, int]] = {}

    all_stats["WA"] = run_wa(sb)
    all_stats["SUDAM"] = run_sudam(sb)
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

    # summary log
    try:
        sb.log("RUN", "SUMMARY", "", message=json.dumps(all_stats, ensure_ascii=False))
    except Exception:
        pass

    send_email(f"🏁 MDV Scraper | {RUN_ID} | {MDV_UPDATER_VERSION}", body)

    # Solo WA es fatal (para no romperte Actions cuando CONSANAT cae)
    wa_errors = all_stats["WA"]["errors"]
    return 1 if wa_errors else 0


if __name__ == "__main__":
    sys.exit(main())
