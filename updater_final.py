#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MDV Records Updater (WA + CONSANAT + PanAm Aquatics)

 Objetivo
- Scrapea récords desde fuentes oficiales.
- Hace UPSERT en Supabase (records_standards):
  * actualiza tiempos si cambiaron
  * inserta récords faltantes
  * completa campos incompletos aunque el tiempo no cambie
- Registra un log editable en scraper_logs.

Variables de entorno requeridas:
- SUPABASE_URL
- SUPABASE_KEY  (service_role o key con permisos de escritura)

Opcionales:
- EMAIL_USER / EMAIL_PASS (si quieres email resumen)
- PANAM_AQUATICS_URL (por defecto usa el customPage que pasaste)

Notas importantes
- World Aquatics se descarga vía XLSX (requiere Playwright).
- CONSANAT se parsea desde HTML.
- PanAm Aquatics: el parser es tolerante, pero si el HTML cambia o viene embebido, puede requerir ajustes.
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

# (Opcional) Carga variables desde .env cuando corres local.
# En GitHub Actions no hace falta porque usamos secrets/env.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except ModuleNotFoundError:
    pass


# -------------------------- Config --------------------------

MDV_UPDATER_VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+CONS+PANAM_v5")
RUN_ID = os.getenv("GITHUB_RUN_ID") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASS = os.getenv("EMAIL_PASS", "").strip()

PANAM_AQUATICS_URL = os.getenv(
    "PANAM_AQUATICS_URL",
    "https://www.panamaquatics.com/customPage/ed0bf338-6fab-4b35-abc0-418e0db1749e",
).strip()

# Preferimos nombres en español, compatibles con tu tabla.
STROKE_MAP = {
    "freestyle": "Libre",
    "backstroke": "Espalda",
    "breaststroke": "Pecho",
    "butterfly": "Mariposa",
    "medley": "Combinado",

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

    # Normaliza separadores raros
    s = re.sub(r"\s+", "", s)

    # Formato con ':'
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

    # Formato con '.' (puede ser seg.cent o min.seg.cent o h.min.seg.cent)
    # Ej: 23.76 / 1.54.50 / 15.48.32
    dot_parts = s.split(".")
    try:
        if len(dot_parts) == 2:
            sec = float(s)
            return int(round(sec * 1000))
        if len(dot_parts) == 3:
            mm = int(dot_parts[0])
            ss = int(dot_parts[1])
            cc = int(dot_parts[2])
            return (mm * 60 + ss) * 1000 + int(round(cc * 10))
        if len(dot_parts) == 4:
            hh = int(dot_parts[0])
            mm = int(dot_parts[1])
            ss = int(dot_parts[2])
            cc = int(dot_parts[3])
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
    # ya viene ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # dd/mm/yyyy o d/m/yy
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

    return None


# -------------------------- Helpers: event parsing --------------------------

EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.IGNORECASE,
)


def parse_event(event_raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Devuelve (distance_m, stroke_es)"""
    if not event_raw:
        return None, None
    s = str(event_raw).strip().lower()

    # Normaliza sinónimos
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

    def fetch_records(self) -> List[Dict[str, Any]]:
        # Trae todo para hacer match local (es más simple que queries por evento)
        out = []
        page = 0
        page_size = 1000
        while True:
            start = page * page_size
            end = start + page_size - 1
            resp = (
                self.client.table("records_standards")
                .select("*")
                .range(start, end)
                .execute()
            )
            data = resp.data or []
            out.extend(data)
            if len(data) < page_size:
                break
            page += 1
        return out

    def upsert_record(self, payload: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Devuelve (status, db_row). status in: inserted|updated|filled|unchanged"""
        # Identidad del récord (llave compuesta en tu modelo)
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

        # Buscamos row existente
        q = self.client.table("records_standards").select("*")
        for k in key_fields:
            q = q.eq(k, payload[k])
        existing_resp = q.limit(1).execute()
        existing = (existing_resp.data or [None])[0]

        # Normaliza time
        new_ms = parse_time_to_ms(payload.get("record_time", ""))

        # Si no existe: insert
        if not existing:
            insert_payload = dict(payload)
            insert_payload["last_updated"] = RUN_TS
            resp = self.client.table("records_standards").insert(insert_payload).execute()
            row = (resp.data or [None])[0]
            return "inserted", row

        # Si existe: actualiza si cambió tiempo o si hay campos faltantes
        old_ms = parse_time_to_ms(existing.get("record_time") or "")
        time_changed = (new_ms is not None and old_ms is not None and new_ms != old_ms)

        # Completar campos incompletos (aunque el tiempo no cambie)
        fill_fields = [
            "athlete_name",
            "country",
            "record_date",
            "competition_name",
            "competition_location",
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

        # Si cambió tiempo, actualizamos record_time sí o sí
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
            if time_changed:
                return "updated", row
            return "filled", row

        return "unchanged", existing

    def log(self, scope: str, prueba: str, atleta: str, t_old: str = "", t_new: str = "") -> None:
        """Inserta en scraper_logs (tolerante a schema)."""
        base = {
            "fecha": RUN_TS,
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "tiempo_anterior": t_old,
            "tiempo_nuevo": t_new,
        }

        # Intento 1: schema base
        try:
            self.client.table("scraper_logs").insert(base).execute()
            return
        except Exception:
            pass

        # Intento 2: si la tabla tiene columna "message" o similar
        try:
            alt = dict(base)
            alt["message"] = atleta
            self.client.table("scraper_logs").insert(alt).execute()
            return
        except Exception:
            # No matamos el run por logs
            return


# -------------------------- WA (World Aquatics) --------------------------

@dataclass
class WASpec:
    code: str  # WR, OR, WJ, CR_AMERICAS
    pool: str  # LCM/SCM
    gender: str  # M/F


def wa_url(spec: WASpec) -> str:
    """Construye URL. OJO: WA usa varios parámetros; mantenemos lo mínimo para que XLSX sea consistente."""
    base = "https://www.worldaquatics.com/swimming/records"

    # World Records
    if spec.code == "WR":
        return f"{base}?recordType=WR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"

    # Olympic Records
    if spec.code == "OR":
        return f"{base}?recordType=OR&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"

    # World Junior Records (recordCode)
    if spec.code == "WJ":
        return f"{base}?recordCode=WJ&eventTypeId=&region=&countryId=&gender={spec.gender}&pool={spec.pool}"

    # Continental records: Americas
    if spec.code == "CR_AMERICAS":
        # En WA: recordType=PAN + recordCode=CR + region=AMERICAS
        return f"{base}?recordType=PAN&recordCode=CR&eventTypeId=&region=AMERICAS&countryId=&gender={spec.gender}&pool={spec.pool}"

    raise ValueError(f"Unknown WA code {spec.code}")


def wa_download_xlsx(page, url: str, out_dir: str) -> str:
    """Navega y baja el XLSX. Devuelve path."""
    page.goto(url, wait_until="networkidle", timeout=120_000)

    # Cookies banner (si aparece)
    try:
        page.get_by_role("button", name=re.compile(r"Accept Cookies", re.I)).click(timeout=3000)
    except Exception:
        pass

    # El link suele ser 'XLSX'
    with page.expect_download(timeout=120_000) as dl_info:
        try:
            page.get_by_role("link", name=re.compile(r"XLSX", re.I)).click(timeout=10_000)
        except Exception:
            # fallback: 'Download Records'
            page.get_by_role("link", name=re.compile(r"Download Records", re.I)).click(timeout=10_000)

    download = dl_info.value
    filename = download.suggested_filename or f"wa_{uuid.uuid4().hex}.xlsx"
    path = os.path.join(out_dir, filename)
    download.save_as(path)
    return path


def wa_parse_xlsx(xlsx_path: str) -> List[Dict[str, Any]]:
    """Devuelve lista de dicts con campos base: event, time, athlete, country, date, location, competition."""
    wb = load_workbook(xlsx_path, data_only=True)
    rows_out: List[Dict[str, Any]] = []

    def norm(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    # Recorremos sheets
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        values = list(ws.values)
        if not values:
            continue

        # Buscar header row (Event/Time/Athlete)
        header_idx = None
        header_map: Dict[str, int] = {}
        for i, row in enumerate(values[:50]):
            row_norm = [norm(x).lower() for x in row]
            if any("event" in c for c in row_norm) and any("time" in c for c in row_norm):
                header_idx = i
                # mapeo
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
            # No sheet utilizable
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
                {
                    "event": event,
                    "time": t,
                    "athlete": athlete,
                    "country": country,
                    "date": date,
                    "location": location,
                    "competition": competition,
                }
            )

    return rows_out


def wa_specs() -> List[WASpec]:
    # Importante: eliminamos PAN y SAM como "récord regional" de WA.
    # Solo dejamos: WR, OR, WJ y Continental Americas (como CR_AMERICAS).
    out: List[WASpec] = []
    for gender in ("M", "F"):
        for pool in ("LCM", "SCM"):
            out.append(WASpec("WR", pool, gender))
            out.append(WASpec("WJ", pool, gender))
            out.append(WASpec("CR_AMERICAS", pool, gender))
        # OR solo en LCM
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
        # OJO: NO es "Panamericano".
        return "Américas", "Récord Continental Américas" + (" SC" if is_scm else "")

    raise ValueError(code)


# -------------------------- CONSANAT --------------------------

CONS_NATACION_URL = "https://consanat.com/records/136/natacion"


def consanat_fetch() -> str:
    r = requests.get(CONS_NATACION_URL, timeout=60)
    r.raise_for_status()
    return r.text


def consanat_parse(html: str) -> List[Dict[str, Any]]:
    """Parsea el texto en bloques de 7 campos."""
    soup = BeautifulSoup(html, "html.parser")

    # Extraemos texto visible, compacto
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Localizamos secciones por piscina
    def find_idx(pattern: str) -> Optional[int]:
        for i, ln in enumerate(lines):
            if pattern.lower() in ln.lower():
                return i
        return None

    out: List[Dict[str, Any]] = []

    # Determina pool por header
    sections: List[Tuple[str, int, int]] = []
    idx_scm = find_idx("RECORDS SUDAMERICANOS DE PISCINA CORTA")
    idx_lcm = find_idx("RECORDS SUDAMERICANOS DE PISCINA LARGA")

    if idx_scm is not None:
        sections.append(("SCM", idx_scm, idx_lcm if idx_lcm is not None else len(lines)))
    if idx_lcm is not None:
        sections.append(("LCM", idx_lcm, len(lines)))

    for pool, start, end in sections:
        chunk = lines[start:end]

        # Dentro del chunk: "FEMININO" y "MASCULINO"
        # Recorremos ambas por separado
        for gender_word, gender in (("FEMININO", "F"), ("MASCULINO", "M")):
            try:
                g_start = next(i for i, ln in enumerate(chunk) if ln.upper() == gender_word)
            except StopIteration:
                continue
            # fin en el próximo gender o fin
            g_end = None
            for i in range(g_start + 1, len(chunk)):
                if chunk[i].upper() in ("FEMININO", "MASCULINO"):
                    g_end = i
                    break
            if g_end is None:
                g_end = len(chunk)

            g_lines = chunk[g_start:g_end]

            # Encontrar header fila "PRUEBAS"...
            try:
                h = next(i for i, ln in enumerate(g_lines) if ln.upper() == "PRUEBAS")
            except StopIteration:
                continue

            data = g_lines[h:]

            # la cabecera tiene 7 labels
            # PRUEBAS TIEMPO RECORDISTA PAÍS FECHA LOCAL COMPETICIÓN
            # Saltamos hasta después de COMPETICIÓN
            try:
                header_end = next(i for i, ln in enumerate(data) if "COMPET" in ln.upper())
            except StopIteration:
                continue

            rows = data[header_end + 1 :]

            # Parse en chunks de 7
            # Algunos valores pueden venir pegados (ej: "São Paulo, Brasil"). Igual cuenta como 1.
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

                out.append(
                    {
                        "pool": pool,
                        "gender": gender,
                        "event": event,
                        "distance": dist,
                        "stroke": stroke,
                        "time_ms": ms,
                        "athlete": athlete,
                        "country": country,
                        "date": parse_date(date) or date,
                        "location": location,
                        "competition": comp,
                        "source_url": CONS_NATACION_URL,
                        "source_name": "CONSANAT",
                    }
                )

    return out


# -------------------------- PanAm Aquatics (Panamaquatics) --------------------------


def panam_fetch(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def panam_parse(html: str, source_url: str) -> List[Dict[str, Any]]:
    """Parser genérico para páginas con tablas o listas.

    Estrategia:
    - Intentar read_html implícito (no usamos pandas acá) no disponible.
    - Buscar tablas <table> y si existen, parsear filas.
    - Si no hay tablas, caer al texto plano y buscar patrones evento+tiempo.

    IMPORTANTE: Este parser puede necesitar ajuste fino cuando veamos el HTML real.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    # 1) Tablas
    tables = soup.find_all("table")
    if tables:
        # Intentamos inferir género/pool desde títulos cercanos
        for t in tables:
            # contexto
            ctx_text = " ".join(
                [
                    (t.find_previous(["h1", "h2", "h3", "h4", "h5"]) or {}).get_text(" ", strip=True)
                    if t.find_previous(["h1", "h2", "h3", "h4", "h5"])
                    else ""
                ]
            ).lower()
            gender = "M" if "men" in ctx_text or "masc" in ctx_text else ("F" if "women" in ctx_text or "fem" in ctx_text else "")
            pool = "LCM" if "50" in ctx_text or "lcm" in ctx_text or "long" in ctx_text else ("SCM" if "25" in ctx_text or "scm" in ctx_text or "short" in ctx_text else "")

            rows = t.find_all("tr")
            if not rows:
                continue

            # header
            header_cells = [c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th", "td"])]

            def col_idx(keys: Iterable[str]) -> Optional[int]:
                for k in keys:
                    for i, c in enumerate(header_cells):
                        if k in c:
                            return i
                return None

            c_event = col_idx(["event", "prueba"]) or 0
            c_time = col_idx(["time", "tiempo", "mark"]) or 1
            c_ath = col_idx(["athlete", "swimmer", "recordist", "recordista"]) or 2
            c_country = col_idx(["country", "nation", "país", "pais"])  # opcional
            c_date = col_idx(["date", "fecha"])  # opcional
            c_comp = col_idx(["competition", "meet", "competición", "competicion"])  # opcional
            c_loc = col_idx(["location", "place", "local", "venue"])  # opcional

            for r in rows[1:]:
                cells = [c.get_text(" ", strip=True) for c in r.find_all(["th", "td"])]
                if len(cells) < 3:
                    continue
                event = cells[c_event] if c_event < len(cells) else ""
                t_raw = cells[c_time] if c_time < len(cells) else ""
                athlete = cells[c_ath] if c_ath < len(cells) else ""
                country = cells[c_country] if (c_country is not None and c_country < len(cells)) else ""
                date = cells[c_date] if (c_date is not None and c_date < len(cells)) else ""
                comp = cells[c_comp] if (c_comp is not None and c_comp < len(cells)) else ""
                loc = cells[c_loc] if (c_loc is not None and c_loc < len(cells)) else ""

                dist, stroke = parse_event(event)
                ms = parse_time_to_ms(t_raw)
                if not dist or not stroke or ms is None:
                    continue

                out.append(
                    {
                        "pool": pool or "LCM",
                        "gender": gender or "M",
                        "event": event,
                        "distance": dist,
                        "stroke": stroke,
                        "time_ms": ms,
                        "athlete": athlete,
                        "country": country,
                        "date": parse_date(date) or date,
                        "location": loc,
                        "competition": comp,
                        "source_url": source_url,
                        "source_name": "PanAm Aquatics",
                    }
                )

        return out

    # 2) Fallback texto plano: busca secuencias event + time
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Heurística: si aparece "Men"/"Women" y "50m"/"25m" en headings, ir actualizando contexto
    gender = "M"
    pool = "LCM"

    for ln in lines:
        lo = ln.lower()
        if any(k in lo for k in ["women", "femen", "fem", "damas"]):
            gender = "F"
        if any(k in lo for k in ["men", "masc", "varones", "hombres"]):
            gender = "M"
        if "25" in lo and ("pool" in lo or "scm" in lo or "short" in lo):
            pool = "SCM"
        if "50" in lo and ("pool" in lo or "lcm" in lo or "long" in lo):
            pool = "LCM"

        # intenta detectar un evento
        dist, stroke = parse_event(ln)
        if not dist or not stroke:
            continue

        # buscar tiempo en la próxima línea (heurístico)
        # time candidates: 20.91 / 1.54.50 / 00:01:54.50
        # OJO: no queremos parsear fechas
        #
        # Esta parte no es perfecta sin ver el HTML real.
        idx = lines.index(ln)
        if idx + 1 >= len(lines):
            continue
        t_raw = lines[idx + 1]
        if parse_time_to_ms(t_raw) is None:
            continue

        out.append(
            {
                "pool": pool,
                "gender": gender,
                "event": ln,
                "distance": dist,
                "stroke": stroke,
                "time_ms": parse_time_to_ms(t_raw),
                "athlete": "",
                "country": "",
                "date": "",
                "location": "",
                "competition": "",
                "source_url": source_url,
                "source_name": "PanAm Aquatics",
            }
        )

    return out


# -------------------------- Build payloads --------------------------


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
        "competition_location": comp_location or "",
        "source_name": source_name or "",
        "source_url": source_url or "",
        "notes": "",
    }


# -------------------------- Main --------------------------


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

                    # para log: buscamos old time en DB si existe
                    status, row = sb.upsert_record(payload)
                    if status == "inserted":
                        stats["inserted"] += 1
                        sb.log(record_scope, f"{spec.gender} {dist}m {stroke} ({spec.pool})", payload.get("athlete_name", ""), "", payload["record_time"])
                    elif status == "updated":
                        stats["updated"] += 1
                        sb.log(record_scope, f"{spec.gender} {dist}m {stroke} ({spec.pool})", payload.get("athlete_name", ""), "(changed)", payload["record_time"])
                    elif status == "filled":
                        stats["filled"] += 1
                        # no spam: log solo si faltaba data clave
                        sb.log(record_scope, f"{spec.gender} {dist}m {stroke} ({spec.pool})", payload.get("athlete_name", ""), "(fill)", payload["record_time"])
                    else:
                        stats["unchanged"] += 1

            except Exception as e:
                stats["errors"] += 1
                err = f"WA {spec.code} {spec.pool} {spec.gender} error: {e}"
                print("❌", err)
                sb.log("ERROR", f"WA {spec.code} {spec.pool} {spec.gender}", err)
                continue

        browser.close()

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return stats


def run_consanat(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    try:
        html = consanat_fetch()
        rows = consanat_parse(html)
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
                comp_name=r.get("competition", ""),
                comp_location=r.get("location", ""),
                source_name=r.get("source_name", "CONSANAT"),
                source_url=r.get("source_url", CONS_NATACION_URL),
            )

            status, _ = sb.upsert_record(payload)
            if status == "inserted":
                stats["inserted"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "", payload["record_time"])
            elif status == "updated":
                stats["updated"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "(changed)", payload["record_time"])
            elif status == "filled":
                stats["filled"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "(fill)", payload["record_time"])
            else:
                stats["unchanged"] += 1

    except Exception as e:
        stats["errors"] += 1
        sb.log("ERROR", "CONSANAT", f"{e}")

    return stats


def run_panam(sb: SB) -> Dict[str, int]:
    stats = {"seen": 0, "updated": 0, "inserted": 0, "filled": 0, "unchanged": 0, "skipped": 0, "errors": 0}

    try:
        html = panam_fetch(PANAM_AQUATICS_URL)
        rows = panam_parse(html, PANAM_AQUATICS_URL)

        for r in rows:
            stats["seen"] += 1

            record_scope = "Panamericano"
            record_type = "Récord Panamericano" + (" SC" if r["pool"] == "SCM" else "")

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
                comp_name=r.get("competition", ""),
                comp_location=r.get("location", ""),
                source_name=r.get("source_name", "PanAm Aquatics"),
                source_url=r.get("source_url", PANAM_AQUATICS_URL),
            )

            status, _ = sb.upsert_record(payload)
            if status == "inserted":
                stats["inserted"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "", payload["record_time"])
            elif status == "updated":
                stats["updated"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "(changed)", payload["record_time"])
            elif status == "filled":
                stats["filled"] += 1
                sb.log(record_scope, f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})", payload.get("athlete_name", ""), "(fill)", payload["record_time"])
            else:
                stats["unchanged"] += 1

    except Exception as e:
        stats["errors"] += 1
        sb.log("ERROR", "PANAM", f"{e}")

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

    all_stats: Dict[str, Dict[str, int]] = {}

    # 1) World Aquatics
    all_stats["WA"] = run_wa(sb)

    # 2) CONSANAT (Sudamericano)
    all_stats["CONSANAT"] = run_consanat(sb)

    # 3) PanAm Aquatics (Panamericano)
    all_stats["PANAM"] = run_panam(sb)

    # Summary
    lines = [
        f"Version: {MDV_UPDATER_VERSION}",
        f"Run ID: {RUN_ID}",
        f"Timestamp (UTC): {RUN_TS}",
        "",
    ]

    for k, st in all_stats.items():
        lines.append(f"[{k}] seen={st['seen']} | inserted={st['inserted']} | updated={st['updated']} | filled={st['filled']} | unchanged={st['unchanged']} | skipped={st['skipped']} | errors={st['errors']}")

    body = "\n".join(lines)
    print(body)

    # Log summary row
    try:
        sb.log("RUN", "SUMMARY", f"{json.dumps(all_stats, ensure_ascii=False)}")
    except Exception:
        pass

    send_email(f"🏁 MDV Scraper | {RUN_ID} | {MDV_UPDATER_VERSION}", body)

    # exit code si hubo errores
    total_errors = sum(st["errors"] for st in all_stats.values())
    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
