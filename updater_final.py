#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MDV Records Updater (WA + CONSANAT + PanAm Aquatics) — v7 (column mapping fixed)

Qué corrige esta versión (respecto a tu updater actual):
- Deja de escribir columnas inexistentes en `records_standards`:
  * `record_time`            -> usa `time_clock`, `time_clock_2dp`, `time_ms`
  * `competition_location`   -> usa `city`
  * `notes`                  -> usa `source_note`
- Compara y actualiza por `time_ms` (numérico), y sincroniza `time_clock` y `time_clock_2dp`.
- Mantiene el comportamiento “fill”: completa campos vacíos aunque el tiempo no cambie.

Requiere env:
- SUPABASE_URL
- SUPABASE_KEY  (service_role o key con permisos de escritura)

Opcional:
- EMAIL_USER / EMAIL_PASS (email resumen)
- PANAM_AQUATICS_URL
"""

from __future__ import annotations

import os
import re
import sys
import json
import uuid
import shutil
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone, date
from email.mime.text import MIMEText
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from playwright.sync_api import sync_playwright
from supabase import create_client

# (Opcional) .env local. En GitHub Actions no hace falta.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ModuleNotFoundError:
    pass


# -------------------------- Config --------------------------

MDV_UPDATER_VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+CONS+PANAM_v8_CONS_RETRY_PANAM_PLAYWRIGHT")
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
    "im": "Combinado",
}

# -------------------------- Helpers: time/date --------------------------

def parse_time_to_ms(raw: str) -> Optional[int]:
    """Convierte varios formatos a ms.

    Admite:
    - "20.91"
    - "1.41.32"
    - "00:01:41.32"
    - "1:41.32"
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


def format_ms_to_hms(ms: int) -> str:
    """Formato tabla: HH:MM:SS.xx"""
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


def parse_date(raw: Any) -> Optional[str]:
    """Devuelve YYYY-MM-DD si puede."""
    if raw is None:
        return None

    # openpyxl puede devolver date/datetime
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()

    s = str(raw).strip()
    if not s:
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # "YYYY-MM-DD HH:MM:SS"
    mdt = re.match(r"^(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}", s)
    if mdt:
        return mdt.group(1)

    # dd/mm/yyyy
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


def split_city(location: str) -> str:
    """Heurística: 'Doha, Qatar' -> 'Doha'."""
    if not location:
        return ""
    s = str(location).strip()
    if "," in s:
        return s.split(",", 1)[0].strip()
    return s


# -------------------------- Helpers: event parsing --------------------------

EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.IGNORECASE,
)


def parse_event(event_raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Devuelve (distance_m, stroke_es)."""
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
        """Devuelve (status, row). status: inserted|updated|filled|unchanged"""
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

        # Buscar row existente
        q = self.client.table("records_standards").select("*")
        for k in key_fields:
            q = q.eq(k, payload[k])
        existing_resp = q.limit(1).execute()
        existing = (existing_resp.data or [None])[0]

        new_ms = payload.get("time_ms")
        if isinstance(new_ms, str):
            new_ms = parse_time_to_ms(new_ms)
        if new_ms is None:
            # sin tiempo no podemos hacer nada útil
            return "unchanged", existing

        # Insert si no existe
        if not existing:
            insert_payload = dict(payload)
            insert_payload.setdefault("last_updated", RUN_DATE)
            insert_payload.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
            resp = self.client.table("records_standards").insert(insert_payload).execute()
            row = (resp.data or [None])[0]
            return "inserted", row

        old_ms = existing.get("time_ms")
        try:
            old_ms_int = int(old_ms) if old_ms is not None else None
        except Exception:
            old_ms_int = None

        time_changed = (old_ms_int is not None and int(new_ms) != old_ms_int)

        # Fill fields (solo si en DB está vacío)
        fill_fields = [
            "athlete_name",
            "country",
            "record_date",
            "competition_name",
            "city",
            "source_name",
            "source_url",
            "source_note",
            "time_clock",
            "time_clock_2dp",
        ]

        updates: Dict[str, Any] = {}

        for f in fill_fields:
            newv = payload.get(f)
            oldv = existing.get(f)
            if newv is None or str(newv).strip() == "":
                continue
            if oldv is None or str(oldv).strip() == "":
                updates[f] = newv

        # Si cambió el tiempo, sincronizamos los 3 campos de tiempo
        if time_changed:
            updates["time_ms"] = int(new_ms)
            updates["time_clock"] = payload.get("time_clock", format_ms_to_hms(int(new_ms)))
            updates["time_clock_2dp"] = payload.get("time_clock_2dp", payload.get("time_clock", format_ms_to_hms(int(new_ms))))

        if updates:
            updates["last_updated"] = RUN_DATE
            updates["updated_at"] = datetime.now(timezone.utc).isoformat()

            resp = (
                self.client.table("records_standards")
                .update(updates)
                .eq("id", existing["id"])
                .execute()
            )
            row = (resp.data or [None])[0]
            return ("updated" if time_changed else "filled"), row

        return "unchanged", existing

    def log(self, scope: str, prueba: str, atleta: str, t_old: str = "", t_new: str = "") -> None:
        base = {
            "fecha": datetime.now(timezone.utc).isoformat(),
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "tiempo_anterior": t_old,
            "tiempo_nuevo": t_new,
        }
        try:
            self.client.table("scraper_logs").insert(base).execute()
        except Exception:
            # no romper el run por logs
            return


# -------------------------- WA (World Aquatics) --------------------------

@dataclass
class WASpec:
    code: str   # WR, OR, WJ, CR_AMERICAS
    pool: str   # LCM/SCM
    gender: str # M/F


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

    # cookies banner
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

    def norm(v: Any) -> Any:
        if v is None:
            return ""
        return v

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        values = list(ws.values)
        if not values:
            continue

        header_idx = None
        header_map: Dict[str, int] = {}

        for i, row in enumerate(values[:50]):
            row_norm = [str(norm(x)).strip().lower() for x in row]
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
            event = str(norm(row[header_map["event"]])).strip()
            t = str(norm(row[header_map["time"]])).strip()
            if not event or not t:
                continue

            athlete = str(norm(row[header_map.get("athlete", -1)])).strip() if "athlete" in header_map else ""
            country = str(norm(row[header_map.get("country", -1)])).strip() if "country" in header_map else ""
            d_raw = norm(row[header_map.get("date", -1)]) if "date" in header_map else ""
            loc = str(norm(row[header_map.get("location", -1)])).strip() if "location" in header_map else ""
            comp = str(norm(row[header_map.get("competition", -1)])).strip() if "competition" in header_map else ""

            rows_out.append(
                {
                    "event": event,
                    "time": t,
                    "athlete": athlete,
                    "country": country,
                    "date": d_raw,
                    "location": loc,
                    "competition": comp,
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


# -------------------------- CONSANAT --------------------------

CONS_NATACION_URL = "https://consanat.com/records/136/natacion"


def consanat_fetch() -> str:
    """Fetch robusto para CONSANAT (GitHub runners a veces reciben 403/5xx)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            r = requests.get(CONS_NATACION_URL, timeout=60, headers=headers)
            print(f"🌐 CONSANAT fetch status={r.status_code} len={len(r.text)} attempt={attempt}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            wait_s = 3 * attempt
            print(f"⚠️ CONSANAT fetch intento {attempt}/3 falló: {e} (reintento en {wait_s}s)")
            time.sleep(wait_s)
    raise last_err or RuntimeError("CONSANAT fetch failed")


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
                country = rows[i + 3]
                d_raw = rows[i + 4]
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
                        "distance": dist,
                        "stroke": stroke,
                        "time_ms": ms,
                        "athlete": athlete,
                        "country": country,
                        "record_date": parse_date(d_raw) or str(d_raw),
                        "city": split_city(location),
                        "competition_name": comp,
                        "source_url": CONS_NATACION_URL,
                        "source_name": "CONSANAT",
                    }
                )
    return out


# -------------------------- PanAm Aquatics (Panamaquatics) --------------------------

def panam_fetch(url: str) -> str:
    """Fetch simple por requests (puede devolver HTML 'vacío' si el contenido es JS)."""
    r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    print(f"🌐 PANAM fetch status={r.status_code} len={len(r.text)}")
    r.raise_for_status()
    return r.text


def panam_fetch_rendered(url: str) -> str:
    """Fetch renderizado con Playwright (para páginas con contenido inyectado por JS)."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=120_000)
            # banners típicos
            for name in ["Accept", "Aceptar", "I agree", "OK"]:
                try:
                    page.get_by_role("button", name=re.compile(name, re.I)).click(timeout=1500)
                except Exception:
                    pass
            # esperar un poco por render JS
            page.wait_for_timeout(3000)
            html = page.content()
            print(f"🌐 PANAM rendered len={len(html)}")
            return html
        finally:
            browser.close()



def panam_parse(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    tables = soup.find_all("table")
    if tables:
        for t in tables:
            ctx = ""
            prev = t.find_previous(["h1", "h2", "h3", "h4", "h5"])
            if prev:
                ctx = prev.get_text(" ", strip=True).lower()

            gender = "M" if ("men" in ctx or "masc" in ctx) else ("F" if ("women" in ctx or "fem" in ctx) else "")
            pool = "LCM" if ("50" in ctx or "lcm" in ctx or "long" in ctx) else ("SCM" if ("25" in ctx or "scm" in ctx or "short" in ctx) else "")

            rows = t.find_all("tr")
            if not rows:
                continue

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
            c_country = col_idx(["country", "nation", "país", "pais"])
            c_date = col_idx(["date", "fecha"])
            c_comp = col_idx(["competition", "meet", "competición", "competicion"])
            c_loc = col_idx(["location", "place", "local", "venue"])

            for r in rows[1:]:
                cells = [c.get_text(" ", strip=True) for c in r.find_all(["th", "td"])]
                if len(cells) < 3:
                    continue
                event = cells[c_event] if c_event < len(cells) else ""
                t_raw = cells[c_time] if c_time < len(cells) else ""
                athlete = cells[c_ath] if c_ath < len(cells) else ""
                country = cells[c_country] if (c_country is not None and c_country < len(cells)) else ""
                d_raw = cells[c_date] if (c_date is not None and c_date < len(cells)) else ""
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
                        "distance": dist,
                        "stroke": stroke,
                        "time_ms": ms,
                        "athlete": athlete,
                        "country": country,
                        "record_date": parse_date(d_raw) or str(d_raw),
                        "city": split_city(loc),
                        "competition_name": comp,
                        "source_url": source_url,
                        "source_name": "PanAm Aquatics",
                    }
                )
        return out

    # fallback texto plano
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    gender = "M"
    pool = "LCM"

    for idx, ln in enumerate(lines):
        lo = ln.lower()
        if any(k in lo for k in ["women", "femen", "fem", "damas"]):
            gender = "F"
        if any(k in lo for k in ["men", "masc", "varones", "hombres"]):
            gender = "M"
        if "25" in lo and ("pool" in lo or "scm" in lo or "short" in lo):
            pool = "SCM"
        if "50" in lo and ("pool" in lo or "lcm" in lo or "long" in lo):
            pool = "LCM"

        dist, stroke = parse_event(ln)
        if not dist or not stroke:
            continue
        if idx + 1 >= len(lines):
            continue
        t_raw = lines[idx + 1]
        ms = parse_time_to_ms(t_raw)
        if ms is None:
            continue

        out.append(
            {
                "pool": pool,
                "gender": gender,
                "distance": dist,
                "stroke": stroke,
                "time_ms": ms,
                "athlete": "",
                "country": "",
                "record_date": "",
                "city": "",
                "competition_name": "",
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
    competition_name: str,
    location: str,
    source_name: str,
    source_url: str,
) -> Dict[str, Any]:
    time_ms_int = int(time_ms)
    clock = format_ms_to_hms(time_ms_int)
    return {
        "record_scope": record_scope,
        "record_type": record_type,
        "category": "Open",
        "pool_length": pool_label(pool),
        "gender": gender_label(gender),
        "distance": int(distance),
        "stroke": stroke,
        "time_clock": clock,
        "time_clock_2dp": clock,
        "time_ms": time_ms_int,
        "athlete_name": athlete or "",
        "country": country or "",
        "record_date": record_date or "",
        "competition_name": competition_name or "",
        "city": split_city(location),
        "source_name": source_name or "",
        "source_url": source_url or "",
        "source_note": "",
        "is_active": True,
        # last_updated / updated_at los setea SB.upsert_record si faltan
    }


# -------------------------- Runners --------------------------

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
                        record_date=parse_date(r.get("date")) or str(r.get("date") or ""),
                        competition_name=r.get("competition", ""),
                        location=r.get("location", ""),
                        source_name="World Aquatics",
                        source_url=url,
                    )

                    status, row = sb.upsert_record(payload)
                    prueba = f"{spec.gender} {dist}m {stroke} ({spec.pool})"
                    if status == "inserted":
                        stats["inserted"] += 1
                        sb.log(record_scope, prueba, payload.get("athlete_name", ""), "", payload["time_clock"])
                    elif status == "updated":
                        stats["updated"] += 1
                        old = format_ms_to_hms(int(row.get("time_ms"))) if row and row.get("time_ms") else "(changed)"
                        sb.log(record_scope, prueba, payload.get("athlete_name", ""), old, payload["time_clock"])
                    elif status == "filled":
                        stats["filled"] += 1
                        sb.log(record_scope, prueba, payload.get("athlete_name", ""), "(fill)", payload["time_clock"])
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
                record_date=r.get("record_date", ""),
                competition_name=r.get("competition_name", ""),
                location=r.get("city", ""),
                source_name=r.get("source_name", "CONSANAT"),
                source_url=r.get("source_url", CONS_NATACION_URL),
            )

            status, _ = sb.upsert_record(payload)
            prueba = f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})"
            if status == "inserted":
                stats["inserted"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "", payload["time_clock"])
            elif status == "updated":
                stats["updated"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "(changed)", payload["time_clock"])
            elif status == "filled":
                stats["filled"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "(fill)", payload["time_clock"])
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
        if not rows:
            print('⚠️ PANAM: 0 filas con requests; intento renderizado Playwright…')
            html2 = panam_fetch_rendered(PANAM_AQUATICS_URL)
            rows = panam_parse(html2, PANAM_AQUATICS_URL)
        if not rows:
            raise RuntimeError('PANAM: 0 filas parseadas (probable contenido embebido/PDF/JS)')

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
                record_date=r.get("record_date", ""),
                competition_name=r.get("competition_name", ""),
                location=r.get("city", ""),
                source_name=r.get("source_name", "PanAm Aquatics"),
                source_url=r.get("source_url", PANAM_AQUATICS_URL),
            )

            status, _ = sb.upsert_record(payload)
            prueba = f"{r['gender']} {r['distance']}m {r['stroke']} ({r['pool']})"
            if status == "inserted":
                stats["inserted"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "", payload["time_clock"])
            elif status == "updated":
                stats["updated"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "(changed)", payload["time_clock"])
            elif status == "filled":
                stats["filled"] += 1
                sb.log(record_scope, prueba, payload.get("athlete_name", ""), "(fill)", payload["time_clock"])
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
    all_stats["WA"] = run_wa(sb)
    all_stats["CONSANAT"] = run_consanat(sb)
    all_stats["PANAM"] = run_panam(sb)

    lines = [
        f"Version: {MDV_UPDATER_VERSION}",
        f"Run ID: {RUN_ID}",
        f"Timestamp (UTC): {RUN_TS}",
        "",
    ]
    for k, st in all_stats.items():
        lines.append(
            f"[{k}] seen={st['seen']} | inserted={st['inserted']} | updated={st['updated']} | filled={st['filled']} | "
            f"unchanged={st['unchanged']} | skipped={st['skipped']} | errors={st['errors']}"
        )

    body = "\n".join(lines)
    print(body)

    try:
        sb.log("RUN", "SUMMARY", json.dumps(all_stats, ensure_ascii=False))
    except Exception:
        pass

    send_email(f"🏁 MDV Scraper | {RUN_ID} | {MDV_UPDATER_VERSION}", body)
    total_errors = sum(st["errors"] for st in all_stats.values())
    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
