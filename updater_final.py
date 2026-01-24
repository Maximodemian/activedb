#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MDV Sports - Records Updater (GitHub Actions)
---------------------------------------------
Objetivo:
- Scrappear fuentes oficiales de récords (World Aquatics, CONSANAT, etc.)
- Hacer UPSERT en public.records_standards (actualiza si existe, inserta si falta)
- Completar campos faltantes aunque el tiempo no haya cambiado
- Registrar cambios (y opcionalmente inserts) en public.scraper_logs
- (Opcional) enviar mail resumen (Gmail SMTP) si EMAIL_USER/EMAIL_PASS están configurados.

NOTA IMPORTANTE:
- Este script NO imprime secrets.
- Todo lo "inestable" (selectores, columnas exactas de XLSX/PDF) se maneja con heurísticas y try/except.
"""

import os
import re
import json
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from supabase import create_client, Client

# Fuentes extra
from bs4 import BeautifulSoup

# Playwright para descargas (WA / páginas con JS / Cloudflare)
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# PDF parsing (para IPC SDMS, si se habilita)
import pdfplumber

MDV_UPDATER_VERSION = "MDV_UPDATER_VERSION=WA+CONSANAT+IPC_STUB_v5_2026-01-22"

# =========================
# Helpers (env, time, logs)
# =========================

def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return str(val) if val is not None else ""

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_gender(g: str) -> str:
    g = (g or "").strip().upper()
    if g in ("M", "MEN", "MALE", "H", "HOMBRE", "HOMBRES"):
        return "M"
    if g in ("W", "F", "WOMEN", "FEMALE", "MUJER", "MUJERES"):
        return "F"
    return g[:1] if g else ""

STROKE_MAP = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "MEDLEY": "Combinado",
    "INDIVIDUAL MEDLEY": "Combinado",
    "IM": "Combinado",
}

def parse_stroke(s: str) -> str:
    s0 = norm_space(s).upper()
    return STROKE_MAP.get(s0, s.title() if s else "")

def is_relay_event(event_name: str) -> bool:
    e = (event_name or "").lower()
    return ("relay" in e) or ("4x" in e) or ("medley relay" in e)

def parse_distance(event_name: str) -> Optional[int]:
    """
    Extrae distancia en metros de textos tipo:
    - "Men 200m Freestyle"
    - "200m Butterfly"
    - "50 m LIBRE"
    """
    m = re.search(r"(\d{2,4})\s*m", event_name.replace(" ", "").lower())
    if not m:
        m = re.search(r"(\d{2,4})\s*m", event_name.lower())
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def time_clock_to_ms(clock: str) -> Optional[int]:
    """
    Acepta:
      00:01:52.69
      1:52.69
      52.69
      00:52.69
      1.30.51   (CONSANAT / formato con puntos => 1:30.51)
      14.48.53  (=> 14:48.53)
    """
    c = (clock or "").strip()
    if not c:
        return None

    # normalizar separadores decimales
    c = c.replace(",", ".")

    # Si viene con puntos como separador de min/seg/centésimas: m.ss.cc o mm.ss.cc
    # Ej: 1.30.51 -> 1:30.51
    if ":" not in c and re.fullmatch(r"\d{1,3}\.\d{2}\.\d{2}", c):
        a, b, h = c.split(".")
        c = f"{a}:{b}.{h}"

    # Si viene con puntos como separador de hora/min/seg/centésimas: h.mm.ss.cc (raro)
    if ":" not in c and re.fullmatch(r"\d{1,2}\.\d{2}\.\d{2}\.\d{2}", c):
        hh, mm, ss, cc = c.split(".")
        c = f"{hh}:{mm}:{ss}.{cc}"

    parts = c.split(":")
    try:
        if len(parts) == 3:
            hh = int(parts[0]); mm = int(parts[1]); ss = float(parts[2])
            return int(round((hh*3600 + mm*60 + ss) * 1000))
        if len(parts) == 2:
            mm = int(parts[0]); ss = float(parts[1])
            return int(round((mm*60 + ss) * 1000))
        # solo segundos
        ss = float(parts[0])
        return int(round(ss * 1000))
    except:
        return None

def ms_to_time_clock_2dp(ms: int) -> str:
    if ms is None:
        return ""
    total_sec = ms / 1000.0
    hh = int(total_sec // 3600)
    rem = total_sec - hh*3600
    mm = int(rem // 60)
    ss = rem - mm*60
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:05.2f}"
    if mm > 0:
        return f"{mm:02d}:{ss:05.2f}"
    return f"{ss:.2f}"

def parse_date_any(s: str) -> Optional[str]:
    """
    Devuelve YYYY-MM-DD si se puede.
    """
    s = (s or "").strip()
    if not s:
        return None
    # formatos comunes
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%y", "%d-%m-%y", "%d %b %Y", "%d %B %Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except:
            pass
    # fallback: buscar YYYY-MM-DD dentro
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def supa_client() -> Client:
    url = env("SUPABASE_URL", required=True)
    key = env("SUPABASE_KEY", required=True)
    return create_client(url, key)

def insert_scraper_log(sb: Client, scope: str, prueba: str, atleta: str,
                      t_old: str, t_new: str) -> None:
    try:
        sb.table("scraper_logs").insert({
            "fecha": utc_now_iso(),
            "scope": scope,
            "prueba": prueba,
            "atleta": atleta,
            "tiempo_anterior": t_old,
            "tiempo_nuevo": t_new,
        }).execute()
    except Exception:
        # no frenamos el run si falla logging
        pass

# =========================
# Modelo de registro target
# =========================

@dataclass
class RecordRow:
    record_scope: str
    record_type: str
    category: str
    pool_length: str
    gender: str
    distance: int
    stroke: str

    time_clock: str = ""
    time_ms: Optional[int] = None
    record_date: Optional[str] = None
    competition_name: str = ""
    athlete_name: str = ""
    city: str = ""
    country: str = ""
    last_updated: Optional[str] = None

    source_name: str = ""
    source_url: str = ""
    source_note: str = ""
    verified: bool = True
    is_active: bool = True

    def key_filter(self) -> Dict[str, Any]:
        return {
            "record_scope": self.record_scope,
            "record_type": self.record_type,
            "category": self.category,
            "pool_length": self.pool_length,
            "gender": self.gender,
            "distance": self.distance,
            "stroke": self.stroke,
        }

    def to_insert_payload(self) -> Dict[str, Any]:
        payload = asdict(self)
        # columnas calculadas
        if self.time_ms is not None:
            payload["time_clock_2dp"] = ms_to_time_clock_2dp(self.time_ms)
        else:
            payload["time_clock_2dp"] = ""
        payload["updated_at"] = utc_now_iso()
        return payload

    def to_update_payload(self) -> Dict[str, Any]:
        # Solo campos "datos" (no clave)
        payload = {
            "time_clock": self.time_clock,
            "time_ms": self.time_ms,
            "time_clock_2dp": ms_to_time_clock_2dp(self.time_ms) if self.time_ms is not None else "",
            "record_date": self.record_date,
            "competition_name": self.competition_name,
            "athlete_name": self.athlete_name,
            "city": self.city,
            "country": self.country,
            "last_updated": self.last_updated,
            "source_name": self.source_name,
            "source_url": self.source_url,
            "source_note": self.source_note,
            "verified": self.verified,
            "is_active": self.is_active,
            "updated_at": utc_now_iso(),
        }
        return payload

# =========================
# Upsert core
# =========================

def fetch_existing_by_key(sb: Client, key_filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    q = sb.table("records_standards").select("*")
    for k, v in key_filter.items():
        q = q.eq(k, v)
    res = q.limit(1).execute()
    data = res.data or []
    return data[0] if data else None

def upsert_one(sb: Client, rec: RecordRow, *, log_inserts: bool = False) -> Tuple[str, Optional[str]]:
    """
    Returns: (action, message)
      action in {"INSERT","UPDATE","FILL","SKIP","ERROR"}
    """
    try:
        key = rec.key_filter()
        existing = fetch_existing_by_key(sb, key)

        # Insert si no existe
        if not existing:
            sb.table("records_standards").insert(rec.to_insert_payload()).execute()
            if log_inserts:
                insert_scraper_log(
                    sb,
                    rec.record_scope,
                    f"{rec.gender} {rec.distance}m {rec.stroke} {rec.pool_length}",
                    rec.athlete_name or "(sin atleta)",
                    "",
                    rec.time_clock or ""
                )
            return ("INSERT", "new row")

        # Si existe: decidir si hay update de tiempo y/o completar campos faltantes
        existing_time_ms = existing.get("time_ms")
        existing_time_clock = safe_str(existing.get("time_clock"))
        existing_athlete = safe_str(existing.get("athlete_name"))
        existing_comp = safe_str(existing.get("competition_name"))
        existing_date = safe_str(existing.get("record_date"))
        existing_city = safe_str(existing.get("city"))
        existing_country = safe_str(existing.get("country"))

        changed_time = (rec.time_ms is not None and existing_time_ms is not None and int(rec.time_ms) != int(existing_time_ms)) \
                       or (rec.time_ms is not None and existing_time_ms is None and rec.time_clock) \
                       or (rec.time_ms is None and rec.time_clock and existing_time_clock and rec.time_clock != existing_time_clock)

        # Completar si faltan datos
        def is_blank(x: str) -> bool:
            return (x or "").strip() == ""

        need_fill = (
            (is_blank(existing_athlete) and not is_blank(rec.athlete_name)) or
            (is_blank(existing_comp) and not is_blank(rec.competition_name)) or
            (is_blank(existing_date) and rec.record_date) or
            (is_blank(existing_city) and not is_blank(rec.city)) or
            (is_blank(existing_country) and not is_blank(rec.country)) or
            (existing.get("source_url") in (None, "", "null") and rec.source_url) or
            (existing.get("source_name") in (None, "", "null") and rec.source_name)
        )

        if not changed_time and not need_fill:
            return ("SKIP", None)

        sb.table("records_standards").update(rec.to_update_payload()).eq("id", existing["id"]).execute()

        if changed_time:
            insert_scraper_log(
                sb,
                rec.record_scope,
                f"{rec.gender} {rec.distance}m {rec.stroke} {rec.pool_length}",
                rec.athlete_name or existing_athlete,
                existing_time_clock or safe_str(existing_time_ms),
                rec.time_clock or safe_str(rec.time_ms),
            )
            return ("UPDATE", "time changed")

        return ("FILL", "filled missing fields")

    except Exception as e:
        return ("ERROR", f"{type(e).__name__}: {e}")

# =========================
# World Aquatics scraper
# =========================

def wa_build_url(record_code: str, pool: str, gender: str, *, record_type: Optional[str] = None,
                 region: str = "", country_id: str = "", event_type_id: str = "") -> str:
    """
    Ejemplos válidos (según tu captura):
      WR: recordType=WR&recordCode=WR...
      WJ: recordType=WJ&recordCode=WJ...
      Continental: recordType=PAN&recordCode=CR&region=AMERICAS...
    """
    record_type = record_type or record_code
    return (
        "https://www.worldaquatics.com/swimming/records"
        f"?recordType={record_type}"
        f"&piscina={'50m' if pool=='LCM' else '25m'}"
        f"&recordCode={record_code}"
        f"&eventTypeId={event_type_id}"
        f"&region={region}"
        f"&countryId={country_id}"
        f"&gender={gender}"
        f"&pool={pool}"
    )

def wa_download_xlsx(play, url: str, out_path: str, *, timeout_ms: int = 45000) -> None:
    """
    Abre la URL en WorldAquatics y descarga el XLSX (link 'XLSX' o 'Download Records').
    """
    browser = play.chromium.launch(headless=True)
    ctx = browser.new_context(accept_downloads=True)
    page = ctx.new_page()

    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

    # Aceptar cookies si el banner tapa
    try:
        # suele aparecer como botón "Accept Cookies"
        page.get_by_role("button", name=re.compile(r"accept", re.I)).click(timeout=3000)
    except Exception:
        pass

    # Scroll un poco para asegurar que renderice links
    try:
        page.mouse.wheel(0, 800)
    except Exception:
        pass

    # Intento 1: click directo en link "XLSX"
    dl = None
    try:
        with page.expect_download(timeout=timeout_ms) as dlinfo:
            # suele ser un link visible 'XLSX'
            page.get_by_role("link", name=re.compile(r"xlsx", re.I)).click(timeout=8000)
        dl = dlinfo.value
    except Exception:
        dl = None

    # Intento 2: "Download Records" y elegir XLSX
    if dl is None:
        try:
            page.get_by_role("link", name=re.compile(r"download records", re.I)).click(timeout=8000)
            with page.expect_download(timeout=timeout_ms) as dlinfo:
                page.get_by_role("link", name=re.compile(r"xlsx", re.I)).click(timeout=8000)
            dl = dlinfo.value
        except Exception:
            dl = None

    if dl is None:
        raise RuntimeError("No pude descargar XLSX (no apareció link / descarga).")

    dl.save_as(out_path)

    ctx.close()
    browser.close()

def wa_parse_xlsx_to_records(xlsx_path: str, *, record_scope: str, record_type: str,
                             pool: str, gender: str, source_url: str, source_name: str,
                             source_note: str = "") -> List[RecordRow]:
    """
    Lee el XLSX y lo convierte a RecordRow(s).
    Heurística flexible por nombres de columnas.
    """
    df = pd.read_excel(xlsx_path)
    cols = {c: norm_space(str(c)).lower() for c in df.columns}
    # encontrar columnas por substrings
    def find_col(*needles: str) -> Optional[str]:
        for c, lc in cols.items():
            for n in needles:
                if n in lc:
                    return c
        return None

    col_event = find_col("event") or find_col("discipline") or find_col("prueba")
    col_time = find_col("time") or find_col("mark") or find_col("marca")
    col_athlete = find_col("athlete") or find_col("name") or find_col("swimmer") or find_col("nadador")
    col_country = find_col("country") or find_col("nation") or find_col("noc") or find_col("país")
    col_date = find_col("date") or find_col("fecha")
    col_comp = find_col("competition") or find_col("meet") or find_col("event name") or find_col("competición")
    col_city = find_col("city") or find_col("location") or find_col("venue") or find_col("lugar")

    out: List[RecordRow] = []
    for _, row in df.iterrows():
        event = safe_str(row.get(col_event, "")).strip()
        if not event:
            continue
        if is_relay_event(event):
            continue

        dist = parse_distance(event)
        if not dist:
            continue

        # stroke: buscar keywords
        ev_upper = event.upper()
        stroke = ""
        for k, v in STROKE_MAP.items():
            if k in ev_upper:
                stroke = v
                break
        if not stroke:
            # intentar última palabra
            stroke = parse_stroke(event.split()[-1])

        t_clock = safe_str(row.get(col_time, "")).strip()
        t_ms = time_clock_to_ms(t_clock)

        # WA a veces devuelve mm:ss.xx sin horas; ok
        rec_date = parse_date_any(safe_str(row.get(col_date, "")))

        athlete = norm_space(safe_str(row.get(col_athlete, "")))
        country = norm_space(safe_str(row.get(col_country, "")))
        comp = norm_space(safe_str(row.get(col_comp, "")))
        city = norm_space(safe_str(row.get(col_city, "")))

        out.append(RecordRow(
            record_scope=record_scope,
            record_type=record_type,
            category="Open",
            pool_length=pool,
            gender=parse_gender(gender),
            distance=dist,
            stroke=stroke,

            time_clock=t_clock,
            time_ms=t_ms,
            record_date=rec_date,
            competition_name=comp,
            athlete_name=athlete,
            city=city,
            country=country,
            last_updated=utc_now_iso(),

            source_name=source_name,
            source_url=source_url,
            source_note=source_note,
            verified=True,
            is_active=True,
        ))
    return out

# =========================
# CONSANAT scraper (Sudamericano)
# =========================

def consanat_extract_blocks(text: str) -> List[Dict[str, str]]:
    """
    CONSANAT expone el contenido como secciones (corta/larga) y por género (FEMININO/MASCULINO/MIXTO).
    En la extracción textual se ve como bloques repetidos:

      PRUEBAS
      TIEMPO
      RECORDISTA
      PAÍS
      FECHA
      LOCAL
      COMPETICIÓN
      <prueba>
      <tiempo>
      <recordista>
      <pais>
      <fecha>
      <local>
      <competición>
      ...

    Parseamos eso sin depender de <table>.
    """
    lines = [norm_space(l) for l in (text or "").splitlines() if l.strip()]
    out: List[Dict[str, str]] = []

    pool = None  # "SCM" o "LCM"
    gender = None  # "F" / "M" / "X"

    hdr = ["PRUEBAS", "TIEMPO", "RECORDISTA", "PAÍS", "FECHA", "LOCAL", "COMPETICIÓN"]

    i = 0
    while i < len(lines):
        ln = lines[i].upper()

        if "PISCINA CORTA" in ln:
            pool = "SCM"
            i += 1
            continue
        if "PISCINA LARGA" in ln:
            pool = "LCM"
            i += 1
            continue

        if ln.strip("# ").startswith("FEMININO") or ln.strip() == "FEMININO":
            gender = "F"; i += 1; continue
        if ln.strip("# ").startswith("MASCULINO") or ln.strip() == "MASCULINO":
            gender = "M"; i += 1; continue
        if ln.strip("# ").startswith("MIXTO") or ln.strip() == "MIXTO":
            gender = "X"; i += 1; continue

        # detectar comienzo de tabla por encabezados
        if i + 6 < len(lines) and [lines[i+j].upper() for j in range(7)] == hdr:
            i += 7
            # consumir bloques de 7 campos
            while i + 6 < len(lines):
                # si aparece un nuevo encabezado/sección, cortamos
                if lines[i].upper() in ("##",) or "PISCINA" in lines[i].upper() or lines[i].upper() in ("FEMININO","MASCULINO","MIXTO"):
                    break
                # Heurística: si vuelve a aparecer "PRUEBAS" asumimos reinicio
                if lines[i].upper() == "PRUEBAS":
                    break

                prueba = lines[i]; tiempo = lines[i+1]; recordista = lines[i+2]
                pais = lines[i+3]; fecha = lines[i+4]; local = lines[i+5]; compet = lines[i+6]
                out.append({
                    "pool_length": pool or "",
                    "gender": gender or "",
                    "event": prueba,
                    "time": tiempo,
                    "athlete": recordista,
                    "country": pais,
                    "date": fecha,
                    "city": local,
                    "competition": compet,
                })
                i += 7
            continue

        i += 1

    return out

def consanat_fetch_records(url: str) -> List[Dict[str, str]]:
    """
    Descarga página CONSANAT y extrae bloques.
    """
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n")
    return consanat_extract_blocks(text)

def consanat_blocks_to_records(blocks: List[Dict[str, str]], *, source_url: str) -> List[RecordRow]:
    out: List[RecordRow] = []
    for b in blocks:
        event = safe_str(b.get("event","")).strip()
        if not event or is_relay_event(event):
            continue

        dist = parse_distance(event)
        if not dist:
            continue

        # stroke
        u = event.upper()
        if "LIBRE" in u or "FREESTYLE" in u:
            stroke = "Libre"
        elif "ESPALDA" in u or "BACKSTROKE" in u:
            stroke = "Espalda"
        elif "PECHO" in u or "BREAST" in u:
            stroke = "Pecho"
        elif "MARIPOSA" in u or "BUTTER" in u:
            stroke = "Mariposa"
        elif "COMBINADO" in u or "MEDLEY" in u:
            stroke = "Combinado"
        else:
            stroke = parse_stroke(event.split()[-1])

        t_clock = safe_str(b.get("time","")).strip()
        t_ms = time_clock_to_ms(t_clock)
        if t_ms is None:
            continue

        g = b.get("gender","")
        # CONSANAT usa "MIXTO" para postas; nosotros salteamos relays, así que debería quedar M o F
        if g == "X":
            continue

        out.append(RecordRow(
            record_scope="Sudamericano",
            record_type="Récord Sudamericano",
            category="Open",
            pool_length=safe_str(b.get("pool_length","")).strip() or "SCM",
            gender=parse_gender(g),
            distance=dist,
            stroke=stroke,

            time_clock=t_clock.replace(".", ":") if re.fullmatch(r"\d{1,3}\.\d{2}\.\d{2}", t_clock.replace(" ", "")) else t_clock,
            time_ms=t_ms,
            record_date=parse_date_any(safe_str(b.get("date",""))),
            competition_name=norm_space(safe_str(b.get("competition","CONSANAT"))),
            athlete_name=norm_space(safe_str(b.get("athlete",""))),
            city=norm_space(safe_str(b.get("city",""))),
            country=norm_space(safe_str(b.get("country",""))),
            last_updated=utc_now_iso(),

            source_name="CONSANAT",
            source_url=source_url,
            source_note="Scrape texto estructurado (piscina corta y larga).",
            verified=True,
            is_active=True,
        ))
    return out

# =========================
# IPC SDMS (Paralímpico) - stub implementable
# =========================

def ipc_sdms_pdf_url(record_type: str, category: str, gender: str, age: str = "senior") -> str:
    """
    Estructura vista en páginas públicas (sdms web record sw pdf). Ej:
      https://www.paralympic.org/sdms/web/record/sw/pdf/type/WR/category/SC
    También existe en ipc-services.org (históricamente).
    """
    # preferimos paralympic.org porque es el frente público
    return f"https://www.paralympic.org/sdms/web/record/sw/pdf/type/{record_type}/category/{category}/gender/{gender}/age/{age}"

def ipc_download_pdf(play, url: str, out_path: str, *, timeout_ms: int = 45000) -> None:
    browser = play.chromium.launch(headless=True)
    ctx = browser.new_context(accept_downloads=True)
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    # si abre directo PDF, playwright igual lo "navega"; intentamos descargar:
    dl = None
    try:
        with page.expect_download(timeout=timeout_ms) as dlinfo:
            page.evaluate("window.print && window.print()")  # a veces dispara; si no, cae al plan B
        dl = dlinfo.value
    except Exception:
        dl = None

    if dl is None:
        # plan B: request directa (a veces funciona)
        try:
            resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if resp.ok and resp.headers.get("content-type","").lower().startswith("application/pdf"):
                with open(out_path, "wb") as f:
                    f.write(resp.content)
                ctx.close(); browser.close()
                return
        except Exception:
            pass
        raise RuntimeError("No pude descargar PDF de IPC SDMS (posible bloqueo).")

    dl.save_as(out_path)
    ctx.close(); browser.close()

def ipc_parse_pdf_to_records(pdf_path: str, *, pool: str, gender: str, source_url: str) -> List[RecordRow]:
    """
    Parser muy conservador: intenta leer texto y extraer filas con patrón:
      <Event> <Class> <Time> <Athlete> <NPC> <Date> <Location>
    Si no encuentra, devuelve vacío (no rompe el run).
    """
    out: List[RecordRow] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join([(p.extract_text() or "") for p in pdf.pages[:3]])
        # si no hay texto, abortamos (probable PDF tabular sin texto)
        if not text.strip():
            return out

        # Heurística: buscar líneas con tiempo tipo mm:ss.xx
        lines = [norm_space(l) for l in text.splitlines() if l.strip()]
        time_re = re.compile(r"\b(\d{1,2}:\d{2}\.\d{2}|\d{2}\.\d{2})\b")

        for ln in lines:
            if not time_re.search(ln):
                continue
            if "Relay" in ln or "4x" in ln:
                continue
            # intentar distancia/stroke
            dist = parse_distance(ln)
            if not dist:
                continue
            stroke = ""
            u = ln.upper()
            for k, v in STROKE_MAP.items():
                if k in u:
                    stroke = v
                    break
            if not stroke:
                # en para a veces figura "Freestyle" etc
                stroke = "Libre"

            t_clock = time_re.search(ln).group(1)
            t_ms = time_clock_to_ms(t_clock)

            # athlete: difícil; tomamos todo lo posterior al tiempo como fallback
            parts = ln.split(t_clock, 1)
            tail = parts[1].strip() if len(parts) > 1 else ""
            athlete = tail[:80]  # truncate

            out.append(RecordRow(
                record_scope="Paralímpico",
                record_type="Récord Mundial Para",
                category="Open",
                pool_length=pool,
                gender=parse_gender(gender),
                distance=dist,
                stroke=stroke,

                time_clock=t_clock,
                time_ms=t_ms,
                record_date=None,
                competition_name="World Para Swimming Records",
                athlete_name=athlete,
                city="",
                country="",
                last_updated=utc_now_iso(),

                source_name="Paralympic SDMS",
                source_url=source_url,
                source_note="Parser básico (mejorable) sobre PDF SDMS.",
                verified=False,  # hasta que refine el parser
                is_active=True,
            ))
    except Exception:
        return []
    return out

# =========================
# Main
# =========================

def main():
    sb = supa_client()

    enable_wa = env("ENABLE_WA", "1") == "1"
    enable_consanat = env("ENABLE_CONSANAT", "1") == "1"
    enable_ipc = env("ENABLE_IPC", "0") == "1"

    log_inserts = env("LOG_INSERTS", "0") == "1"

    # Resumen
    summary = {
        "version": MDV_UPDATER_VERSION,
        "started_at": utc_now_iso(),
        "seen": 0,
        "inserted": 0,
        "updated": 0,
        "filled": 0,
        "skipped": 0,
        "errors": 0,
        "errors_list": [],
    }

    records: List[RecordRow] = []

    # -------- WA --------
    if enable_wa:
        wa_tasks = []

        # WR + OR + WJ
        for pool in ("LCM", "SCM"):
            for gender in ("M", "F"):
                wa_tasks.append(("WR", "WR", pool, gender, "Mundial", "Récord Mundial"))
                wa_tasks.append(("OR", "OR", pool, gender, "Mundial", "Récord Olímpico"))
                wa_tasks.append(("WJ", "WJ", pool, gender, "Mundial", "Récord Mundial Junior"))

        # Continental (Americas) - NO es "Panamericano" (es un filtro continental de WA)
        for pool in ("LCM", "SCM"):
            for gender in ("M", "F"):
                wa_tasks.append(("CR", "PAN", pool, gender, "Americas (WA)", "Récord Continental (Americas)"))

        with sync_playwright() as play:
            for record_code, record_type, pool, gender, scope, rtype in wa_tasks:
                try:
                    region = "AMERICAS" if (record_code == "CR" and scope.startswith("Americas")) else ""
                    url = wa_build_url(record_code=record_code, pool=pool, gender=gender, record_type=record_type, region=region)
                    print(f"🔎 WA | {record_code} | {pool} | {gender} | {url}")

                    tmp_xlsx = f"/tmp/wa_{record_code}_{record_type}_{pool}_{gender}.xlsx"
                    wa_download_xlsx(play, url, tmp_xlsx)

                    recs = wa_parse_xlsx_to_records(
                        tmp_xlsx,
                        record_scope=scope,
                        record_type=rtype,
                        pool=pool,
                        gender=gender,
                        source_url=url,
                        source_name="World Aquatics",
                        source_note=f"WA {record_code} ({pool})",
                    )
                    records.extend(recs)
                except Exception as e:
                    summary["errors"] += 1
                    summary["errors_list"].append(f"WA {record_code}/{pool}/{gender}: {type(e).__name__}: {e}")

    # -------- CONSANAT --------
    if enable_consanat:
        try:
            url = "https://consanat.com/records/136/natacion"
            rows = consanat_fetch_records(url)
            cons = consanat_blocks_to_records(rows, source_url=url)
            # Nota: CONSANAT no siempre trae género; si querés, podemos separar por secciones más adelante.
            records.extend(cons)
        except Exception as e:
            summary["errors"] += 1
            summary["errors_list"].append(f"CONSANAT: {type(e).__name__}: {e}")

    # -------- IPC (Paralímpico) --------
    if enable_ipc:
        try:
            with sync_playwright() as play:
                for pool, cat in (("LCM","LC"), ("SCM","SC")):
                    for gender in ("M","F"):
                        url = ipc_sdms_pdf_url("WR", cat, gender, age="senior")
                        pdf_path = f"/tmp/ipc_wr_{cat}_{gender}.pdf"
                        try:
                            ipc_download_pdf(play, url, pdf_path)
                            recs = ipc_parse_pdf_to_records(pdf_path, pool=pool, gender=gender, source_url=url)
                            records.extend(recs)
                        except Exception as e:
                            summary["errors"] += 1
                            summary["errors_list"].append(f"IPC {cat}/{gender}: {type(e).__name__}: {e}")
        except Exception as e:
            summary["errors"] += 1
            summary["errors_list"].append(f"IPC: {type(e).__name__}: {e}")

    # -------- UPSERT --------
    for rec in records:
        summary["seen"] += 1

        # Si no hay género, no podemos matchear bien si la tabla exige género.
        # En tu tabla hay muchos con gender vacío? Si no, omitimos esos y reportamos.
        if rec.record_scope == "Sudamericano" and rec.gender == "":
            # En records_standards, Sudamericano suele tener M/F. Si no lo tenemos, no insertamos.
            summary["skipped"] += 1
            continue

        action, msg = upsert_one(sb, rec, log_inserts=log_inserts)
        if action == "INSERT":
            summary["inserted"] += 1
        elif action == "UPDATE":
            summary["updated"] += 1
        elif action == "FILL":
            summary["filled"] += 1
        elif action == "SKIP":
            summary["skipped"] += 1
        else:
            summary["errors"] += 1
            summary["errors_list"].append(msg or "unknown error")

    summary["finished_at"] = utc_now_iso()
    print("✅ DONE", json.dumps(summary, ensure_ascii=False))

    # Mail opcional
    email_user = env("EMAIL_USER", "")
    email_pass = env("EMAIL_PASS", "")
    if email_user and email_pass:
        try:
            send_mail_summary(email_user, email_pass, summary)
        except Exception:
            # no rompe el run
            pass

def send_mail_summary(email_user: str, email_pass: str, summary: Dict[str, Any]) -> None:
    import smtplib
    from email.mime.text import MIMEText

    subject = f"🏊 MDV Scraper | {summary.get('version','')} | upd={summary.get('updated',0)} ins={summary.get('inserted',0)} err={summary.get('errors',0)}"
    body = [
        f"Version: {summary.get('version')}",
        f"Started: {summary.get('started_at')}",
        f"Finished: {summary.get('finished_at')}",
        "",
        f"seen={summary.get('seen')} | inserted={summary.get('inserted')} | updated={summary.get('updated')} | filled={summary.get('filled')} | skipped={summary.get('skipped')} | errors={summary.get('errors')}",
        "",
    ]
    if summary.get("errors_list"):
        body.append("ERRORES:")
        body.extend([f"- {e}" for e in summary["errors_list"][:50]])

    msg = MIMEText("\n".join(body), _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = email_user
    msg["To"] = email_user

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(email_user, email_pass)
        s.send_message(msg)

if __name__ == "__main__":
    main()
