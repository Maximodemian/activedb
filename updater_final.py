#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MDV Records Updater (v16)

Fixes / adds:
- PANAM_GAMES Wikipedia parser: section-aware gender (Men/Women/Mixed) -> no more 100% skipped
- Relay handling: distance = total (4x100 => 400) to avoid collisions with individual events
- competition_location column: event venue stored here (NOT in athlete country)
- Athlete country stored in `country` (city often unavailable)
- Duplicate key (23505) safeguard: if insert hits unique constraint, fallback to update

Env vars required:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY (recommended) or SUPABASE_KEY

Optional:
- MDV_UPDATER_VERSION
- HTTP_TIMEOUT (seconds)
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import math
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from supabase import create_client
except Exception as e:
    raise SystemExit(f"Missing dependency supabase. Did you install requirements? Error: {e}")

# -------------------------- Config --------------------------

VERSION = os.getenv("MDV_UPDATER_VERSION", "WA+SUDAM+PANAM_v16_PANAM_GENDER_RELAY_TOTALDIST")
RUN_ID = os.getenv("GITHUB_RUN_ID") or str(int(time.time()))
UTC_TS = dt.datetime.now(dt.timezone.utc)
UTC_DATE = UTC_TS.date().isoformat()

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
UA = os.getenv(
    "HTTP_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es-AR;q=0.8,es;q=0.7",
}

# -------------------------- Helpers: parsing --------------------------

STROKE_MAP = {
    "freestyle": "Libre",
    "free": "Libre",
    "libre": "Libre",
    "backstroke": "Espalda",
    "back": "Espalda",
    "espalda": "Espalda",
    "breaststroke": "Pecho",
    "breast": "Pecho",
    "pecho": "Pecho",
    "butterfly": "Mariposa",
    "fly": "Mariposa",
    "mariposa": "Mariposa",
    "medley": "Combinado",
    "individual medley": "Combinado",
    "im": "Combinado",
    "combinado": "Combinado",
}

RELAY_ANY_RE = re.compile(r"\brelay\b|\brelevo\b|\b(?P<n>\d)\s*[x×]\s*(?P<leg>\d{2,4})\b", re.I)

# Individual events like "50 m freestyle" / "200m backstroke" / "400m individual medley"
EVENT_RE = re.compile(
    r"(?P<dist>\d{2,4})\s*m\s*(?P<stroke>freestyle|backstroke|breaststroke|butterfly|individual\s*medley|medley|libre|espalda|pecho|mariposa|combinado|im)",
    re.I,
)

TIME_RE = re.compile(r"^\s*(?:(?P<m>\d+):)?(?P<s>\d{1,2})(?:\.(?P<cs>\d{1,3}))?\s*$")


def parse_time_to_ms(t: Any) -> Optional[int]:
    """Accepts 21.58, 1:54.32, 0:52.1, etc. Returns milliseconds."""
    if t is None:
        return None
    s = str(t).strip()
    if not s or s.lower() in ("—", "-", "na", "n/a"):
        return None
    s = s.replace(",", ".")
    m = TIME_RE.match(s)
    if not m:
        return None
    mins = int(m.group("m") or 0)
    secs = int(m.group("s") or 0)
    cs_raw = m.group("cs")
    if cs_raw is None:
        cs = 0
    else:
        # normalize to centiseconds
        if len(cs_raw) == 1:
            cs = int(cs_raw) * 10
        elif len(cs_raw) == 2:
            cs = int(cs_raw)
        else:
            # milliseconds provided -> to centiseconds rounding down
            cs = int(cs_raw[:2])
    total_ms = (mins * 60 + secs) * 1000 + cs * 10
    return total_ms


def ms_to_clock_2dp(ms: int) -> str:
    if ms is None:
        return ""
    total_cs = int(ms // 10)
    cs = total_cs % 100
    total_s = total_cs // 100
    s = total_s % 60
    m = (total_s // 60) % 60
    h = total_s // 3600
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}.{cs:02d}"
    return f"{m:02d}:{s:02d}.{cs:02d}"


def parse_date_any(s: Any) -> Optional[str]:
    if not s:
        return None
    txt = str(s).strip()
    if not txt or txt.lower() in ("—", "-", "na", "n/a"):
        return None
    # Wikipedia often: "20 October 2011"
    for fmt in (
        "%d %B %Y",
        "%d %b %Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            return dt.datetime.strptime(txt, fmt).date().isoformat()
        except Exception:
            pass
    # fallback: try dateutil if available
    try:
        from dateutil import parser as du

        return du.parse(txt, dayfirst=False, fuzzy=True).date().isoformat()
    except Exception:
        return None


def parse_event(event_raw: Any) -> Tuple[Optional[int], Optional[str], str]:
    """Return (distance_m, stroke_es, type_probe).

    Important: For relays we return TOTAL distance (4x100 => 400) to avoid
    collisions with individual events under the current unique key.
    """
    if not event_raw:
        return None, None, "individual"
    s = str(event_raw).strip().lower()
    s = s.replace("individual medley", "medley")

    mrel = RELAY_ANY_RE.search(s)
    if mrel:
        n = mrel.groupdict().get("n")
        leg = mrel.groupdict().get("leg")
        total_dist = None
        if n and leg:
            try:
                total_dist = int(n) * int(leg)
            except Exception:
                total_dist = None
        # Some pages might write "4 × 50 m medley relay" without capturing groups
        if total_dist is None:
            m2 = re.search(r"(\d)\s*[x×]\s*(\d{2,4})\s*m", s)
            if m2:
                total_dist = int(m2.group(1)) * int(m2.group(2))

        # stroke heuristic
        stroke_key = None
        for k in ("medley", "im", "combinado", "backstroke", "breaststroke", "butterfly", "freestyle", "espalda", "pecho", "mariposa", "libre"):
            if k in s:
                stroke_key = k
                break
        stroke_es = STROKE_MAP.get(stroke_key or "freestyle", "Libre")
        return total_dist, stroke_es, "relay"

    m = EVENT_RE.search(s)
    if not m:
        return None, None, "individual"
    dist = int(m.group("dist"))
    stroke_key = m.group("stroke").lower().strip()
    stroke_es = STROKE_MAP.get(stroke_key)
    return dist, stroke_es, "individual"


def norm_text(x: Any) -> str:
    return re.sub(r"\s+", " ", str(x or "").strip())


def gender_from_text(ctx: str) -> Optional[str]:
    t = (ctx or "").lower()
    if any(k in t for k in ("women", "mujeres", "femenino", "female")):
        return "F"
    if any(k in t for k in ("men", "hombres", "masculino", "male")):
        return "M"
    if any(k in t for k in ("mixed", "mixto")):
        return "X"
    return None


# -------------------------- HTTP fetch --------------------------

def fetch_html(url: str, timeout: int = HTTP_TIMEOUT) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# -------------------------- Supabase layer --------------------------

@dataclass
class UpsertStats:
    seen: int = 0
    inserted: int = 0
    updated: int = 0
    filled: int = 0
    unchanged: int = 0
    skipped: int = 0
    errors: int = 0


class SB:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise SystemExit("Missing SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) in secrets/env")
        self.client = create_client(url, key)
        self.table = "records_standards"
        self.columns = self._detect_columns()

    def _detect_columns(self) -> List[str]:
        try:
            res = self.client.table(self.table).select("*").limit(1).execute()
            rows = res.data or []
            if rows:
                return list(rows[0].keys())
        except Exception:
            pass
        # fallback: most common schema
        return [
            "gender",
            "category",
            "pool_length",
            "stroke",
            "distance",
            "time_ms",
            "time_clock",
            "time_clock_2dp",
            "record_scope",
            "record_type",
            "competition_name",
            "competition_location",
            "athlete_name",
            "record_date",
            "city",
            "country",
            "last_updated",
            "source_url",
            "source_name",
            "source_note",
            "type_probe",
            "verified",
            "is_active",
        ]

    def _filter_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in payload.items() if k in self.columns}

    def _select_existing(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        q = self.client.table(self.table).select("*")
        for k, v in key.items():
            q = q.eq(k, v)
        res = q.limit(1).execute()
        rows = res.data or []
        return rows[0] if rows else None

    def upsert_record(self, payload: Dict[str, Any], stats: UpsertStats, fill_fields: List[str]) -> None:
        stats.seen += 1

        # key for uniqueness (must match DB unique index semantics)
        record_scope = payload.get("record_scope")
        if record_scope is None:
            payload["record_scope"] = ""  # keep non-null to match COALESCE unique index

        key = {
            "gender": payload.get("gender"),
            "category": payload.get("category"),
            "pool_length": payload.get("pool_length"),
            "stroke": payload.get("stroke"),
            "distance": payload.get("distance"),
            "record_type": payload.get("record_type"),
            "record_scope": payload.get("record_scope"),
        }

        # Basic validation
        if not all(key.values()):
            stats.skipped += 1
            return

        existing = self._select_existing(key)

        filtered = self._filter_payload(payload)
        # always update last_updated if exists
        if "last_updated" in self.columns:
            filtered["last_updated"] = UTC_DATE

        try:
            if not existing:
                # insert
                try:
                    self.client.table(self.table).insert(filtered).execute()
                    stats.inserted += 1
                    return
                except Exception as e:
                    # 23505 duplicate -> treat as update
                    if "23505" not in repr(e) and "duplicate key" not in str(e).lower():
                        raise
                    existing = self._select_existing(key)
                    if not existing:
                        raise

            # compare and update + fill
            updates: Dict[str, Any] = {}
            filled = 0
            changed = 0
            for k, v in filtered.items():
                if k == "id":
                    continue
                old = existing.get(k)
                if old in (None, "") and k in fill_fields and v not in (None, ""):
                    updates[k] = v
                    filled += 1
                else:
                    # avoid overwriting with empty
                    if v in (None, ""):
                        continue
                    if old != v:
                        updates[k] = v
                        changed += 1

            if updates:
                self.client.table(self.table).update(updates).match({"id": existing["id"]}).execute()
                if filled and changed:
                    stats.filled += filled
                    stats.updated += 1
                elif filled:
                    stats.filled += filled
                else:
                    stats.updated += 1
            else:
                stats.unchanged += 1

        except Exception:
            stats.errors += 1


# -------------------------- Payload builder --------------------------


def build_payload(
    *,
    gender: str,
    category: str,
    pool: str,
    event: str,
    record_type: str,
    record_scope: str,
    time_raw: Any,
    athlete_name: str,
    athlete_country: str,
    record_date: Any,
    competition_name: str,
    competition_location: str,
    source_url: str,
    source_name: str,
    source_note: str = "",
) -> Optional[Dict[str, Any]]:
    dist, stroke, type_probe = parse_event(event)
    if not dist or not stroke:
        return None

    ms = parse_time_to_ms(time_raw)
    if ms is None:
        return None

    payload: Dict[str, Any] = {
        "gender": (gender or "").upper()[:1],
        "category": category,
        "pool_length": pool,
        "stroke": stroke,
        "distance": int(dist),
        "time_ms": int(ms),
        "time_clock_2dp": ms_to_clock_2dp(int(ms)),
        "time_clock": ms_to_clock_2dp(int(ms)),
        "record_scope": record_scope or "",
        "record_type": record_type,
        "competition_name": norm_text(competition_name),
        "competition_location": norm_text(competition_location),
        "athlete_name": norm_text(athlete_name),
        # Athlete city often not provided reliably in sources
        "city": "",
        "country": norm_text(athlete_country),
        "record_date": parse_date_any(record_date),
        "last_updated": UTC_DATE,
        "source_url": source_url,
        "source_name": source_name,
        "source_note": source_note,
        "type_probe": type_probe,
        "is_active": True,
    }

    return payload


# -------------------------- Scrapers --------------------------


def parse_worldaquatics_records_page(html: str) -> List[Dict[str, Any]]:
    """Parse WA records listing. Returns list of dict with raw columns."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    # header map
    headers = [norm_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    # WA tables can repeat th in each row; fallback to first row
    if not headers:
        first = table.find("tr")
        if first:
            headers = [norm_text(th.get_text(" ", strip=True)) for th in first.find_all(["th", "td"])]

    rows_out: List[Dict[str, Any]] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        vals = [norm_text(td.get_text(" ", strip=True)) for td in tds]
        # If headers longer than vals, we still map by position
        row = {}
        for i, v in enumerate(vals):
            key = headers[i] if i < len(headers) else f"col{i}"
            row[key] = v
        rows_out.append(row)

    return rows_out


def wa_extract_rows(url: str) -> List[Dict[str, Any]]:
    html = fetch_html(url)
    return parse_worldaquatics_records_page(html)


def run_wa(sb: SB) -> UpsertStats:
    stats = UpsertStats()

    # record_type config
    # WR, WJ, OR and Continental (Americas)
    tasks = [
        ("Récord Mundial", "Mundial", "WR", "LCM", "M"),
        ("Récord Mundial", "Mundial", "WR", "LCM", "F"),
        ("Récord Mundial SC", "Mundial", "WR", "SCM", "M"),
        ("Récord Mundial SC", "Mundial", "WR", "SCM", "F"),
        ("Récord Mundial Junior", "Mundial", "WJ", "LCM", "M"),
        ("Récord Mundial Junior", "Mundial", "WJ", "LCM", "F"),
        ("Récord Mundial Junior SC", "Mundial", "WJ", "SCM", "M"),
        ("Récord Mundial Junior SC", "Mundial", "WJ", "SCM", "F"),
        ("Récord Olímpico", "Olímpico", "OR", "LCM", "M"),
        ("Récord Olímpico", "Olímpico", "OR", "LCM", "F"),
        ("Récord Continental Américas", "Américas", "CR_AMERICAS", "LCM", "M"),
        ("Récord Continental Américas", "Américas", "CR_AMERICAS", "LCM", "F"),
        ("Récord Continental Américas SC", "Américas", "CR_AMERICAS", "SCM", "M"),
        ("Récord Continental Américas SC", "Américas", "CR_AMERICAS", "SCM", "F"),
    ]

    def build_url(code: str, pool: str, gender: str) -> str:
        if code == "WR":
            return f"https://www.worldaquatics.com/swimming/records?recordType=WR&eventTypeId=&region=&countryId=&gender={gender}&pool={pool}"
        if code == "WJ":
            return f"https://www.worldaquatics.com/swimming/records?recordCode=WJ&eventTypeId=&region=&countryId=&gender={gender}&pool={pool}"
        if code == "OR":
            return f"https://www.worldaquatics.com/swimming/records?recordType=OR&eventTypeId=&region=&countryId=&gender={gender}&pool={pool}"
        if code == "CR_AMERICAS":
            return f"https://www.worldaquatics.com/swimming/records?recordType=PAN&recordCode=CR&eventTypeId=&region=AMERICAS&countryId=&gender={gender}&pool={pool}"
        raise ValueError(code)

    fill_fields = [
        "time_ms",
        "time_clock",
        "time_clock_2dp",
        "athlete_name",
        "country",
        "record_date",
        "competition_name",
        "competition_location",
        "source_url",
        "source_name",
        "source_note",
        "type_probe",
    ]

    for record_type, record_scope, code, pool, g in tasks:
        url = build_url(code, pool, g)
        print(f"🔎 WA | {code} | {pool} | {g} | {url}")
        try:
            rows = wa_extract_rows(url)
            for r in rows:
                # header names can vary; try multiple keys
                event = r.get("Event") or r.get("Discipline") or r.get("Record") or r.get("col0")
                timev = r.get("Time") or r.get("Mark") or r.get("col1")
                athlete = r.get("Athlete") or r.get("Name") or r.get("Competitor") or r.get("col2")
                country = r.get("Country") or r.get("Nation") or r.get("Nationality") or r.get("col3")
                datev = r.get("Date") or r.get("col4")
                comp = r.get("Competition") or r.get("Meet") or r.get("Event")  # sometimes same
                loc = r.get("Location") or r.get("Venue") or r.get("col5")

                payload = build_payload(
                    gender=g,
                    category="Open",
                    pool=pool,
                    event=event,
                    record_type=record_type,
                    record_scope=record_scope,
                    time_raw=timev,
                    athlete_name=athlete,
                    athlete_country=country,
                    record_date=datev,
                    competition_name=comp or "",
                    competition_location=loc or "",
                    source_url=url,
                    source_name="World Aquatics",
                    source_note="",
                )
                if not payload:
                    stats.skipped += 1
                    continue
                sb.upsert_record(payload, stats, fill_fields)
        except Exception as e:
            stats.errors += 1
            print(f"❌ WA {code} {pool} {g} error: {e}")

    return stats


# -------------------------- Wikipedia scrapers --------------------------


def wiki_parse_table(table, gender_fixed: Optional[str], pool_fixed: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    headers = [norm_text(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
    if not headers:
        return out

    def col_idx(*names: str) -> Optional[int]:
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    i_event = col_idx("event", "prueba", "discipline")
    i_time = col_idx("time", "marca")
    i_name = col_idx("name", "athlete", "swimmer", "nadador")
    i_country = col_idx("country", "nation", "país")
    i_date = col_idx("date", "fecha")
    i_games = col_idx("games", "competition", "meet", "torneo")
    i_loc = col_idx("location", "place", "venue", "sede")

    # Some wiki pages have multirow headers; use tbody tr with td length
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        vals = [norm_text(td.get_text(" ", strip=True)) for td in tds]
        if i_event is None or i_time is None:
            continue
        if i_event >= len(vals) or i_time >= len(vals):
            continue

        event = vals[i_event]
        timev = vals[i_time]
        namev = vals[i_name] if i_name is not None and i_name < len(vals) else ""
        countryv = vals[i_country] if i_country is not None and i_country < len(vals) else ""
        datev = vals[i_date] if i_date is not None and i_date < len(vals) else ""
        gamesv = vals[i_games] if i_games is not None and i_games < len(vals) else ""
        locv = vals[i_loc] if i_loc is not None and i_loc < len(vals) else ""

        g = gender_fixed
        p = pool_fixed
        out.append(
            {
                "gender": g,
                "pool": p,
                "event": event,
                "time": timev,
                "athlete": namev,
                "athlete_country": countryv,
                "record_date": datev,
                "competition_name": gamesv,
                "competition_location": locv,
            }
        )

    return out


def wiki_parse_sudam() -> List[Dict[str, Any]]:
    url = "https://es.wikipedia.org/wiki/Anexo:Plusmarcas_de_Sudam%C3%A9rica_de_nataci%C3%B3n"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.wikitable")
    rows_all: List[Dict[str, Any]] = []

    # Heuristic: caption or previous heading indicates gender + pool.
    for table in tables:
        ctx = ""
        cap = table.find("caption")
        if cap:
            ctx = cap.get_text(" ", strip=True)
        if not ctx:
            prev = table.find_previous(["h2", "h3", "h4", "h5"])
            if prev:
                ctx = prev.get_text(" ", strip=True)

        g = gender_from_text(ctx)
        # pool
        p = None
        t = (ctx or "").lower()
        if any(k in t for k in ("larga", "50", "lcm", "long course")):
            p = "LCM"
        elif any(k in t for k in ("corta", "25", "scm", "short course")):
            p = "SCM"

        # If we can't infer, skip table (better than wrong gender)
        if g is None or p is None:
            continue

        rows_all.extend(wiki_parse_table(table, g, p))

    # fallback: if nothing inferred, parse all tables with unknowns (will be skipped later)
    return rows_all


def wiki_parse_panam_games() -> Tuple[str, List[Dict[str, Any]]]:
    url = "https://en.wikipedia.org/wiki/List_of_Pan_American_Games_records_in_swimming"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    out_rows: List[Dict[str, Any]] = []

    # Wikipedia sections have spans with ids. We collect tables after each section heading.
    def collect_tables_after(section_id: str) -> List[Any]:
        span = soup.find("span", {"id": section_id})
        if not span:
            return []
        heading = span.find_parent(["h2", "h3", "h4"])
        if not heading:
            return []
        tables = []
        for sib in heading.find_all_next():
            # stop when next major heading begins
            if sib.name in ("h2", "h3"):
                sspan = sib.find("span", class_="mw-headline")
                if sspan and sspan.get("id") in ("Men", "Women", "Mixed_relay", "Mixed", "See_also", "References"):
                    if sspan.get("id") != section_id:
                        break
            if sib.name == "table" and "wikitable" in (sib.get("class") or []):
                tables.append(sib)
        return tables

    sections = [
        ("Men", "M"),
        ("Women", "F"),
        ("Mixed relay", "X"),
        ("Mixed_relay", "X"),
        ("Mixed", "X"),
    ]

    found_any = False
    for sec_id, g in sections:
        tbs = collect_tables_after(sec_id)
        if not tbs:
            continue
        found_any = True
        for tb in tbs:
            out_rows.extend(wiki_parse_table(tb, g, "LCM"))

    # final fallback if section anchors failed: parse all tables with gender inference from nearby text
    if not found_any:
        for tb in soup.select("table.wikitable"):
            ctx = ""
            prev = tb.find_previous(["h2", "h3", "h4", "h5"])
            if prev:
                ctx = prev.get_text(" ", strip=True)
            g = gender_from_text(ctx)
            if not g:
                continue
            out_rows.extend(wiki_parse_table(tb, g, "LCM"))

    return url, out_rows


def run_sudam(sb: SB) -> UpsertStats:
    stats = UpsertStats()

    # CONSANAT is often down; use Wikipedia stable source.
    url = "https://es.wikipedia.org/wiki/Anexo:Plusmarcas_de_Sudam%C3%A9rica_de_nataci%C3%B3n"
    rows = wiki_parse_sudam()
    print(f"🌎 SUDAM source=WIKI filas={len(rows)}")

    fill_fields = [
        "time_ms",
        "time_clock",
        "time_clock_2dp",
        "athlete_name",
        "country",
        "record_date",
        "competition_name",
        "competition_location",
        "source_url",
        "source_name",
        "type_probe",
    ]

    for r in rows:
        payload = build_payload(
            gender=r.get("gender"),
            category="Open",
            pool=r.get("pool"),
            event=r.get("event"),
            record_type="Récord Sudamericano",
            record_scope="Sudamérica",
            time_raw=r.get("time"),
            athlete_name=r.get("athlete"),
            athlete_country=r.get("athlete_country"),
            record_date=r.get("record_date"),
            competition_name=r.get("competition_name") or "",
            competition_location=r.get("competition_location") or "",
            source_url=url,
            source_name="Wikipedia",
            source_note="",
        )
        if not payload:
            stats.skipped += 1
            continue
        sb.upsert_record(payload, stats, fill_fields)

    return stats


def run_panam_games(sb: SB) -> UpsertStats:
    stats = UpsertStats()
    url, rows = wiki_parse_panam_games()
    print(f"🌎 PANAM_GAMES source=WIKI filas={len(rows)}")

    fill_fields = [
        "time_ms",
        "time_clock",
        "time_clock_2dp",
        "athlete_name",
        "country",
        "record_date",
        "competition_name",
        "competition_location",
        "source_url",
        "source_name",
        "type_probe",
    ]

    for r in rows:
        payload = build_payload(
            gender=r.get("gender"),
            category="Open",
            pool="LCM",
            event=r.get("event"),
            record_type="Récord Juegos Panamericanos",
            record_scope="Juegos Panamericanos",
            time_raw=r.get("time"),
            athlete_name=r.get("athlete"),
            athlete_country=r.get("athlete_country"),
            record_date=r.get("record_date"),
            competition_name=r.get("competition_name") or "",
            competition_location=r.get("competition_location") or "",
            source_url=url,
            source_name="Wikipedia",
            source_note="",
        )
        if not payload:
            stats.skipped += 1
            continue
        sb.upsert_record(payload, stats, fill_fields)

    return stats


# -------------------------- Main --------------------------


def main() -> int:
    print(f"🧬 DB columns detectadas: {len(SB().columns)}")
    print(f"MDV_UPDATER_VERSION={VERSION}")
    print(f"RUN_ID={RUN_ID}")
    print(f"Timestamp (UTC)={UTC_TS.isoformat()}")

    sb = SB()

    all_stats: Dict[str, UpsertStats] = {}

    all_stats["WA"] = run_wa(sb)
    all_stats["SUDAM"] = run_sudam(sb)
    all_stats["PANAM_GAMES"] = run_panam_games(sb)

    print(f"Version: {VERSION}")
    print(f"Run ID: {RUN_ID}")
    print(f"Timestamp (UTC): {UTC_TS.isoformat()}")
    for k, st in all_stats.items():
        print(
            f"[{k}] seen={st.seen} | inserted={st.inserted} | updated={st.updated} | filled={st.filled} | unchanged={st.unchanged} | skipped={st.skipped} | errors={st.errors}"
        )

    # Fail workflow if any errors (keeps GH Actions red when a source breaks)
    total_errors = sum(st.errors for st in all_stats.values())
    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
