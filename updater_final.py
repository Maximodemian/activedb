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
import csv
import io
from urllib.parse import urlencode
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




def pool_from_text(ctx: str) -> Optional[str]:
    """Infer pool length (LCM/SCM) from nearby heading/caption text."""
    t = (ctx or "").lower()
    # Short course (25m)
    if any(k in t for k in ["scm", "short course", "25m", "25 m", "25-m", "25 metre", "25 meter", "piscina corta"]):
        return "SCM"
    # Long course (50m)
    if any(k in t for k in ["lcm", "long course", "50m", "50 m", "50-m", "50 metre", "50 meter", "piscina larga"]):
        return "LCM"
    return None

# -------------------------- HTTP fetch --------------------------

def fetch_html(url: str, timeout: int = HTTP_TIMEOUT) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def fetch_text(url: str, timeout: int = HTTP_TIMEOUT, headers: Optional[Dict[str, str]] = None,
               retries: int = 3, backoff: float = 1.7) -> str:
    """GET text with retries (useful for API endpoints that occasionally throttle)."""
    h = dict(HEADERS)
    if headers:
        h.update(headers)

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=h, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** (attempt - 1))
            else:
                break
    raise last_err or RuntimeError(f"Failed to fetch: {url}")



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



# --------------------- World Aquatics (API) ----------------------

WA_API_BASE = "https://api.worldaquatics.com/fina/records/report"

def wa_api_url(record_code: str, gender: str, pool: str, region: str = "") -> str:
    params = {
        "countryId": "",
        "eventTypeId": "",
        "gender": gender,
        "pool": pool,
        "recordCode": record_code,
        "region": region or "",
    }
    return WA_API_BASE + "?" + urlencode(params)

def _wa_find_event_cell(row: List[str]) -> Optional[str]:
    # Most exports include the event name near the end, preceded by an event id (e.g., "10, Men 100m Backstroke")
    for cell in reversed(row):
        c = (cell or "").strip()
        if not c:
            continue
        lc = c.lower()

        # skip pure time-like cells
        if re.fullmatch(r"\d{1,2}:\d{2}\.\d{2}", c) or re.fullmatch(r"\d{1,2}:\d{2}\.\d{1,2}", c) or re.fullmatch(r"\d{1,2}\.\d{2}", c):
            continue

        if "relay" in lc or "relevo" in lc:
            return c
        if any(w in lc for w in ["men", "women", "mixed"]):
            if "m" in lc:
                return c
        if re.search(r"\b\d{2,4}\s*m\b", lc) and any(
            k in lc for k in [
                "freestyle","backstroke","breaststroke","butterfly","medley","individual medley",
                "libre","espalda","pecho","mariposa","combinado","mixto",
            ]
        ):
            return c
    return None

def wa_parse_report_csv(text: str) -> List[Dict[str, Any]]:
    """
    Parse the World Aquatics `records/report` CSV-ish export.
    It sometimes comes with a header row, sometimes without (depends on server version / caching).
    """
    txt = (text or "").lstrip("\ufeff").strip()
    if not txt:
        return []

    reader = csv.reader(io.StringIO(txt), skipinitialspace=True)
    rows = [r for r in reader if any((c or "").strip() for c in r)]
    if not rows:
        return []

    header = None
    start = 0
    if rows[0] and "record" in rows[0][0].lower() and "description" in rows[0][0].lower():
        header = [norm_text(h).lower() for h in rows[0]]
        start = 1

    def idx_like(keys: List[str]) -> Optional[int]:
        if not header:
            return None
        for i, h in enumerate(header):
            for k in keys:
                if k in h:
                    return i
        return None

    i_time = idx_like(["time"])
    i_athlete = idx_like(["athlete", "swimmer", "name"])
    i_nf = idx_like(["nf code", "noc", "nf"])
    i_gender = idx_like(["gender"])
    i_comp = idx_like(["competition", "meet", "games"])
    i_country = idx_like(["country"])
    i_city = idx_like(["city"])
    i_date = idx_like(["date"])
    i_event = idx_like(["event"])

    out: List[Dict[str, Any]] = []

    for row in rows[start:]:
        def get(i: Optional[int]) -> str:
            if i is None or i < 0 or i >= len(row):
                return ""
            return norm_text(row[i])

        timev = get(i_time) if i_time is not None else (norm_text(row[2]) if len(row) > 2 else "")
        athlete = get(i_athlete) if i_athlete is not None else (norm_text(row[3]) if len(row) > 3 else "")
        nf = get(i_nf) if i_nf is not None else (norm_text(row[4]) if len(row) > 4 else "")
        comp = get(i_comp) if i_comp is not None else (norm_text(row[6]) if len(row) > 6 else "")
        country = get(i_country) if i_country is not None else (norm_text(row[7]) if len(row) > 7 else "")
        city = get(i_city) if i_city is not None else (norm_text(row[8]) if len(row) > 8 else "")
        datev = get(i_date)
        event = get(i_event) if i_event is not None else _wa_find_event_cell([norm_text(c) for c in row])

        if not (event and timev and athlete):
            continue

        loc = ", ".join([x for x in [country, city] if x])

        out.append(
            {
                "event": event,
                "time": timev,
                "athlete": athlete,
                "athlete_country": nf,
                "record_date": datev,
                "competition_name": comp,
                "competition_location": loc,
            }
        )

    return out

def wa_fetch_report_rows(record_code: str, gender: str, pool: str, region: str = "") -> Tuple[str, List[Dict[str, Any]]]:
    url = wa_api_url(record_code=record_code, gender=gender, pool=pool, region=region)
    # Ask for CSV explicitly
    txt = fetch_text(url, headers={"Accept": "text/csv,*/*"})
    return url, wa_parse_report_csv(txt)


def run_wa(sb: SB, dry_run: bool, stats: UpsertStats) -> None:
    print("\n=== WORLD AQUATICS (records/report API) ===")

    # record_type: stored in DB; record_code: used for the WA API request
    tasks = [
        {
            "label": "Récord Mundial",
            "record_scope": "Mundial",
            "record_type": "WR",
            "record_code": "WR",
            "region": "",
            "pools": ["LCM", "SCM"],
            "genders": ["M", "F", "X"],
            "source_name": "WORLD AQUATICS",
        },
        {
            "label": "Récord Mundial Junior",
            "record_scope": "Mundial Junior",
            "record_type": "WJ",
            "record_code": "WJ",
            "region": "",
            "pools": ["LCM", "SCM"],
            "genders": ["M", "F", "X"],
            "source_name": "WORLD AQUATICS",
        },
        {
            "label": "Récord Olímpico",
            "record_scope": "Olímpico",
            "record_type": "OR",
            "record_code": "OR",
            "region": "",
            "pools": ["LCM"],
            "genders": ["M", "F", "X"],
            "source_name": "WORLD AQUATICS",
        },
        {
            # World Aquatics uses recordCode=AM for Americas records, plus region=AMERICAS
            "label": "Récord Continental Américas",
            "record_scope": "Américas",
            "record_type": "CR",
            "record_code": "AM",
            "region": "AMERICAS",
            "pools": ["LCM", "SCM"],
            "genders": ["M", "F", "X"],
            "source_name": "WORLD AQUATICS",
        },
    ]

    fill_fields = [
        "time_ms",
        "time_clock",
        "time_clock_2dp",
        "athlete_name",
        "athlete_country",
        "record_date",
        "competition_name",
        "competition_location",
        "source_name",
        "source_url",
        "source_note",
    ]

    for t in tasks:
        for pool in t["pools"]:
            for gender in t["genders"]:
                try:
                    source_url, rows = wa_fetch_report_rows(
                        record_code=t["record_code"],
                        gender=gender,
                        pool=pool,
                        region=t["region"],
                    )
                except Exception as e:
                    stats.errors += 1
                    print(f"❌ WA ERROR {t['label']} {pool} {gender}: {e}")
                    continue

                if not rows:
                    stats.errors += 1
                    print(f"❌ WA EMPTY {t['label']} {pool} {gender}: 0 filas (fallo de scrape/API)")
                    continue

                print(f"✅ WA {t['label']} | {pool} | {gender}: {len(rows)} filas")

                for r in rows:
                    payload = build_payload(
                        source_name=t["source_name"],
                        source_url=source_url,
                        source_note=f"recordCode={t['record_code']}; region={t['region'] or 'ALL'}",
                        record_type=t["record_type"],
                        record_scope=t["record_scope"],
                        pool=pool,
                        gender=gender,
                        category="Absoluto",
                        event=r.get("event") or "",
                        time_raw=r.get("time") or "",
                        athlete_name=r.get("athlete") or "",
                        athlete_country=r.get("athlete_country") or "",
                        record_date=r.get("record_date") or "",
                        competition_name=r.get("competition_name") or "",
                        competition_location=r.get("competition_location") or "",
                    )

                    if payload is None:
                        stats.skipped += 1
                        continue

                    if dry_run:
                        stats.seen += 1
                        continue

                    sb.upsert_record(payload, stats, fill_fields=fill_fields)

def wiki_parse_table(table, gender_fixed: Optional[str], pool_fixed: Optional[str]) -> List[Dict[str, Any]]:
    """
    Parse a Wikipedia "wikitable" containing swimming records.

    IMPORTANT: Wikipedia record tables often use <th scope="row"> for the event name in each data row.
    So we must:
      1) detect the real header row (column headers),
      2) read row cells using both <th> and <td> to keep alignment.
    """
    out: List[Dict[str, Any]] = []

    # 1) Detect header row (column headers only, NOT all <th> in the table)
    headers: List[str] = []
    header_tr = None
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if not ths:
            continue
        txt = norm_text(tr.get_text(" ", strip=True)).lower()
        # Heuristic: a header row usually contains the word "event" / "time" and has several <th>
        if len(ths) >= 3 and (("event" in txt) or ("time" in txt) or any((th.get("scope") or "").lower() in ("col", "colgroup") for th in ths)):
            headers = [norm_text(th.get_text(" ", strip=True)).lower() for th in ths]
            header_tr = tr
            break
    if not headers:
        # Fallback: first row with >=3 cells (th+td)
        tr0 = table.find("tr")
        if tr0:
            cells0 = tr0.find_all(["th", "td"])
            if len(cells0) >= 3:
                headers = [norm_text(c.get_text(" ", strip=True)).lower() for c in cells0]
                header_tr = tr0

    if not headers:
        return out

    def col_idx(*names: str) -> Optional[int]:
        for n in names:
            n = n.lower()
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    i_event = col_idx("event", "prueba", "discipline")
    i_time = col_idx("time", "marca")
    i_name = col_idx("name", "athlete", "swimmer", "nadador")
    i_country = col_idx("nation", "country", "nf", "noc", "país", "pais")
    i_date = col_idx("date", "fecha")
    i_games = col_idx("meet", "competition", "games", "tournament", "campeonato")
    i_loc = col_idx("location", "venue", "city", "place", "sede", "lugar")

    g = gender_fixed
    p = pool_fixed

    # 2) Parse rows
    for tr in table.find_all("tr"):
        if header_tr is not None and tr == header_tr:
            continue
        # skip header-like rows
        if tr.find("th", attrs={"scope": "col"}):
            continue

        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        def cell_get(i: Optional[int]) -> str:
            if i is None:
                return ""
            if i < 0 or i >= len(cells):
                return ""
            return norm_text(cells[i].get_text(" ", strip=True))

        event = cell_get(i_event)
        timev = cell_get(i_time)
        namev = cell_get(i_name)
        countryv = cell_get(i_country)
        datev = cell_get(i_date)
        gamesv = cell_get(i_games)
        locv = cell_get(i_loc)

        # Some tables put the event in the first cell as a row header (<th scope="row">)
        if not event and cells:
            first = norm_text(cells[0].get_text(" ", strip=True))
            if any(k in first.lower() for k in ["m", "relay", "relevo", "freestyle", "backstroke", "breaststroke", "butterfly", "medley", "individual medley"]):
                event = first

        if not (event and timev and namev):
            continue

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
    """
    South American records (Wikipedia).

    Wikipedia pages often structure records by headings like:
      - Men's long course (50 m)
      - Women's short course (25 m)
    and then tables.

    We walk the main content in document order and keep the last-seen (gender, pool) context from headings/captions.
    """
    url = "https://en.wikipedia.org/wiki/List_of_South_American_records_in_swimming"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    content = soup.select_one("#mw-content-text .mw-parser-output") or soup.body or soup
    out: List[Dict[str, Any]] = []

    cur_gender: Optional[str] = None
    cur_pool: Optional[str] = None

    def update_ctx(text: str) -> None:
        nonlocal cur_gender, cur_pool
        g = gender_from_text(text)
        p = pool_from_text(text)
        if g:
            cur_gender = g
        if p:
            cur_pool = p

    # Walk top-level nodes in the content area (order matters)
    for node in content.find_all(recursive=False):
        if node.name in ("h2", "h3", "h4", "h5"):
            update_ctx(node.get_text(" ", strip=True))
            continue

        if node.name != "table":
            # also learn from paragraphs / list items that sometimes contain "Long course" headings
            if node.name in ("p", "div"):
                t = node.get_text(" ", strip=True)
                if t:
                    update_ctx(t)
            continue

        if "wikitable" not in node.get("class", []):
            continue

        # Try caption / first header row as local context
        cap = node.caption.get_text(" ", strip=True) if node.caption else ""
        if cap:
            update_ctx(cap)

        header_text = ""
        first_tr = node.find("tr")
        if first_tr:
            header_text = norm_text(first_tr.get_text(" ", strip=True))
            if header_text:
                update_ctx(header_text)

        g = gender_from_text(cap) or gender_from_text(header_text) or cur_gender
        p = pool_from_text(cap) or pool_from_text(header_text) or cur_pool

        if not g or not p:
            # last attempt: scan a bit more text from inside the table
            sample = norm_text(node.get_text(" ", strip=True))[:300]
            g = g or gender_from_text(sample)
            p = p or pool_from_text(sample)

        if not g or not p:
            print(f"⚠️  SUDAM: skipping table (could not infer gender/pool). ctx_gender={cur_gender} ctx_pool={cur_pool} caption={cap[:80]!r}")
            continue

        out.extend(wiki_parse_table(node, g, p))

    return out

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

    return out_rows


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

    wa_stats = UpsertStats()
    run_wa(sb, dry_run=False, stats=wa_stats)
    all_stats["WA"] = wa_stats
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
