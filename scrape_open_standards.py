#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Open scraper (USA + CADDA) -> Supabase tables:
- public.standards_usa
- public.standards_cadda

Diseñado para correr en GitHub Actions / local.
Requiere Java si usás tabula-py (recomendado para PDFs con tablas).

USO (ejemplos):
  python scrape_open_standards.py --only-download
  python scrape_open_standards.py --run usa_2024_2028_age_group --upsert
  python scrape_open_standards.py --run cadda_minimas_2023_2024 --upsert

ENV (para upsert):
  SUPABASE_URL=https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY=... (o una key con permisos de insert/upsert)

Mejora v1.1:
  - Reporte final (qué hizo y qué no) con warnings.
  - Fallback para USA: si lattice no extrae filas, reintenta en modo stream.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# --- Intentamos importar tabula-py (Java) ---
try:
    import tabula  # type: ignore
except Exception:
    tabula = None  # noqa

try:
    import pandas as pd  # type: ignore
except Exception as e:
    print("Falta pandas. Instalá requirements. Error:", e, file=sys.stderr)
    sys.exit(2)


# =========================
# Config de fuentes OPEN
# =========================
SOURCES: Dict[str, Dict[str, Any]] = {
    # USA Swimming Motivational Standards 2024-2028 (Age Group) – PDF oficial
    "usa_2024_2028_age_group": {
        "kind": "usa",
        "url": "https://websitedevsa.blob.core.windows.net/sitefinity/docs/default-source/timesdocuments/time-standards/2025/2028-motivational-standards-age-group.pdf",
        "season_year": "2024-2028",
        "standard_type": "AGE_GROUP",
        "ciclo": "2024-2028",
        "out_name": "usa_2024_2028_age_group.pdf",
    },
    # CADDA Marcas mínimas 2023/2024 – PDF oficial CADDA
    "cadda_minimas_2023_2024": {
        "kind": "cadda",
        "url": "https://cadda.org.ar/wp-content/uploads/2022/12/Marcas-Minimas-2023-y-2024.pdf",
        "anio": "2023/2024",
        "tipo_marca": "MINIMA",
        "target_meet": "NACIONAL",
        "curso": "SCM",
        "out_name": "cadda_marcas_minimas_2023_2024.pdf",
    },
}


# =========================
# Helpers normalización
# =========================
STYLE_MAP_USA = {
    "FR": "Freestyle",
    "FREE": "Freestyle",
    "BK": "Backstroke",
    "BACK": "Backstroke",
    "BR": "Breaststroke",
    "BREAST": "Breaststroke",
    "FLY": "Butterfly",
    "IM": "IM",
    "MEDLEY": "IM",
}


def norm_course(x: str) -> str:
    s = (x or "").strip().upper()
    if s in ("SCY", "SCM", "LCM"):
        return s
    if s in ("MTS", "METROS", "25M", "PILETA 25", "PILETA 25M"):
        return "SCM"
    if s in ("50M", "PILETA 50", "PILETA 50M"):
        return "LCM"
    return s


_TIME_RE = re.compile(r"^\s*(?:(\d+)\s*:\s*)?(\d+)(?:[.,]\s*(\d{1,2}))?\s*$")


def parse_time_to_seconds(raw: Any) -> Optional[float]:
    """
    Convierte strings tipo:
      - 27.80
      - 1:08.79
      - 38"51  (CADDA a veces)
      - 36'00" (CADDA a veces)
    a segundos (float).
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s in ("—", "-", "–"):
        return None

    s = s.replace("″", '"').replace("’", "'").replace("´", "'")
    if '"' in s and ":" not in s:
        s = s.replace('"', ".")
    if "'" in s and ":" not in s:
        s = s.replace("'", ".").replace('"', "")

    s = s.replace(",", ".")
    m = _TIME_RE.match(s)
    if not m:
        return None

    mm = int(m.group(1)) if m.group(1) else 0
    ss = int(m.group(2))
    cc = m.group(3)
    frac = int(cc) / (100 if len(cc) == 2 else 10) if cc else 0.0
    return mm * 60 + ss + frac


def http_get(url: str, out_path: Path) -> Tuple[Path, int]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    return out_path, len(r.content)


# =========================
# Supabase REST upsert
# =========================
def sb_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str) -> Tuple[int, str]:
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY en ENV.")

    endpoint = f"{url}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    r = requests.post(endpoint, headers=headers, data=json.dumps(rows), timeout=180)
    if r.status_code >= 300:
        raise RuntimeError(f"Upsert {table} falló: {r.status_code} {r.text[:800]}")
    # No todas las configs devuelven body con detalle; devolvemos status + texto truncado
    return r.status_code, (r.text[:200] if r.text else "")


# =========================
# Reporte por fuente
# =========================
@dataclass
class SourceReport:
    key: str
    kind: str
    url: str
    pdf_path: Path
    downloaded: bool = False
    pdf_bytes: int = 0
    parse_mode: str = ""
    parsed_rows: int = 0
    dedup_rows: int = 0
    upsert_attempted: bool = False
    upsert_table: str = ""
    upsert_rows: int = 0
    upsert_status: Optional[int] = None
    upsert_error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.upsert_error:
            return "FAIL"
        if self.kind in ("usa", "cadda") and not self.parsed_rows:
            return "WARN"
        return "OK"


# =========================
# Parsers por fuente
# =========================
def require_tabula() -> None:
    if tabula is None:
        raise RuntimeError(
            "tabula-py no está disponible. Instalá requirements y asegurate de tener Java.\n"
            "Recomendado: pip install tabula-py && apt-get install default-jre (en Linux)."
        )


def _save_debug_tables(dfs: List["pd.DataFrame"], debug_dir: Path) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    for i, df in enumerate(dfs):
        try:
            df.to_csv(debug_dir / f"table_{i:03d}.csv", index=False)
        except Exception:
            pass


def _parse_usa_from_dfs(dfs: List["pd.DataFrame"], meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for df in dfs:
        if df is None or df.empty:
            continue

        cols = [str(c).strip() for c in df.columns]
        joined_cols = " | ".join(cols).upper()

        has_event = any("EVENT" in str(c).upper() for c in cols) or "EVENT" in joined_cols

        df_str = df.astype(str)
        has_course_anywhere = df_str.apply(
            lambda r: r.str.contains(r"\bSCY\b|\bSCM\b|\bLCM\b", case=False, regex=True).any(),
            axis=1,
        ).any()

        if not has_event and not has_course_anywhere:
            continue

        for _, r in df.iterrows():
            cells = [str(x).strip() for x in r.tolist()]
            line = " ".join(cells).replace("  ", " ").strip()

            m = re.search(r"(\d+)\s*(FR|BK|BR|FLY|IM)\s*(SCY|SCM|LCM)", line, re.IGNORECASE)
            if not m:
                continue

            dist = int(m.group(1))
            estilo = STYLE_MAP_USA.get(m.group(2).upper(), m.group(2).upper())
            curso = norm_course(m.group(3).upper())

            level_candidates = [c for c in cols if re.fullmatch(r"A{1,4}|B{1,2}", str(c).strip().upper() or "X")]
            if not level_candidates:
                level_candidates = ["B", "BB", "A", "AA", "AAA", "AAAA"]

            times: List[float] = []
            for x in cells:
                t = parse_time_to_seconds(x)
                if t is not None:
                    times.append(t)

            if not times:
                continue

            for j in range(min(len(times), len(level_candidates))):
                rows.append(
                    {
                        "ciclo": meta.get("ciclo"),
                        "genero": meta.get("genero") or None,  # TODO: inferir desde encabezados
                        "edad": meta.get("edad") or None,      # TODO: inferir desde encabezados
                        "estilo": estilo,
                        "distancia_m": dist,
                        "curso": curso,
                        "nivel": level_candidates[j],
                        "tiempo_s": times[j],
                        "season_year": meta.get("season_year"),
                        "standard_type": meta.get("standard_type"),
                    }
                )

    return rows


def parse_usa_pdf(pdf_path: Path, meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """
    Parser heurístico para USA Motivational Standards PDF.
    - Intenta primero lattice=True.
    - Si devuelve 0 filas, reintenta con stream=True.
    Deja debug CSV en:
      out/debug/usa/lattice  y  out/debug/usa/stream
    """
    require_tabula()

    # 1) lattice
    dfs_lattice = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True)
    _save_debug_tables(dfs_lattice, Path("out/debug/usa/lattice"))
    rows = _parse_usa_from_dfs(dfs_lattice, meta)
    if rows:
        return rows, "lattice"

    # 2) stream fallback
    dfs_stream = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, stream=True)
    _save_debug_tables(dfs_stream, Path("out/debug/usa/stream"))
    rows2 = _parse_usa_from_dfs(dfs_stream, meta)
    return rows2, "stream"


def parse_cadda_pdf(pdf_path: Path, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parser CADDA marcas mínimas PDF (tablas) con tabula.
    Guardamos debug CSV en out/debug/cadda
    """
    require_tabula()

    dfs = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True)
    _save_debug_tables(dfs, Path("out/debug/cadda"))

    rows: List[Dict[str, Any]] = []
    cat_pat = re.compile(r"(INFANTIL|MENOR|CADETE|JUVENIL|JUNIOR|MAYORES|MASTER)", re.IGNORECASE)

    for df in dfs:
        if df is None or df.empty:
            continue
        cols = [str(c).strip() for c in df.columns]
        if not any(cat_pat.search(c) for c in cols):
            continue

        genero = None
        header_blob = " ".join(cols).upper()
        if "DAMAS" in header_blob:
            genero = "F"
        elif "VARONES" in header_blob or "CABALLEROS" in header_blob:
            genero = "M"

        for _, r in df.iterrows():
            cells = [str(x).strip() for x in r.tolist()]

            dist = None
            for c in cells:
                m = re.search(r"\b(\d{2,4})\b", c)
                if m:
                    dist = int(m.group(1))
                    break
            if not dist:
                continue

            estilo_raw = None
            for c in cells:
                cu = c.upper()
                if any(k in cu for k in ("LIBRE", "ESPALDA", "PECHO", "MARIP", "COMBINADO")):
                    estilo_raw = cu
                    break
            if not estilo_raw:
                continue

            estilo = estilo_raw
            if "MARIP" in estilo:
                estilo = "MARIPOSA"

            for ci, col in enumerate(cols):
                cat = str(col).strip().upper()
                if not cat_pat.search(cat):
                    continue
                val = cells[ci] if ci < len(cells) else None
                t = parse_time_to_seconds(val)
                if t is None:
                    continue

                rows.append(
                    {
                        "genero": genero,
                        "categoria": cat,
                        "estilo": estilo,
                        "distancia_m": dist,
                        "curso": norm_course(meta.get("curso", "SCM")),
                        "tipo_marca": meta.get("tipo_marca", "MINIMA"),
                        "tiempo_s": t,
                        "año": meta.get("anio"),
                        "target_meet": meta.get("target_meet"),
                    }
                )

    return rows


# =========================
# Utilidades
# =========================
def dedup(rows: List[Dict[str, Any]], key_fields: List[str]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        k = tuple((r.get(f) or "") for f in key_fields)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def print_summary(
    reports: List[SourceReport],
    only_download: bool,
    upsert: bool,
    totals: Dict[str, int],
    out_json: Path,
    elapsed_s: float,
) -> None:
    print("\n" + "=" * 78)
    print("OPEN SCRAPER SUMMARY")
    print(f"UTC: {datetime.now(timezone.utc).isoformat()}")
    print(f"only_download={only_download}  upsert={upsert}  elapsed={elapsed_s:.2f}s")
    print("-" * 78)

    for rep in reports:
        print(f"[{rep.status}] {rep.key} ({rep.kind})")
        print(f"  url: {rep.url}")
        if rep.downloaded:
            kb = rep.pdf_bytes / 1024 if rep.pdf_bytes else 0
            print(f"  pdf: {rep.pdf_path}  ({kb:.1f} KB)")
        else:
            print("  pdf: (no descargado)")

        if not only_download:
            print(f"  parse: rows={rep.parsed_rows}  mode={rep.parse_mode or '-'}  dedup={rep.dedup_rows}")
            if rep.kind == "usa":
                print("  debug: out/debug/usa/(lattice|stream)")
            else:
                print("  debug: out/debug/cadda")

        if upsert:
            if rep.upsert_attempted:
                if rep.upsert_error:
                    print(f"  upsert: FAIL table={rep.upsert_table} rows={rep.upsert_rows} err={rep.upsert_error}")
                else:
                    print(f"  upsert: OK   table={rep.upsert_table} rows={rep.upsert_rows} status={rep.upsert_status}")
            else:
                print("  upsert: (skip)")

        if rep.warnings:
            for w in rep.warnings:
                print(f"  warn: {w}")

        print("")

    print("-" * 78)
    print(f"TOTAL USA rows (dedup):   {totals.get('usa', 0)}")
    print(f"TOTAL CADDA rows (dedup): {totals.get('cadda', 0)}")
    print(f"Dump JSON: {out_json}")
    print("Outputs esperados para artifacts: out/parsed_rows.json, out/debug/, out/pdfs/")
    print("=" * 78)

    # Señalización útil para CI
    warn_any = any(r.status == "WARN" for r in reports)
    fail_any = any(r.status == "FAIL" for r in reports)
    if fail_any:
        print("NOTA: Hubo FAIL. Revisá errores arriba.")
    elif warn_any:
        print("NOTA: Hubo WARN (p.ej., filas=0). Revisá debug CSV para ajustar heurísticas.")


# =========================
# Main
# =========================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", choices=sorted(SOURCES.keys()), help="Fuente a correr (o todas si omitís)")
    ap.add_argument("--only-download", action="store_true", help="Solo descarga PDFs (no parsea ni upsertea)")
    ap.add_argument("--upsert", action="store_true", help="Hace upsert a Supabase")
    ap.add_argument("--outdir", default="out/pdfs", help="Carpeta de descarga")
    args = ap.parse_args()

    t0 = time.time()

    runs = [args.run] if args.run else list(SOURCES.keys())
    outdir = Path(args.outdir)
    all_rows_by_table: Dict[str, List[Dict[str, Any]]] = {"standards_usa": [], "standards_cadda": []}
    reports: List[SourceReport] = []

    for key in runs:
        src = SOURCES[key]
        pdf_path = outdir / src["out_name"]
        rep = SourceReport(key=key, kind=src["kind"], url=src["url"], pdf_path=pdf_path)

        print(f"\n==> {key}: descargando {src['url']}")
        try:
            _, nbytes = http_get(src["url"], pdf_path)
            rep.downloaded = True
            rep.pdf_bytes = nbytes
            print(f"OK -> {pdf_path}")
        except Exception as e:
            rep.upsert_error = f"download: {e}"
            rep.warnings.append("No se pudo descargar el PDF.")
            reports.append(rep)
            print(f"ERROR descargando {key}: {e}", file=sys.stderr)
            continue

        if args.only_download:
            reports.append(rep)
            continue

        try:
            if rep.kind == "usa":
                parsed, mode = parse_usa_pdf(pdf_path, src)
                rep.parse_mode = mode
                rep.parsed_rows = len(parsed)
                all_rows_by_table["standards_usa"].extend(parsed)
                if rep.parsed_rows == 0:
                    rep.warnings.append("USA devolvió 0 filas. Revisar out/debug/usa/* para ajustar el parser.")
                print(f"Parse USA: rows={len(parsed)} (debug CSV en out/debug/usa/{mode})")
            elif rep.kind == "cadda":
                parsed = parse_cadda_pdf(pdf_path, src)
                rep.parsed_rows = len(parsed)
                all_rows_by_table["standards_cadda"].extend(parsed)
                if rep.parsed_rows == 0:
                    rep.warnings.append("CADDA devolvió 0 filas. Revisar out/debug/cadda para ajustar el parser.")
                print(f"Parse CADDA: rows={len(parsed)} (debug CSV en out/debug/cadda)")
            else:
                raise RuntimeError(f"kind desconocido: {rep.kind}")
        except Exception as e:
            rep.upsert_error = f"parse: {e}"
            rep.warnings.append("Falló el parseo (ver error).")
            print(f"ERROR parseando {key}: {e}", file=sys.stderr)

        reports.append(rep)

    # Dedup simple en memoria (no reemplaza la UNIQUE de SQL)
    usa_rows = dedup(
        all_rows_by_table["standards_usa"],
        ["standard_type", "season_year", "genero", "edad", "estilo", "distancia_m", "curso", "nivel"],
    )
    cadda_rows = dedup(
        all_rows_by_table["standards_cadda"],
        ["genero", "categoria", "estilo", "distancia_m", "curso", "tipo_marca", "año", "target_meet"],
    )

    # Asignamos dedup por kind a los reports (hoy 1 a 1, pero soporta varios)
    for rep in reports:
        if rep.kind == "usa":
            rep.dedup_rows = len(usa_rows)
        elif rep.kind == "cadda":
            rep.dedup_rows = len(cadda_rows)

    print(f"\nTOTAL USA rows (dedup): {len(usa_rows)}")
    print(f"TOTAL CADDA rows (dedup): {len(cadda_rows)}")

    # Upsert
    if args.upsert:
        for rep in reports:
            if rep.upsert_error:  # si ya falló download/parse
                continue

            if rep.kind == "usa":
                rows = usa_rows
                table = "standards_usa"
                on_conflict = "standard_type,season_year,genero,edad,estilo,distancia_m,curso,nivel"
            elif rep.kind == "cadda":
                rows = cadda_rows
                table = "standards_cadda"
                on_conflict = "genero,categoria,estilo,distancia_m,curso,tipo_marca,año,target_meet"
            else:
                continue

            if not rows:
                rep.warnings.append("No hay filas para upsert (skip).")
                continue

            rep.upsert_attempted = True
            rep.upsert_table = table
            rep.upsert_rows = len(rows)
            try:
                status, _ = sb_upsert(table, rows, on_conflict)
                rep.upsert_status = status
                print(f"Upsert OK: {table} rows={len(rows)} status={status}")
            except Exception as e:
                rep.upsert_error = str(e)
                print(f"ERROR upsert {table}: {e}", file=sys.stderr)

    # Dump para inspección
    out_json = Path("out/parsed_rows.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps({"usa": usa_rows, "cadda": cadda_rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nDump JSON -> {out_json}")

    elapsed = time.time() - t0
    totals = {"usa": len(usa_rows), "cadda": len(cadda_rows)}
    print_summary(
        reports=reports,
        only_download=args.only_download,
        upsert=args.upsert,
        totals=totals,
        out_json=out_json,
        elapsed_s=elapsed,
    )

    # Exit code útil: si todo quedó vacío, marcamos error
    if not args.only_download and (len(usa_rows) + len(cadda_rows) == 0):
        return 4

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
