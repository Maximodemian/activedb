#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Open scraper (USA + CADDA) -> Supabase tables:
- public.standards_usa
- public.standards_cadda

Diseñado para correr en GitHub Actions / local.
Requiere Java si usás tabula-py (recomendado para PDFs con tablas).
Si tabula no está disponible, el script aborta con mensaje claro.

USO (ejemplos):
  python scrape_open_standards.py --only-download
  python scrape_open_standards.py --run usa_2024_2028_age_group --upsert
  python scrape_open_standards.py --run cadda_minimas_2023_2024 --upsert

ENV (para upsert):
  SUPABASE_URL=https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY=... (o una key con permisos de insert/upsert)
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
        # ciclo es opcional en tu tabla pero lo dejamos para no perder info
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
        # En PDF suele figurar MTS -> nosotros normalizamos a SCM (25m)
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

STYLE_MAP_CADDA = {
    "Freestyle": "LIBRE",
    "Backstroke": "ESPALDA",
    "Breaststroke": "PECHO",
    "Butterfly": "MARIPOSA",
    "IM": "COMBINADO",
    "COMBINED": "COMBINADO",
    "POSTA LIBRE": "POSTA LIBRE",
    "POSTA COMBINADA": "POSTA COMBINADA",
}


def norm_course(x: str) -> str:
    s = (x or "").strip().upper()
    if s in ("SCY", "SCM", "LCM"):
        return s
    # CADDA: MTS => SCM
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

    # Normalizaciones CADDA típicas
    # 38"51 -> 38.51
    s = s.replace("″", '"').replace("’", "'").replace("´", "'")
    # si tiene comillas como separador de centésimas
    if '"' in s and ":" not in s:
        s = s.replace('"', ".")
    # si tiene apóstrofe en lugar de punto
    if "'" in s and ":" not in s:
        # 36'00 -> 36.00
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


def http_get(url: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    return out_path


# =========================
# Supabase REST upsert
# =========================
def sb_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
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
    r = requests.post(endpoint, headers=headers, data=json.dumps(rows), timeout=120)
    if r.status_code >= 300:
        raise RuntimeError(f"Upsert {table} falló: {r.status_code} {r.text[:600]}")
    print(f"Upsert OK: {table} rows={len(rows)} status={r.status_code}")


# =========================
# Parsers por fuente
# =========================
def require_tabula() -> None:
    if tabula is None:
        raise RuntimeError(
            "tabula-py no está disponible. Instalá requirements y asegurate de tener Java.\n"
            "Recomendado: pip install tabula-py && apt-get install default-jre (en Linux)."
        )


def parse_usa_pdf(pdf_path: Path, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parser heurístico para USA Motivational Standards PDF.
    Estrategia:
      - tabula lee tablas por página.
      - detectamos tablas que contengan 'Event' y/o abreviaturas de curso (SCY/SCM/LCM)
      - reconstruimos filas: (edad, genero, estilo, distancia, curso, nivel, tiempo_s)
    IMPORTANTE: Los PDFs de USA cambian formato. Este parser está diseñado para iterar:
      - si en tu primer run quedan filas en 0, miramos los CSV debug en out/debug/
    """
    require_tabula()

    dfs = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True)
    rows: List[Dict[str, Any]] = []

    # Debug: guardamos cada tabla a csv para ajustar heurística
    debug_dir = Path("out/debug/usa")
    debug_dir.mkdir(parents=True, exist_ok=True)
    for i, df in enumerate(dfs):
        try:
            df.to_csv(debug_dir / f"table_{i:03d}.csv", index=False)
        except Exception:
            pass

    # Heurística mínima: buscamos columnas que incluyan 'Event' o algo similar
    for df in dfs:
        if df is None or df.empty:
            continue

        cols = [str(c).strip() for c in df.columns]
        joined_cols = " | ".join(cols).upper()

        # Muchos PDFs tienen Event como columna, otros lo meten en la primera col.
        has_event = any("EVENT" in str(c).upper() for c in cols) or "EVENT" in joined_cols

        # Intentamos detectar filas que contengan SCY/SCM/LCM
        df_str = df.astype(str)
        if not has_event and not df_str.apply(lambda r: r.str.contains(r"\bSCY\b|\bSCM\b|\bLCM\b", case=False, regex=True).any(), axis=1).any():
            continue

        # A partir de acá, el parse depende del layout. Implementamos un "best effort":
        # buscamos celdas con patrón "NN <STROKE> <COURSE>" p.ej. "50 FR SCY"
        for _, r in df.iterrows():
            cells = [str(x).strip() for x in r.tolist()]
            line = " ".join(cells).replace("  ", " ").strip()
            m = re.search(r"(\d+)\s*(?:Y|M)?\s*(FR|BK|BR|FLY|IM)\s*(SCY|SCM|LCM)", line, re.IGNORECASE)
            if not m:
                continue

            dist = int(m.group(1))
            estilo = STYLE_MAP_USA.get(m.group(2).upper(), m.group(2).upper())
            curso = norm_course(m.group(3).upper())

            # Nivel: en USA suele ser B/BB/A/AA/AAA/AAAA
            # Tomamos todas las celdas que parezcan tiempo y las mapeamos por "nivel" según el orden de columnas.
            # Si las columnas no son niveles, esto quedará para ajustar.
            level_candidates = [c for c in cols if re.fullmatch(r"A{1,4}|B{1,2}", str(c).strip().upper() or "X")]

            # Si no detectamos columnas nivel, intentamos inferir por cantidad (6 niveles típicos)
            if not level_candidates:
                level_candidates = ["B", "BB", "A", "AA", "AAA", "AAAA"]

            # Extraemos todos los tiempos de la fila
            times: List[float] = []
            for x in cells:
                t = parse_time_to_seconds(x)
                if t is not None:
                    times.append(t)

            if not times:
                continue

            # Asignamos por posición (hasta min)
            for j in range(min(len(times), len(level_candidates))):
                rows.append(
                    {
                        "ciclo": meta.get("ciclo"),
                        "genero": meta.get("genero") or None,  # si no lo inferimos, queda null
                        "edad": meta.get("edad") or None,      # idem
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


def parse_cadda_pdf(pdf_path: Path, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parser CADDA marcas mínimas PDF (tablas).
    Estrategia:
      - tabula lee tablas.
      - detectamos tablas con categorías en columnas (INFANTIL/MENOR/CADETE/etc.)
      - filas suelen ser estilo (LIBRE/ESPALDA/...) + distancias
      - genero suele estar en encabezado ("DAMAS"/"VARONES")

    Guardamos debug CSV para ajustar.
    """
    require_tabula()

    dfs = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True)
    rows: List[Dict[str, Any]] = []

    debug_dir = Path("out/debug/cadda")
    debug_dir.mkdir(parents=True, exist_ok=True)
    for i, df in enumerate(dfs):
        try:
            df.to_csv(debug_dir / f"table_{i:03d}.csv", index=False)
        except Exception:
            pass

    # Heurística: tablas que tengan alguna categoría típica
    cat_pat = re.compile(r"(INFANTIL|MENOR|CADETE|JUVENIL|JUNIOR|MAYORES|MASTER)", re.IGNORECASE)

    for df in dfs:
        if df is None or df.empty:
            continue
        cols = [str(c).strip() for c in df.columns]
        if not any(cat_pat.search(c) for c in cols):
            continue

        # Intento de detectar genero en columnas o primera fila
        genero = None
        header_blob = " ".join(cols).upper()
        if "DAMAS" in header_blob:
            genero = "F"
        elif "VARONES" in header_blob or "CABALLEROS" in header_blob:
            genero = "M"

        # Recorremos filas: buscamos una celda que parezca estilo, y una que parezca distancia
        for _, r in df.iterrows():
            cells = [str(x).strip() for x in r.tolist()]

            # Distancia (primer número que encontremos)
            dist = None
            for c in cells:
                m = re.search(r"\b(\d{2,4})\b", c)
                if m:
                    dist = int(m.group(1))
                    break
            if not dist:
                continue

            # Estilo (buscamos palabras típicas)
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

            # Las columnas (categorías) contienen tiempos, armamos filas por categoría
            for ci, col in enumerate(cols):
                cat = str(col).strip().upper()
                if not cat_pat.search(cat):
                    continue
                # la celda correspondiente puede estar desfasada si tabula crea columnas extra
                # intentamos usar el mismo índice si coincide
                val = None
                if ci < len(cells):
                    val = cells[ci]
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
# Main
# =========================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", choices=sorted(SOURCES.keys()), help="Fuente a correr (o todas si omitís)")
    ap.add_argument("--only-download", action="store_true", help="Solo descarga PDFs (no parsea ni upsertea)")
    ap.add_argument("--upsert", action="store_true", help="Hace upsert a Supabase")
    ap.add_argument("--outdir", default="out/pdfs", help="Carpeta de descarga")
    args = ap.parse_args()

    runs = [args.run] if args.run else list(SOURCES.keys())
    outdir = Path(args.outdir)
    all_rows_by_table: Dict[str, List[Dict[str, Any]]] = {"standards_usa": [], "standards_cadda": []}

    for key in runs:
        src = SOURCES[key]
        pdf_path = outdir / src["out_name"]
        print(f"\n==> {key}: descargando {src['url']}")
        http_get(src["url"], pdf_path)
        print(f"OK -> {pdf_path}")

        if args.only_download:
            continue

        kind = src["kind"]
        if kind == "usa":
            parsed = parse_usa_pdf(pdf_path, src)
            all_rows_by_table["standards_usa"].extend(parsed)
            print(f"Parse USA: rows={len(parsed)} (debug CSV en out/debug/usa)")
        elif kind == "cadda":
            parsed = parse_cadda_pdf(pdf_path, src)
            all_rows_by_table["standards_cadda"].extend(parsed)
            print(f"Parse CADDA: rows={len(parsed)} (debug CSV en out/debug/cadda)")
        else:
            raise RuntimeError(f"kind desconocido: {kind}")

    # Dedup simple en memoria (no reemplaza la UNIQUE de SQL)
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

    usa_rows = dedup(
        all_rows_by_table["standards_usa"],
        ["standard_type", "season_year", "genero", "edad", "estilo", "distancia_m", "curso", "nivel"],
    )
    cadda_rows = dedup(
        all_rows_by_table["standards_cadda"],
        ["genero", "categoria", "estilo", "distancia_m", "curso", "tipo_marca", "año", "target_meet"],
    )

    print(f"\nTOTAL USA rows (dedup): {len(usa_rows)}")
    print(f"TOTAL CADDA rows (dedup): {len(cadda_rows)}")

    if args.upsert:
        # Requiere UNIQUE/INDEX con esos campos para que on_conflict funcione bien
        if usa_rows:
            sb_upsert(
                "standards_usa",
                usa_rows,
                "standard_type,season_year,genero,edad,estilo,distancia_m,curso,nivel",
            )
        if cadda_rows:
            sb_upsert(
                "standards_cadda",
                cadda_rows,
                "genero,categoria,estilo,distancia_m,curso,tipo_marca,año,target_meet",
            )

    # Dump para inspección
    out_json = Path("out/parsed_rows.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"usa": usa_rows, "cadda": cadda_rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nDump JSON -> {out_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
