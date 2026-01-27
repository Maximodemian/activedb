import os
import sys
import requests
import pdfplumber
import io
import re
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

# --- 0. CORRECCIÓN DE ENCODING ---
sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURACIÓN ---
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# FUENTES INTERNACIONALES
INTERNATIONAL_TARGETS = [
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2024_Summer_Olympics_%E2%80%93_Qualification", 
        "name": "JJOO Paris 2024",
        "pool": "LCM"
    },
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2025_World_Aquatics_Championships", 
        "name": "Mundial Singapur 2025",
        "pool": "LCM"
    }
]

# FUENTE NACIONAL
CADDA_BASE_URL = "https://cadda.org.ar/todas-las-novedades/"
FAKE_BROWSER_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

# ==============================================================================
# UTILIDADES
# ==============================================================================

def check_db_status(event_name):
    try:
        res = supabase.table("clasificacion_corte").select("id", count="exact").eq("nombre_evento", event_name).execute()
        return res.count
    except Exception as e:
        print(f"⚠️ Error consultando DB ({event_name}): {e}")
        return 0

def clean_time_generic(time_str):
    if pd.isna(time_str) or str(time_str).strip() == "": return None
    clean = re.sub(r'\[.*?\]', '', str(time_str))
    clean = re.sub(r'\(.*?\)', '', clean)
    clean = clean.replace("'", "").replace('"', '').replace(",", ".").strip()
    try:
        if ':' in clean:
            parts = clean.split(':')
            return float(parts[0]) * 60 + float(parts[1])
        return float(clean)
    except:
        return None

def normalize_event_wiki(event_name):
    e = str(event_name).upper()
    gender = 'X'
    if "MEN" in e and "WOMEN" not in e: gender = 'M'
    if "WOMEN" in e: gender = 'F'
    
    dist = re.search(r'(\d+)', e)
    distance = dist.group(1) if dist else ""
    
    style = "LIBRE"
    if "BACK" in e: style = "ESPALDA"
    if "BREAST" in e: style = "PECHO"
    if "BUTTER" in e or "FLY" in e: style = "MARIPOSA"
    if "MEDLEY" in e or "INDIVIDUAL" in e: style = "IM"
    
    return gender, f"{distance} {style}"

# ==============================================================================
# LÓGICA INTERNACIONAL (WIKIPEDIA)
# ==============================================================================

def process_international_targets():
    print("\n🌐 --- INICIANDO ACTUALIZACIÓN INTERNACIONAL ---")
    
    for target in INTERNATIONAL_TARGETS:
        print(f"🌍 Scrapeando: {target['name']}...")
        try:
            response = requests.get(target['url'], headers=FAKE_BROWSER_HEADER)
            response.raise_for_status()
            tables = pd.read_html(io.StringIO(response.text))
        except Exception as e:
            print(f"❌ Error leyendo HTML: {e}")
            continue

        records = []
        
        for i, df in enumerate(tables):
            # --- 1. APLANAR COLUMNAS ---
            # Convierte headers complejos en texto plano
            clean_cols = []
            for col in df.columns:
                if isinstance(col, tuple):
                    # Unimos niveles, ignorando 'Unnamed'
                    parts = [str(c) for c in col if "Unnamed" not in str(c)]
                    clean_cols.append(" ".join(parts).upper())
                else:
                    clean_cols.append(str(col).upper())
            
            df.columns = clean_cols
            
            # --- 2. DETECTOR DE COLUMNAS (DEBUG MODE) ---
            # Palabras clave ampliadas
            idx_event = -1
            idx_men = -1
            idx_women = -1
            
            # Buscamos Evento
            for idx, c in enumerate(clean_cols):
                if any(x in c for x in ["EVENT", "DISTANCE", "PRUEBA"]):
                    idx_event = idx
                    break
            if idx_event == -1: idx_event = 0 # Default a la primera

            # Buscamos Hombres y Mujeres con lógica laxa
            # Buscamos "MEN" + algo de tiempo, o si la tabla es simple, buscamos "TIME" en columnas separadas
            for idx, c in enumerate(clean_cols):
                is_men = "MEN" in c and "WOMEN" not in c
                is_women = "WOMEN" in c
                
                # Palabras que indican tiempo
                is_time = any(x in c for x in ["OQT", "OCT", "STANDARD", "TIME", "A CUT", "ENTRY"])
                
                # Prioridad 1: "MEN OQT" o "MEN TIME"
                if is_men and is_time: idx_men = idx
                if is_women and is_time: idx_women = idx

            # DEBUG CRÍTICO: Imprimir qué columnas ve si falla la detección
            if idx_men == -1 and idx_women == -1:
                # Solo imprimimos si parece una tabla de natación (tiene Freestyle o similar)
                content = df.to_string().upper()
                if "FREE" in content or "MEDLEY" in content:
                    print(f"   ⚠️ [DEBUG] Tabla {i} ignorada. Columnas encontradas: {clean_cols}")
                continue

            # --- 3. EXTRACCIÓN ---
            for index, row in df.iterrows():
                raw_event = str(row[idx_event]).upper()
                
                # Validación básica: debe tener números
                if not re.search(r'\d+', raw_event): continue 
                
                _, prueba = normalize_event_wiki(raw_event)
                
                # Hombres
                if idx_men != -1:
                    val = row[idx_men]
                    sec = clean_time_generic(val)
                    if sec:
                        records.append({
                            "nombre_evento": target['name'],
                            "tipo_corte": "Marca A / OQT",
                            "categoria": "OPEN",
                            "genero": "M",
                            "prueba": prueba,
                            "piscina": target['pool'],
                            "tiempo_s": sec,
                            "tiempo_display": str(val),
                            "temporada": datetime.datetime.now().year
                        })
                
                # Mujeres
                if idx_women != -1:
                    val = row[idx_women]
                    sec = clean_time_generic(val)
                    if sec:
                        records.append({
                            "nombre_evento": target['name'],
                            "tipo_corte": "Marca A / OQT",
                            "categoria": "OPEN",
                            "genero": "F",
                            "prueba": prueba,
                            "piscina": target['pool'],
                            "tiempo_s": sec,
                            "tiempo_display": str(val),
                            "temporada": datetime.datetime.now().year
                        })

        if records:
            print(f"   🚀 ÉXITO en {target['name']}: {len(records)} tiempos. Actualizando DB...")
            try:
                supabase.table("clasificacion_corte").delete().eq("nombre_evento", target['name']).execute()
                supabase.table("clasificacion_corte").insert(records).execute()
                print("   ✅ Base de datos sincronizada.")
            except Exception as e:
                print(f"   ❌ Error escritura DB: {e}")
        else:
            print(f"   ⚠️ {target['name']}: No se extrajeron filas (Revisar logs DEBUG arriba).")

# ==============================================================================
# LÓGICA NACIONAL (CADDA)
# ==============================================================================

def normalize_event_cadda(text):
    if not text: return None
    text = text.upper().replace("MTS", "").replace("METROS", "").replace(".", "").strip()
    text = text.replace("CROL", "LIBRE").replace("FREE", "LIBRE")
    text = text.replace("BACK", "ESPALDA")
    text = text.replace("BREAST", "PECHO")
    text = text.replace("FLY", "MARIPOSA")
    text = text.replace("COMBINADO", "IM").replace("MEDLEY", "IM")
    match = re.search(r'(\d+)\s+([A-Z]+)', text)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return None

def find_cadda_pdf():
    print(f"🕵️  Buscando reglamentos en: {CADDA_BASE_URL}")
    try:
        resp = requests.get(CADDA_BASE_URL, headers=FAKE_BROWSER_HEADER)
        soup = BeautifulSoup(resp.content, 'html.parser')
        current_year = datetime.datetime.now().year
        next_year = current_year + 1
        
        for art in soup.find_all('article'):
            link_tag = art.find('a')
            if not link_tag: continue
            title = link_tag.get_text(strip=True).upper()
            if "REGLAMENTO" in title and "NACIONAL" in title:
                if str(current_year) in title or str(next_year) in title:
                    print(f"   🎯 Candidato: {title}")
                    post_resp = requests.get(link_tag.get('href'), headers=FAKE_BROWSER_HEADER)
                    for pdf in BeautifulSoup(post_resp.content, 'html.parser').find_all('a', href=re.compile(r'\.pdf$', re.I)):
                        return pdf.get('href'), title
    except Exception as e:
        print(f"❌ Error buscando en CADDA: {e}")
    return None, None

def process_cadda_regulation():
    print("\n🇦🇷 --- INICIANDO ACTUALIZACIÓN CADDA ---")
    pdf_url, event_name = find_cadda_pdf()
    if not pdf_url:
        print("🤷 No se encontraron nuevos reglamentos nacionales hoy.")
        return

    print(f"⬇️  Procesando: {event_name}")
    pool_length = "SCM" if "CORTA" in event_name else "LCM"
    db_count = check_db_status(event_name)
    records_to_insert = []
    
    try:
        resp = requests.get(pdf_url, headers=FAKE_BROWSER_HEADER)
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                if not tables: continue
                for table in tables:
                    category_map = {}
                    for row in table:
                        row_text = [str(c).upper() for c in row if c]
                        if any(x in str(row_text) for x in ['CADETE', 'JUVENIL', 'MAYOR', 'OPEN', 'MENOR']):
                            for col_idx, cell in enumerate(row):
                                if cell:
                                    clean = cell.replace("\n", " ").upper().strip()
                                    if "PRUEBA" not in clean and "DISTANCIA" not in clean:
                                        category_map[col_idx] = clean
                            break
                    if not category_map: continue

                    for row in table:
                        if not row or len(row) < 1: continue
                        event = normalize_event_cadda(row[0])
                        if not event: continue
                        for col_idx, cat in category_map.items():
                            if col_idx < len(row):
                                sec = clean_time_generic(row[col_idx])
                                if sec:
                                    records_to_insert.append({
                                        "nombre_evento": event_name,
                                        "tipo_corte": "Marca Clasificatoria",
                                        "categoria": cat,
                                        "genero": 'X', 
                                        "prueba": event,
                                        "piscina": pool_length,
                                        "tiempo_s": sec,
                                        "tiempo_display": str(row[col_idx]).strip(),
                                        "temporada": datetime.datetime.now().year
                                    })
    except Exception as e:
        print(f"❌ Error PDF: {e}")
        return

    if records_to_insert:
        print(f"   🚀 CADDA: {len(records_to_insert)} tiempos. Actualizando DB...")
        try:
            if db_count > 0: supabase.table("clasificacion_corte").delete().eq("nombre_evento", event_name).execute()
            batch_size = 50
            for i in range(0, len(records_to_insert), batch_size):
                supabase.table("clasificacion_corte").insert(records_to_insert[i:i+batch_size]).execute()
            print("   ✅ Sincronización CADDA completada.")
        except Exception as e:
            print(f"❌ Error DB: {e}")
    else:
        print("   ⚠️ PDF descargado pero sin datos extraíbles.")

if __name__ == "__main__":
    print("🚀 INICIANDO SCRAPER UNIFICADO (V4.0 - DEBUG)")
    process_international_targets()
    process_cadda_regulation()
    print("\n🏁 Proceso finalizado.")
