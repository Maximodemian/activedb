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

# --- 0. CORRECCIÓN DE ENCODING (CRÍTICO PARA GITHUB ACTIONS) ---
# Fuerza a la salida de consola a usar UTF-8 para evitar errores con tildes
sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURACIÓN GLOBAL ---
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# 1. FUENTES INTERNACIONALES (Wikipedia/Webs)
# Usamos URLs codificadas para evitar problemas de caracteres raros
INTERNATIONAL_TARGETS = [
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2024_World_Aquatics_Championships_%E2%80%93_Qualification", 
        "name": "Mundial Doha 2024",
        "pool": "LCM"
    },
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2024_Summer_Olympics_%E2%80%93_Qualification",
        "name": "JJOO Paris 2024",
        "pool": "LCM"
    }
]

# 2. FUENTE NACIONAL (CADDA)
CADDA_BASE_URL = "https://cadda.org.ar/todas-las-novedades/"

# --- HEADER PARA ENGAÑAR A WIKIPEDIA (Evita Error 403) ---
FAKE_BROWSER_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}


# ==============================================================================
# SECCIÓN 1: UTILIDADES COMUNES
# ==============================================================================

def check_db_status(event_name):
    """Cuenta registros existentes en DB para un evento dado."""
    try:
        res = supabase.table("clasificacion_corte")\
            .select("id", count="exact")\
            .eq("nombre_evento", event_name)\
            .execute()
        return res.count
    except Exception as e:
        print(f"⚠️ Error consultando DB ({event_name}): {e}")
        return 0

def clean_time_generic(time_str):
    """Intenta convertir cualquier string de tiempo a segundos."""
    if pd.isna(time_str) or str(time_str).strip() == "": return None
    # Limpieza agresiva: quita notas [a], comillas, espacios
    clean = re.sub(r'\[.*?\]', '', str(time_str)).replace("'", "").replace('"', '').replace(",", ".").strip()
    
    try:
        if ':' in clean:
            parts = clean.split(':')
            return float(parts[0]) * 60 + float(parts[1])
        return float(clean)
    except:
        return None


# ==============================================================================
# SECCIÓN 2: LÓGICA INTERNACIONAL (WIKIPEDIA / HTML)
# ==============================================================================

def normalize_event_wiki(event_name):
    """Normaliza nombres desde inglés (Wiki) a español estándar."""
    e = event_name.upper()
    
    # Género
    gender = 'X'
    if "MEN" in e and "WOMEN" not in e: gender = 'M'
    if "WOMEN" in e: gender = 'F'
    
    # Distancia
    dist = re.search(r'(\d+)', e)
    distance = dist.group(1) if dist else ""
    
    # Estilo
    style = "LIBRE"
    if "BACK" in e: style = "ESPALDA"
    if "BREAST" in e: style = "PECHO"
    if "BUTTER" in e or "FLY" in e: style = "MARIPOSA"
    if "MEDLEY" in e or "INDIVIDUAL" in e: style = "IM"
    
    return gender, f"{distance} {style}"

def process_international_targets():
    print("\n🌐 --- INICIANDO ACTUALIZACIÓN INTERNACIONAL ---")
    
    for target in INTERNATIONAL_TARGETS:
        print(f"🌍 Scrapeando: {target['name']}...")
        try:
            # 1. SOLICITUD HTTP CON HEADERS (La clave para evitar el 403)
            response = requests.get(target['url'], headers=FAKE_BROWSER_HEADER)
            response.raise_for_status() # Lanza error si falla la conexión
            
            # 2. PANDAS LEE EL TEXTO YA DESCARGADO (Usando StringIO)
            tables = pd.read_html(io.StringIO(response.text))
            
        except Exception as e:
            print(f"❌ Error leyendo HTML: {e}")
            continue

        records = []
        for df in tables:
            df_str = df.to_string().upper()
            
            # Heurística para detectar tabla de tiempos (OQT, Standard, Time)
            if "FREE" in df_str and ("OQT" in df_str or "STANDARD" in df_str or "TIME" in df_str):
                cols = [str(c).upper() for c in df.columns]
                
                # Detectar columnas por género
                idx_men = -1
                idx_women = -1
                
                for i, col in enumerate(cols):
                    if "MEN" in col and ("OQT" in col or "A STANDARD" in col or "TIME" in col): idx_men = i
                    if "WOMEN" in col and ("OQT" in col or "A STANDARD" in col or "TIME" in col): idx_women = i
                
                if idx_men == -1 and idx_women == -1: continue

                for index, row in df.iterrows():
                    raw_event = str(row[0]).upper()
                    if not re.search(r'\d+', raw_event): continue 

                    _, prueba = normalize_event_wiki(raw_event)
                    
                    # Hombres
                    if idx_men != -1:
                        sec = clean_time_generic(row[idx_men])
                        if sec:
                            records.append({
                                "nombre_evento": target['name'],
                                "tipo_corte": "OQT / Marca A",
                                "categoria": "OPEN",
                                "genero": "M",
                                "prueba": prueba,
                                "piscina": target['pool'],
                                "tiempo_s": sec,
                                "tiempo_display": str(row[idx_men]),
                                "temporada": datetime.datetime.now().year
                            })
                    
                    # Mujeres
                    if idx_women != -1:
                        sec = clean_time_generic(row[idx_women])
                        if sec:
                            records.append({
                                "nombre_evento": target['name'],
                                "tipo_corte": "OQT / Marca A",
                                "categoria": "OPEN",
                                "genero": "F",
                                "prueba": prueba,
                                "piscina": target['pool'],
                                "tiempo_s": sec,
                                "tiempo_display": str(row[idx_women]),
                                "temporada": datetime.datetime.now().year
                            })

        # Inserción Internacional
        if records:
            print(f"   🚀 Encontrados {len(records)} registros. Actualizando DB...")
            try:
                # Borrar y reescribir
                supabase.table("clasificacion_corte").delete().eq("nombre_evento", target['name']).execute()
                supabase.table("clasificacion_corte").insert(records).execute()
                print("   ✅ Actualizado.")
            except Exception as e:
                print(f"   ❌ Error DB: {e}")
        else:
            print("   ⚠️ No se extrajeron datos de esta URL (¿Cambió el formato?).")


# ==============================================================================
# SECCIÓN 3: LÓGICA NACIONAL (CADDA / PDF)
# ==============================================================================

def normalize_event_cadda(text):
    """Normaliza nombres desde español (CADDA)."""
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
        # Usamos los mismos headers para CADDA por si acaso
        resp = requests.get(CADDA_BASE_URL, headers=FAKE_BROWSER_HEADER)
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        current_year = datetime.datetime.now().year
        next_year = current_year + 1
        
        articles = soup.find_all('article')
        for art in articles:
            link_tag = art.find('a')
            if not link_tag: continue
            
            title = link_tag.get_text(strip=True).upper()
            href = link_tag.get('href')
            
            # Criterio: REGLAMENTO + NACIONAL + (AÑO ACTUAL o SIGUIENTE)
            if "REGLAMENTO" in title and "NACIONAL" in title:
                if str(current_year) in title or str(next_year) in title:
                    print(f"   🎯 Candidato: {title}")
                    
                    post_resp = requests.get(href, headers=FAKE_BROWSER_HEADER)
                    post_soup = BeautifulSoup(post_resp.content, 'html.parser')
                    
                    pdf_links = post_soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
                    for pdf in pdf_links:
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

    print(f"⬇️  Procesando: {event_name} ({pdf_url})")
    pool_length = "SCM" if "CORTA" in event_name else "LCM"
    
    # Diagnóstico previo
    db_count = check_db_status(event_name)
    print(f"   📊 Registros actuales en DB: {db_count}")

    records_to_insert = []
    
    try:
        resp = requests.get(pdf_url, headers=FAKE_BROWSER_HEADER)
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            print(f"   📄 Leyendo {len(pdf.pages)} páginas...")
            
            for page in pdf.pages:
                tables = page.extract_tables()
                if not tables: continue
                
                for table in tables:
                    category_map = {}
                    
                    # Detectar columnas (Categorías)
                    for row in table:
                        row_text = [str(c).upper() for c in row if c]
                        if any(x in str(row_text) for x in ['CADETE', 'JUVENIL', 'MAYOR', 'OPEN', 'MENOR', 'PRIMERA']):
                            for col_idx, cell in enumerate(row):
                                if cell:
                                    clean_cat = cell.replace("\n", " ").upper().strip()
                                    if "PRUEBA" not in clean_cat and "DISTANCIA" not in clean_cat:
                                        category_map[col_idx] = clean_cat
                            break
                    
                    if not category_map: continue

                    # Extraer filas
                    for row in table:
                        if not row or len(row) < 1: continue
                        raw_event = row[0]
                        event = normalize_event_cadda(raw_event)
                        if not event: continue
                        
                        for col_idx, category in category_map.items():
                            if col_idx < len(row):
                                sec = clean_time_generic(row[col_idx])
                                if sec:
                                    records_to_insert.append({
                                        "nombre_evento": event_name,
                                        "tipo_corte": "Marca Clasificatoria",
                                        "categoria": category,
                                        "genero": 'X', 
                                        "prueba": event,
                                        "piscina": pool_length,
                                        "tiempo_s": sec,
                                        "tiempo_display": str(row[col_idx]).strip(),
                                        "temporada": datetime.datetime.now().year
                                    })
    except Exception as e:
        print(f"   ❌ Error crítico leyendo PDF: {e}")
        return

    extracted_count = len(records_to_insert)
    print(f"   🔍 Se extrajeron {extracted_count} tiempos válidos del PDF.")

    if extracted_count > 0:
        print(f"   🚀 Actualizando base de datos...")
        try:
            if db_count > 0:
                supabase.table("clasificacion_corte").delete().eq("nombre_evento", event_name).execute()
            
            batch_size = 50
            for i in range(0, extracted_count, batch_size):
                batch = records_to_insert[i:i+batch_size]
                supabase.table("clasificacion_corte").insert(batch).execute()
            print("   ✅ CADDA Actualizado Exitosamente.")
        except Exception as e:
            print(f"   ❌ Error escritura DB: {e}")
    else:
        print("   ⚠️ El PDF se descargó pero no se extrajeron datos. No se toca la DB.")


# ==============================================================================
# MAIN EJECUTOR
# ==============================================================================

if __name__ == "__main__":
    print("🚀 INICIANDO SCRAPER DE CORTES Y CLASIFICACIÓN (Unified V2.1)")
    
    process_international_targets()
    process_cadda_regulation()
    
    print("\n🏁 Proceso finalizado.")
