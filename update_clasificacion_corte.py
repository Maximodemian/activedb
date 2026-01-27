import os
import requests
import pdfplumber
import io
import re
import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# URL donde CADDA publica las novedades
BASE_URL = "https://cadda.org.ar/todas-las-novedades/"

def time_to_seconds(time_str):
    """Convierte '2:15.50' o '59.80' a segundos (float)."""
    if not time_str: return None
    clean = str(time_str).strip().replace("'", "").replace('"', '').replace(",", ".")
    try:
        if ':' in clean:
            parts = clean.split(':')
            return float(parts[0]) * 60 + float(parts[1])
        return float(clean)
    except ValueError:
        return None

def normalize_event(text):
    """Normaliza nombres de pruebas."""
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

def check_db_status(event_name):
    """Consulta el estado actual del evento en la DB."""
    try:
        # Contamos registros existentes para este evento
        res = supabase.table("clasificacion_corte")\
            .select("id", count="exact")\
            .eq("nombre_evento", event_name)\
            .execute()
        return res.count
    except Exception as e:
        print(f"⚠️ Error consultando DB: {e}")
        return 0

def find_latest_regulation_pdf():
    """Busca automáticamente el PDF del Reglamento Nacional más reciente."""
    print(f"🕵️  Escaneando novedades en: {BASE_URL}")
    try:
        resp = requests.get(BASE_URL, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        current_year = datetime.datetime.now().year
        next_year = current_year + 1
        
        # Buscamos artículos
        articles = soup.find_all('article')
        
        for art in articles:
            link_tag = art.find('a')
            if not link_tag: continue
            
            title = link_tag.get_text(strip=True).upper()
            href = link_tag.get('href')
            
            # CRITERIO DE BÚSQUEDA
            if "REGLAMENTO" in title and "NACIONAL" in title:
                if str(current_year) in title or str(next_year) in title:
                    print(f"🎯 Candidato encontrado en Web: {title}")
                    
                    # Entrar al post para buscar el PDF
                    post_resp = requests.get(href, headers={'User-Agent': 'Mozilla/5.0'})
                    post_soup = BeautifulSoup(post_resp.content, 'html.parser')
                    
                    pdf_links = post_soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
                    
                    for pdf in pdf_links:
                        pdf_url = pdf.get('href')
                        return pdf_url, title
        
        print("Build Info: No se encontraron artículos que cumplan los criterios (REGLAMENTO + NACIONAL + AÑO).")
        return None, None
                        
    except Exception as e:
        print(f"❌ Error buscando en la web: {e}")
        return None, None
    
def process_pdf_standards(pdf_url, event_name, pool_length="LCM"):
    print(f"⬇️  Descargando PDF: {pdf_url}")
    response = requests.get(pdf_url)
    if response.status_code != 200:
        print(f"❌ Error HTTP al descargar: {response.status_code}")
        return

    # 1. DIAGNÓSTICO PREVIO (DB)
    db_count = check_db_status(event_name)
    print(f"📊 Estado DB para '{event_name}': {db_count} registros existentes.")

    records_to_insert = []
    
    # 2. EXTRACCIÓN (PDF)
    try:
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            print(f"📄 Analizando {len(pdf.pages)} páginas del documento...")
            
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables:
                    continue
                    
                for table in tables:
                    header_row = None
                    category_map = {} 
                    
                    # Detección dinámica de columnas
                    for row in table:
                        row_text = [str(c).upper() for c in row if c]
                        if any(x in str(row_text) for x in ['CADETE', 'JUVENIL', 'MAYOR', 'OPEN', 'MENOR', 'PRIMERA']):
                            header_row = row
                            for col_idx, cell in enumerate(row):
                                if cell:
                                    clean_cat = cell.replace("\n", " ").upper().strip()
                                    if "PRUEBA" not in clean_cat and "DISTANCIA" not in clean_cat:
                                        category_map[col_idx] = clean_cat
                            break
                    
                    if not category_map: 
                        # Si encontramos tabla pero no cabecera válida, seguimos
                        continue

                    # Extracción de Tiempos
                    for row in table:
                        if not row or len(row) < 1: continue
                        raw_event = row[0]
                        event = normalize_event(raw_event)
                        
                        if not event: continue 
                        
                        for col_idx, category in category_map.items():
                            if col_idx < len(row):
                                raw_time = row[col_idx]
                                seconds = time_to_seconds(raw_time)
                                
                                if seconds:
                                    record = {
                                        "nombre_evento": event_name,
                                        "tipo_corte": "Marca Clasificatoria",
                                        "categoria": category,
                                        "genero": 'X', 
                                        "prueba": event,
                                        "piscina": pool_length,
                                        "tiempo_s": seconds,
                                        "tiempo_display": str(raw_time).strip(),
                                        "temporada": datetime.datetime.now().year
                                    }
                                    records_to_insert.append(record)
    except Exception as e:
        print(f"❌ Error crítico leyendo el PDF (pdfplumber): {e}")
        return

    extracted_count = len(records_to_insert)
    print(f"🔍 Resultado Extracción: Se encontraron {extracted_count} tiempos válidos en el PDF.")

    # 3. LÓGICA DE COMPARACIÓN Y DECISIÓN
    if extracted_count == 0:
        print("⚠️ ALERTA: El PDF se descargó pero no se extrajo NADA.")
        print("   -> Posible cambio de formato en la tabla del PDF.")
        print("   -> No se tocará la Base de Datos para evitar borrar datos viejos por error.")
        return

    if extracted_count > 0:
        # Si ya hay datos y son similares en cantidad, quizás no haga falta tocar nada
        # Pero como pueden cambiar centésimas, forzamos la actualización si hay extracción exitosa.
        
        print(f"🚀 Procediendo a actualizar DB...")
        
        try:
            # Borrado seguro: Solo si tenemos datos nuevos para poner
            if db_count > 0:
                print(f"🗑️  Borrando {db_count} registros antiguos de '{event_name}'...")
                supabase.table("clasificacion_corte").delete().eq("nombre_evento", event_name).execute()
            
            # Inserción
            batch_size = 50
            print(f"💉 Insertando {extracted_count} nuevos registros...")
            for i in range(0, extracted_count, batch_size):
                batch = records_to_insert[i:i+batch_size]
                supabase.table("clasificacion_corte").insert(batch).execute()
                
            print("✅ ACTUALIZACIÓN EXITOSA: Base de datos sincronizada.")
            
        except Exception as e:
            print(f"❌ Error escribiendo en Supabase: {e}")

if __name__ == "__main__":
    url, title = find_latest_regulation_pdf()
    
    if url:
        print(f"✅ Se procederá a procesar: {title}")
        pool = "SCM" if "CORTA" in title else "LCM"
        process_pdf_standards(url, title, pool)
    else:
        print("zzz No se encontró ningún reglamento nuevo para procesar hoy.")
