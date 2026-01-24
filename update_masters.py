import os
import requests
import pdfplumber
import re
import io
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# URLs de los Campeonatos Nacionales (Suelen tener los PDFs de NQTs)
# Si estamos en 2026, el script buscará los PDFs de este año.
URLS_EVENTOS = {
    "SCY": "https://www.usms.org/events/national-championships/pool-national-championships/national-qualifying-times", # Página general
    # Si tienes la URL directa del PDF 2026, pégala aquí abajo en vez de None
    "PDF_DIRECTO_SCY": None, 
    "PDF_DIRECTO_LCM": None
}

def clean_time(time_str):
    """Convierte '24.50' o '1:05.20' a segundos (float)"""
    try:
        time_str = time_str.strip().replace('*', '') # A veces tienen asteriscos
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3: # Horas (ej: 1650 libre)
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(time_str)
    except:
        return None

def parsear_pdf_usms(pdf_bytes, curso):
    data_to_insert = []
    current_year = "2026" # O el año que detecte el PDF
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"   📄 Analizando {len(pdf.pages)} páginas del PDF ({curso})...")
        
        for page in pdf.pages:
            # Extraer tablas. Los PDFs de USMS suelen ser muy ordenados.
            # La tabla suele tener: Evento | 18-24 | 25-29 | ...
            tables = page.extract_tables()
            
            for table in tables:
                # Buscamos la fila de cabecera con las edades
                header_idx = -1
                age_groups = []
                
                for i, row in enumerate(table):
                    # Limpieza de nulos
                    row = [col if col else '' for col in row]
                    # Detectar si es la fila de edades (buscamos "18-24")
                    if "18-24" in " ".join(row):
                        header_idx = i
                        age_groups = row # Guardamos las columnas de edades
                        print(f"      ✅ Cabecera detectada: {age_groups[:5]}...")
                        break
                
                if header_idx == -1: continue # No es una tabla de tiempos
                
                # Procesar filas de datos (debajo de la cabecera)
                for row in table[header_idx+1:]:
                    row = [col if col else '' for col in row]
                    if len(row) < 2: continue
                    
                    event_name = row[0].replace('\n', ' ').strip() # Ej: "50 Free"
                    if not event_name or "RELAY" in event_name.upper(): continue
                    
                    # Detectar Estilo y Distancia
                    estilo = "Unknown"
                    distancia = 0
                    
                    # Parsing básico del nombre del evento
                    parts = event_name.split()
                    if parts[0].isdigit():
                        distancia = int(parts[0])
                        estilo_raw = " ".join(parts[1:]).upper()
                    else:
                        continue # No es una fila de evento válida
                        
                    # Traducción de estilos al español (como tu DB)
                    if "FREE" in estilo_raw: estilo = "Libre"
                    elif "BACK" in estilo_raw: estilo = "Espalda"
                    elif "BREAST" in estilo_raw: estilo = "Pecho"
                    elif "FLY" in estilo_raw: estilo = "Mariposa"
                    elif "IM" in estilo_raw or "INDIVIDUAL" in estilo_raw: estilo = "Combinado"
                    
                    # Iterar por las columnas de edad
                    for col_idx, time_val in enumerate(row):
                        if col_idx == 0: continue # Saltamos la col de nombre
                        if col_idx >= len(age_groups): break
                        
                        age_range = age_groups[col_idx].replace('\n', '').strip()
                        if not age_range or not time_val: continue
                        if "NO TIME" in time_val.upper(): continue
                        
                        tiempo_segundos = clean_time(time_val)
                        
                        if tiempo_segundos:
                            data_to_insert.append({
                                "ciclo": current_year,
                                "genero": "X", # USMS PDFs suelen separar tablas por género. Ver nota abajo*
                                "edad": age_range,
                                "estilo": estilo,
                                "distancia_m": distancia, # Ojo: si es SCY, esto son yardas. Convertir a metros visualmente en el front o guardar unidad.
                                "curso": curso, # SCY o LCM
                                "nivel": "NQT",
                                "tiempo_s": tiempo_segundos,
                                "standard_type": "MASTERS",
                                "season_year": current_year
                            })
                            
    return data_to_insert

def ejecutar_cazador():
    print("🦈 Iniciando Cazador de Masters USMS...")
    
    # NOTA: Como obtener la URL del PDF dinámicamente es difícil sin navegador visual,
    # RECOMIENDO: Busca en Google "USMS Spring Nationals 2026 NQT PDF" y pega el link aquí.
    # Si no, usaremos estos de ejemplo del 2025 para probar la estructura:
    
    # Ejemplo PDF 2025 (Spring - Yardas)
    pdf_url = "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/2025%20spring%20nationals/2025%20usms%20spring%20nationals%20nqts%20v2.pdf"
    
    try:
        print(f"⬇️ Descargando PDF: {pdf_url}...")
        response = requests.get(pdf_url)
        if response.status_code == 200:
            datos = parsear_pdf_usms(response.content, "SCY")
            print(f"✅ Se extrajeron {len(datos)} registros.")
            
            # NOTA IMPORTANTE: Los PDFs de USMS suelen tener PRIMERO Mujeres y LUEGO Hombres
            # o páginas separadas. El script básico asignó 'X'. 
            # Para producción, necesitaríamos lógica extra para detectar "WOMEN" o "MEN" en el título de la página.
            # Por ahora, los insertaré como prueba, pero REVISA esto en tu Supabase.
            
            # Inserción por lotes
            if datos:
                # Limpiar tabla de masters viejos si quieres
                # supabase.table("standards_usa").delete().eq("standard_type", "MASTERS").execute()
                
                batch_size = 100
                for i in range(0, len(datos), batch_size):
                    batch = datos[i:i+batch_size]
                    supabase.table("standards_usa").insert(batch).execute()
                    print(f"   Inyectado lote {i}-{i+len(batch)}")
                
        else:
            print("❌ Error descargando PDF")
            
    except Exception as e:
        print(f"⚠️ Error crítico: {e}")

if __name__ == "__main__":
    ejecutar_cazador()
