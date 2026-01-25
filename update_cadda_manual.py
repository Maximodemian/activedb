import os
import requests
import pdfplumber
import io
import re
import sys
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Mapeo de Jerarquía: Si el usuario elige un Torneo, definimos el tipo de marca por defecto
TYPE_MAPPING = {
    'NACIONAL_ABSOLUTO': 'MINIMA',
    'NACIONAL_REPUBLICA': 'MINIMA',
    'NACIONAL_HAROLD': 'HAROLD_BARRIOS',
    'NACIONAL_MASTER': 'TIEMPO_TOPE',
    'REGIONAL_PROMOCIONAL': 'PROMOCIONAL'
}

def clean_time(time_str):
    """Convierte tiempos 1:05.20 o 35.20 a segundos (float)"""
    try:
        # Limpieza de basura común en PDFs (asteriscos, espacios)
        time_str = str(time_str).strip().replace('*', '').replace("'", "")
        if not time_str or time_str == '-': return None
        
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3: 
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(time_str)
    except:
        return None

def detectar_estilo_distancia(texto):
    """Intenta extraer '100 Libre' de un texto sucio"""
    texto = texto.upper().replace('\n', ' ')
    
    distancia = None
    # Buscar número al inicio (ej: "100")
    match_num = re.search(r'\b(50|100|200|400|800|1500)\b', texto)
    if match_num:
        distancia = int(match_num.group(1))
    
    estilo = "Unknown"
    if "LIBRE" in texto or "CROL" in texto or "FREE" in texto: estilo = "Libre"
    elif "ESPALDA" in texto or "BACK" in texto: estilo = "Espalda"
    elif "PECHO" in texto or "BREAST" in texto: estilo = "Pecho"
    elif "MARIPOSA" in texto or "FLY" in texto: estilo = "Mariposa"
    elif "COMBINADO" in texto or "IM" in texto or "MEDLEY" in texto: estilo = "Combinado"
    
    return distancia, estilo

def parsear_pdf_cadda(pdf_url, target_meet, season_year, curso):
    print(f"🦈 Iniciando Inyector CADDA para: {target_meet} ({season_year})")
    print(f"⬇️ Descargando: {pdf_url}")
    
    tipo_marca_default = TYPE_MAPPING.get(target_meet, 'MINIMA')
    data_to_insert = []
    
    try:
        response = requests.get(pdf_url)
        if response.status_code != 200:
            print("❌ Error descargando PDF")
            return []
            
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            print(f"   📄 Páginas encontradas: {len(pdf.pages)}")
            
            for p_idx, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables: continue
                
                print(f"   🔎 Analizando página {p_idx + 1}...")
                
                for table in tables:
                    # 1. Detectar Cabeceras (Categorías)
                    # Buscamos filas que tengan palabras clave como "MENOR", "CADETE", "JUVENIL", "PRIMERA"
                    header_map = {} # {col_index: 'NOMBRE_CATEGORIA'}
                    start_row_idx = -1
                    
                    for r_idx, row in enumerate(table):
                        row_str = " ".join([str(c).upper() for c in row if c])
                        # Palabras clave de categorías CADDA
                        if any(x in row_str for x in ['MENOR', 'CADETE', 'JUVENIL', 'JUNIOR', 'PRIMERA', 'MAYORES', 'MASTER']):
                            start_row_idx = r_idx
                            # Mapeamos qué columna es qué categoría
                            for c_idx, cell in enumerate(row):
                                if cell and any(x in str(cell).upper() for x in ['MENOR', 'CADETE', 'JUVENIL', 'JUNIOR', 'PRIMERA', 'MAYORES', 'MASTER']):
                                    # Limpiamos el nombre (ej: "Menores\nVarones" -> "MENORES")
                                    cat_name = str(cell).replace('\n', ' ').strip().upper()
                                    header_map[c_idx] = cat_name
                            break
                    
                    if not header_map: 
                        # Si no encontramos cabecera de categorías, quizás es una tabla simple (Evento | Categoria | Tiempo)
                        # Por ahora saltamos, pero aquí se podría agregar lógica para tablas verticales.
                        continue 

                    print(f"      ✅ Cabeceras detectadas: {list(header_map.values())}")

                    # 2. Leer Filas de Tiempos
                    current_gender = 'M' # Default, pero intentaremos detectar si dice "MUJERES" o "VARONES" antes
                    
                    # Buscamos pistas de género en el texto de la página antes de la tabla
                    page_text = page.extract_text().upper()
                    if "MUJERES" in page_text and "VARONES" not in page_text: current_gender = 'F'
                    elif "NIÑAS" in page_text: current_gender = 'F'
                    
                    for row in table[start_row_idx+1:]:
                        row_clean = [col if col else '' for col in row]
                        if len(row_clean) < 2: continue
                        
                        # La primera columna suele ser el Evento (ej: "100 Libre")
                        texto_evento = str(row_clean[0])
                        distancia, estilo = detectar_estilo_distancia(texto_evento)
                        
                        if not distancia or estilo == "Unknown": continue
                        
                        # Recorremos las columnas mapeadas
                        for col_idx, cat_name in header_map.items():
                            if col_idx >= len(row_clean): break
                            
                            time_val = row_clean[col_idx]
                            tiempo_seg = clean_time(time_val)
                            
                            if tiempo_seg:
                                data_to_insert.append({
                                    "tipo_marca": tipo_marca_default,
                                    "categoria": cat_name,
                                    "estilo": estilo,
                                    "distancia_m": distancia,
                                    "curso": curso, # 'LCM' o 'SCM' (Pasado como argumento)
                                    "tiempo_s": tiempo_seg,
                                    "target_meet": target_meet,
                                    "año": season_year,
                                    "genero": current_gender 
                                    # OJO: El género es lo más difícil de adivinar en CADDA si no está en la tabla.
                                    # Se recomienda correr el script dos veces (una para PDF mujeres, una para hombres)
                                    # o confiar en la detección de texto.
                                })
                                
    except Exception as e:
        print(f"⚠️ Error crítico: {e}")
        
    return data_to_insert

if __name__ == "__main__":
    # Obtener argumentos desde GitHub Action (Variables de Entorno)
    PDF_URL = os.environ.get("INPUT_PDF_URL")
    TARGET_MEET = os.environ.get("INPUT_TARGET_MEET", "NACIONAL_ABSOLUTO")
    SEASON = os.environ.get("INPUT_SEASON", "2025-2026")
    CURSO = os.environ.get("INPUT_CURSO", "LCM") # LCM (50m) o SCM (25m)
    GENDER_FORCE = os.environ.get("INPUT_GENDER", "AUTO") # 'M', 'F' o 'AUTO'

    if not PDF_URL:
        print("❌ Debes proporcionar una URL de PDF (INPUT_PDF_URL)")
        sys.exit(1)

    datos = parsear_pdf_cadda(PDF_URL, TARGET_MEET, SEASON, CURSO)
    
    # Filtrado forzoso de género si el usuario lo pidió
    if GENDER_FORCE != 'AUTO':
        for d in datos: d['genero'] = GENDER_FORCE

    print(f"📊 Se extrajeron {len(datos)} registros.")
    
    if datos:
        # Inserción (Opcional: borrar datos previos de ese torneo/año para evitar duplicados)
        # supabase.table("standards_cadda").delete().match({"target_meet": TARGET_MEET, "año": SEASON}).execute()
        
        batch_size = 100
        for i in range(0, len(datos), batch_size):
            batch = datos[i:i+batch_size]
            supabase.table("standards_cadda").insert(batch).execute()
            print(f"   💉 Inyectado lote {i}-{i+len(batch)}")
            
        print("✅ ¡Operación Exitosa!")
    else:
        print("⚠️ No se encontraron datos válidos. Revisa la URL o el formato del PDF.")
