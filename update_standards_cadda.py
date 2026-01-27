import os
import requests
import pdfplumber
import io
import re
import datetime
import argparse
from decimal import Decimal
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Mapeo de columnas (Esto puede variar según el PDF, aquí definimos un estándar común en CADDA)
# A veces ponen: [Prueba, Cadete 1, Cadete 2, Juvenil 1...]
# Este script asume que la columna 0 es la PRUEBA.

def time_to_seconds(time_str):
    """Convierte '2:15.50' o '59.80' a segundos (float/decimal)."""
    if not time_str: return None
    clean = str(time_str).strip().replace("'", "").replace('"', '')
    try:
        if ':' in clean:
            parts = clean.split(':')
            return float(parts[0]) * 60 + float(parts[1])
        return float(clean)
    except ValueError:
        return None

def normalize_event(text):
    """Limpia el nombre de la prueba: '50 mts. Mariposa' -> '50 Mariposa'"""
    if not text: return None
    text = text.upper().replace("MTS", "").replace("METROS", "").replace(".", "").strip()
    
    # Unificar estilos
    text = text.replace("CROL", "LIBRE").replace("FREE", "LIBRE")
    text = text.replace("BACK", "ESPALDA")
    text = text.replace("BREAST", "PECHO")
    text = text.replace("FLY", "MARIPOSA")
    text = text.replace("COMBINADO", "IM").replace("MEDLEY", "IM")
    
    # Regex para asegurar formato "DISTANCIA ESTILO"
    match = re.search(r'(\d+)\s+([A-Z]+)', text)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return None

def process_pdf_standards(pdf_url, event_name, tipo_corte, pool_length, gender_default=None):
    print(f"⬇️ Descargando reglamento: {pdf_url}")
    response = requests.get(pdf_url)
    if response.status_code != 200:
        print(f"❌ Error descargando PDF: {response.status_code}")
        return

    records_to_insert = []
    
    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        print(f"📄 Analizando {len(pdf.pages)} páginas...")
        
        for page in pdf.pages:
            tables = page.extract_tables()
            
            for table in tables:
                # Análisis de Cabecera (Heurística simple)
                # Buscamos filas que definan categorías en las columnas
                header_row = None
                category_map = {} # {indice_columna: 'NOMBRE_CATEGORIA'}
                
                # Intentamos detectar la cabecera (fila con palabras clave de categorías)
                for idx, row in enumerate(table):
                    row_text = [str(c).upper() for c in row if c]
                    if any(x in str(row_text) for x in ['CADETE', 'JUVENIL', 'MAYOR', 'OPEN', 'MENOR']):
                        header_row = row
                        # Mapear columnas a categorías
                        for col_idx, cell in enumerate(row):
                            if cell:
                                clean_cat = cell.replace("\n", " ").upper().strip()
                                # Ignoramos la columna de la prueba
                                if "PRUEBA" not in clean_cat and "DISTANCIA" not in clean_cat:
                                    category_map[col_idx] = clean_cat
                        break
                
                if not category_map:
                    print("⚠️ No se detectaron categorías en esta tabla, saltando...")
                    continue

                # Procesar Filas de Datos
                for row in table:
                    # La columna 0 suele ser la prueba
                    raw_event = row[0]
                    event = normalize_event(raw_event)
                    
                    if not event: continue # Si no es una prueba válida (ej: basura o encabezado), saltar
                    
                    # Iterar sobre las columnas mapeadas
                    for col_idx, category in category_map.items():
                        if col_idx < len(row):
                            raw_time = row[col_idx]
                            seconds = time_to_seconds(raw_time)
                            
                            if seconds:
                                # Construir el objeto para clasificacion_corte
                                record = {
                                    "nombre_evento": event_name,
                                    "tipo_corte": tipo_corte,
                                    "categoria": category,
                                    "genero": gender_default if gender_default else "X", # 'X' si el PDF mezcla y no lo detectamos
                                    "prueba": event,
                                    "piscina": pool_length,
                                    "tiempo_s": seconds,
                                    "tiempo_display": str(raw_time).strip(),
                                    "temporada": datetime.datetime.now().year
                                }
                                records_to_insert.append(record)

    # Inserción en Supabase
    if records_to_insert:
        print(f"🚀 Insertando {len(records_to_insert)} cortes clasificatorios...")
        try:
            # Insertar en lotes de 50
            batch_size = 50
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i+batch_size]
                supabase.table("clasificacion_corte").insert(batch).execute()
            print("✅ Inserción exitosa.")
        except Exception as e:
            print(f"❌ Error insertando en DB: {e}")
    else:
        print("zzz No se extrajeron datos válidos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scraper de Marcas Clasificatorias CADDA')
    parser.add_argument('--url', required=True, help='URL del PDF del reglamento')
    parser.add_argument('--name', required=True, help='Nombre del Evento (ej: Nacional Open 2025)')
    parser.add_argument('--type', default='Marca Mínima', help='Tipo de corte (ej: Marca A, Marca B)')
    parser.add_argument('--pool', default='LCM', help='Piscina (LCM/SCM)')
    parser.add_argument('--gender', default='M/F', help='Género si el PDF es específico (M o F)')

    args = parser.parse_args()
    
    # Manejo si el usuario pone "M/F", el script intentará deducirlo o duplicará, 
    # pero para simplificar la V1, asumamos que el usuario corre el script una vez por PDF 
    # o que el PDF tiene columnas claras.
    
    process_pdf_standards(args.url, args.name, args.type, args.pool, args.gender if args.gender in ['M', 'F'] else None)
