import requests
import pandas as pd
import os
import io
import datetime
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Franjas "jóvenes" objetivo
TARGET_AGE_GROUPS = ["18-24", "25-29", "30-34", "35-39"]

# Headers para engañar al servidor de USMS (Anti-Scraping básico)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.usms.org/'
}

def clean_time(time_str):
    if not time_str or pd.isna(time_str): return None
    try:
        # Limpieza: "23.54", "1:05.20", "23.54 NV" -> 23.54
        t_str = str(time_str).split()[0].strip()
        # Eliminar caracteres raros si quedan
        t_str = ''.join(c for c in t_str if c.isdigit() or c in ['.', ':'])
        
        if ':' in t_str:
            parts = t_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(t_str)
    except: return None

def cazar_records_usms(age_group, course_code):
    print(f"🦈 Cazando Récords USMS | Edad: {age_group} | Pileta: {course_code}...")
    
    # URL Oficial de Reportes USMS
    url = f"https://www.usms.org/comp/rpts/record_search.php?course={course_code}&age={age_group}&view=current"
    
    data_to_insert = []
    
    try:
        # 1. PETICIÓN CON MÁSCARA (Requests)
        r = requests.get(url, headers=HEADERS, timeout=15)
        
        if r.status_code != 200:
            print(f"   ⚠️ Bloqueo o Error HTTP: {r.status_code}")
            return []
            
        # 2. PARSEO CON PANDAS (Sobre el texto descargado)
        # Usamos io.StringIO para que pandas crea que es un archivo
        dfs = pd.read_html(io.StringIO(r.text))
        
        if not dfs:
            print("   ⚠️ No se encontraron tablas en el HTML.")
            return []

        # USMS suele poner Hombres y Mujeres en tablas separadas o juntas.
        # Iteramos todas las tablas encontradas.
        for i, df in enumerate(dfs):
            # Normalizar columnas
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            # Verificar si es una tabla válida de tiempos (debe tener 'event' y 'time')
            if 'event' not in df.columns or 'time' not in df.columns:
                continue

            # Detectar Género
            # A veces está en el título de la tabla anterior (difícil con pandas) 
            # o implícito. USMS suele poner Women (Tabla 0) y Men (Tabla 1)
            current_gender = 'F' if i == 0 else 'M'
            # Si solo hay 1 tabla, es arriesgado, pero asumiremos F para 0.
            # Mejora: Buscar palabras clave en el contenido si fuera necesario.
            
            print(f"   ✅ Tabla {i} ({current_gender}) detectada: {len(df)} filas.")
            
            for _, row in df.iterrows():
                evt = row.get('event', '')
                time_val = row.get('time', '')
                name = row.get('name', '')
                date_val = row.get('date', None)
                
                # Validación básica
                if pd.isna(evt) or pd.isna(time_val) or "Relay" in str(evt): 
                    continue
                
                # Parsear Evento
                evt_str = str(evt)
                dist_digits = ''.join(filter(str.isdigit, evt_str))
                if not dist_digits: continue
                dist = int(dist_digits)
                
                style = "Unknown"
                if "Free" in evt_str: style = "Libre"
                elif "Back" in evt_str: style = "Espalda"
                elif "Breast" in evt_str: style = "Pecho"
                elif "Fly" in evt_str or "Butter" in evt_str: style = "Mariposa"
                elif "IM" in evt_str or "Medley" in evt_str: style = "Combinado"
                
                t_seg = clean_time(time_val)
                
                if t_seg:
                    record = {
                        "athlete_name": name,
                        "athlete_nationality": "United States",
                        "gender": current_gender,
                        "category": f"MASTER {age_group}",
                        "pool_length": course_code,
                        "stroke": style,
                        "distance": dist,
                        "time_clock": str(time_val).strip(),
                        "time_s": t_seg,
                        "record_scope": "MASTER",
                        "record_type": "Récord USMS",
                        "record_date": pd.to_datetime(date_val).strftime('%Y-%m-%d') if date_val and str(date_val) != 'nan' else None,
                        "source_name": "USMS Official",
                        "competition_country": "United States"
                    }
                    data_to_insert.append(record)

    except Exception as e:
        print(f"   ❌ Error procesando {age_group}: {e}")
        
    return data_to_insert

def ejecutar_caceria():
    total_injected = 0
    # SCY = Yardas, SCM = Metros Corta, LCM = Metros Larga
    # USMS tiene datos para los 3.
    for age in TARGET_AGE_GROUPS:
        for course in ["SCY", "LCM", "SCM"]:
            records = cazar_records_usms(age, course)
            if records:
                try:
                    # Insertar en lotes
                    response = supabase.table("records_standards").upsert(records, on_conflict="category, gender, pool_length, stroke, distance, record_type").execute()
                    print(f"      💉 Guardados {len(records)} registros.")
                    total_injected += len(records)
                except Exception as db_err:
                    print(f"      🔥 Error DB: {db_err}")
            else:
                print(f"      💨 Sin datos útiles.")

    print(f"\n🏆 Misión Cumplida. Total presas capturadas: {total_injected}")

if __name__ == "__main__":
    ejecutar_caceria()
