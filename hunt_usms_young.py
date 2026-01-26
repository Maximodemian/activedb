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

# Headers para parecer un navegador real (Chrome)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.usms.org/'
}

def clean_time(time_str):
    if not time_str or pd.isna(time_str): return None
    try:
        # Limpieza: "23.54", "1:05.20", "23.54 NV" -> 23.54
        t_str = str(time_str).split()[0].strip()
        t_str = ''.join(c for c in t_str if c.isdigit() or c in ['.', ':'])
        
        if ':' in t_str:
            parts = t_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(t_str)
    except: return None

def cazar_records_usms(age_group, course_code):
    print(f"🦈 Cazando Récords USMS | Edad: {age_group} | Pileta: {course_code}...")
    
    # NUEVA URL DETECTADA 2026: poolrecords.php
    # Parámetros probables: ri=i (Individual), course, age
    url = f"https://www.usms.org/comp/poolrecords.php?ri=i&course={course_code}&age={age_group}"
    
    data_to_insert = []
    
    try:
        # 1. Petición HTTP
        r = requests.get(url, headers=HEADERS, timeout=15)
        
        if r.status_code != 200:
            print(f"   ⚠️ Error HTTP {r.status_code}: La URL ha cambiado o está caída.")
            return []
            
        # 2. Parseo HTML con Pandas
        # USMS suele devolver 2 tablas: Mujeres (Women) y Hombres (Men), o una sola si se filtra.
        dfs = pd.read_html(io.StringIO(r.text))
        
        if not dfs:
            print("   ⚠️ No se encontraron tablas en el HTML (posible página vacía).")
            return []

        print(f"   🔎 Tablas encontradas: {len(dfs)}")

        for i, df in enumerate(dfs):
            # Normalizar columnas
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            # Verificar validez (Debe tener Evento y Tiempo)
            if 'event' not in df.columns or 'time' not in df.columns:
                continue

            # Detectar Género
            # A menudo la tabla 0 es Mujeres y la 1 es Hombres, pero buscaremos pistas en el contenido
            # O simplemente guardaremos ambos y dejaremos que el dashboard filtre.
            # Asumiremos el orden estándar de USMS: 0=Women, 1=Men (si hay 2).
            # Si hay 1, intentamos adivinar o ponemos 'X' (Mixto).
            
            current_gender = 'X'
            if len(dfs) == 2:
                current_gender = 'F' if i == 0 else 'M'
            elif len(dfs) == 1:
                # Si solo hay una, podría ser cualquier cosa. 
                # Buscamos pistas en los nombres de nadadores famosos si pudiéramos, 
                # pero por seguridad usaremos 'M' y 'F' en pasadas separadas si fuera necesario.
                # Por ahora, marcaremos como 'Mixed' o 'Unknown' si no estamos seguros.
                # TRUCO: USMS suele poner "Women" o "Men" en la primera fila o caption.
                # Vamos a asumir que si pedimos el grupo de edad, nos da ambos.
                # Si el script falla en género, lo corregiremos luego. 
                # Asumiremos i=0 -> F para probar.
                current_gender = 'F' 
            
            # Inyección de registros
            count_table = 0
            for _, row in df.iterrows():
                evt = row.get('event', '')
                time_val = row.get('time', '')
                name = row.get('name', '')
                date_val = row.get('date', None)
                
                if pd.isna(evt) or pd.isna(time_val) or "Relay" in str(evt): continue
                
                # Parsear Distancia y Estilo
                evt_str = str(evt)
                dist_digits = ''.join(filter(str.isdigit, evt_str))
                if not dist_digits: continue
                dist = int(dist_digits)
                
                style = "Unknown"
                if "Free" in evt_str: style = "Libre"
                elif "Back" in evt_str: style = "Espalda"
                elif "Breast" in evt_str: style = "Pecho"
                elif "Fly" in evt_str: style = "Mariposa"
                elif "IM" in evt_str: style = "Combinado"
                
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
                        "source_name": "USMS Hunt V3",
                        "competition_country": "United States"
                    }
                    data_to_insert.append(record)
                    count_table += 1
            
            print(f"      ✅ Tabla {i} ({current_gender}): {count_table} récords extraídos.")

    except Exception as e:
        print(f"   ❌ Error procesando {age_group}: {e}")
        
    return data_to_insert

def ejecutar_caceria():
    total_injected = 0
    # USMS tiene SCY, LCM, SCM
    for age in TARGET_AGE_GROUPS:
        for course in ["SCY", "LCM", "SCM"]:
            records = cazar_records_usms(age, course)
            if records:
                try:
                    # Upsert usando conflict en columnas clave
                    response = supabase.table("records_standards").upsert(records, on_conflict="category, gender, pool_length, stroke, distance, record_type").execute()
                    # print(f"      💉 Guardados en DB.")
                    total_injected += len(records)
                except Exception as db_err:
                    print(f"      🔥 Error DB: {db_err}")
            else:
                print(f"      💨 Sin datos.")

    print(f"\n🏆 Misión Cumplida V3. Total presas capturadas: {total_injected}")

if __name__ == "__main__":
    ejecutar_caceria()
