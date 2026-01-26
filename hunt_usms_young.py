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

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.usms.org/'
}

def clean_time(time_str):
    if not time_str or pd.isna(time_str): return None
    try:
        t_str = str(time_str).split()[0].strip()
        t_str = ''.join(c for c in t_str if c.isdigit() or c in ['.', ':'])
        
        if ':' in t_str:
            parts = t_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(t_str)
    except: return None

def cazar_records_usms(age_group, course_code, gender_code):
    # gender_code: 'M' o 'F'
    print(f"🦈 Cazando USMS | Edad: {age_group} | Pileta: {course_code} | Sexo: {gender_code}...")
    
    # URL con parámetro de sexo explícito para asegurar que traiga la tabla correcta
    # Parameters: ri=i (Individual), course, age, sex (M/F)
    url = f"https://www.usms.org/comp/poolrecords.php?ri=i&course={course_code}&age={age_group}&sex={gender_code}"
    
    data_to_insert = []
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        
        if r.status_code != 200:
            print(f"   ⚠️ Error HTTP {r.status_code}")
            return []
            
        dfs = pd.read_html(io.StringIO(r.text))
        
        if not dfs:
            print("   ⚠️ No se encontraron tablas.")
            return []

        # Iteramos tablas (por si devuelve varias, aunque con el filtro sex debería ser una)
        for i, df in enumerate(dfs):
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            if 'event' not in df.columns or 'time' not in df.columns:
                continue

            # Usamos el género que pedimos en la URL
            current_gender = gender_code
            
            count_table = 0
            for _, row in df.iterrows():
                evt = row.get('event', '')
                time_val = row.get('time', '')
                name = row.get('name', '')
                date_val = row.get('date', None)
                
                if pd.isna(evt) or pd.isna(time_val) or "Relay" in str(evt): continue
                
                # Parsear Distancia
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
                        "source_name": "USMS Hunt V3.1",
                        
                        # CORRECCIÓN AQUÍ: Usamos 'country' en lugar de 'competition_country'
                        "country": "United States" 
                    }
                    data_to_insert.append(record)
                    count_table += 1
            
            if count_table > 0:
                print(f"      ✅ Tabla encontrada: {count_table} récords ({current_gender}).")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        
    return data_to_insert

def ejecutar_caceria():
    total_injected = 0
    # Iteramos también por Género para asegurar cobertura 100%
    for age in TARGET_AGE_GROUPS:
        for course in ["SCY", "LCM", "SCM"]:
            for sex in ["M", "F"]:
                records = cazar_records_usms(age, course, sex)
                if records:
                    try:
                        # Upsert
                        response = supabase.table("records_standards").upsert(records, on_conflict="category, gender, pool_length, stroke, distance, record_type").execute()
                        total_injected += len(records)
                    except Exception as db_err:
                        print(f"      🔥 Error DB: {db_err}")
                else:
                    pass # Silencioso si no encuentra para no ensuciar log

    print(f"\n🏆 Misión Cumplida V3.1. Total presas capturadas: {total_injected}")

if __name__ == "__main__":
    ejecutar_caceria()
