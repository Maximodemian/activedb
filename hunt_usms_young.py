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

TARGET_AGE_GROUPS = ["18-24", "25-29", "30-34", "35-39"]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.usms.org/'
}

def clean_time_seconds(time_str):
    """Convierte string de tiempo a segundos flotantes"""
    if not time_str or pd.isna(time_str): return None
    try:
        t_str = str(time_str).split()[0].strip() # Quita 'NV' u otros sufijos
        t_str = ''.join(c for c in t_str if c.isdigit() or c in ['.', ':'])
        
        if ':' in t_str:
            parts = t_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(t_str)
    except: return None

def format_time_clock(seconds):
    """Convierte segundos flotantes a formato SQL TIME (00:MM:SS.ms)"""
    if seconds is None: return None
    try:
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        # Formato HH:MM:SS.ms
        return "{:02d}:{:02d}:{:05.2f}".format(int(h), int(m), s)
    except: return None

def cazar_records_usms(age_group, course_code, gender_code):
    print(f"🦈 Cazando USMS | Edad: {age_group} | Pileta: {course_code} | Sexo: {gender_code}...")
    url = f"https://www.usms.org/comp/poolrecords.php?ri=i&course={course_code}&age={age_group}&sex={gender_code}"
    
    data_to_insert = []
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200: return []
        dfs = pd.read_html(io.StringIO(r.text))
        if not dfs: return []

        for df in dfs:
            df.columns = [str(c).lower().strip() for c in df.columns]
            if 'event' not in df.columns or 'time' not in df.columns: continue

            count_table = 0
            for _, row in df.iterrows():
                evt = row.get('event', '')
                time_raw = row.get('time', '')
                name = row.get('name', '')
                date_val = row.get('date', None)
                
                if pd.isna(evt) or pd.isna(time_raw) or "Relay" in str(evt): continue
                
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
                
                # Conversiones de Tiempo
                t_seg = clean_time_seconds(time_raw)
                
                if t_seg:
                    t_ms = int(t_seg * 1000)
                    t_clock = format_time_clock(t_seg) # <--- LA SOLUCIÓN
                    
                    record = {
                        "athlete_name": name,
                        "athlete_nationality": "United States",
                        "gender": gender_code,
                        "category": f"MASTER {age_group}",
                        "pool_length": course_code,
                        "stroke": style,
                        "distance": dist,
                        
                        # Guardamos el formato SQL correcto
                        "time_clock": t_clock, 
                        "time_ms": t_ms,
                        
                        "record_scope": "MASTER",
                        "record_type": "Récord USMS",
                        "record_date": pd.to_datetime(date_val).strftime('%Y-%m-%d') if date_val and str(date_val) != 'nan' else None,
                        "source_name": "USMS Hunt V3.4",
                        "country": "United States"
                    }
                    data_to_insert.append(record)
                    count_table += 1
            
            if count_table > 0:
                print(f"      ✅ Récords preparados: {count_table}")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        
    return data_to_insert

def ejecutar_caceria():
    total_injected = 0
    for age in TARGET_AGE_GROUPS:
        for course in ["SCY", "LCM", "SCM"]:
            for sex in ["M", "F"]:
                records = cazar_records_usms(age, course, sex)
                
                success_count = 0
                for r in records:
                    try:
                        # Chequeo manual de existencia
                        existing = supabase.table("records_standards").select("id")\
                            .eq("category", r['category'])\
                            .eq("gender", r['gender'])\
                            .eq("pool_length", r['pool_length'])\
                            .eq("stroke", r['stroke'])\
                            .eq("distance", r['distance'])\
                            .eq("record_type", "Récord USMS")\
                            .execute()
                        
                        if existing.data:
                            # Update
                            supabase.table("records_standards").update(r).eq("id", existing.data[0]['id']).execute()
                        else:
                            # Insert
                            supabase.table("records_standards").insert(r).execute()
                        success_count += 1
                    except Exception as e_row:
                        print(f"         ⚠️ Error en fila {r['distance']} {r['stroke']}: {e_row}")
                
                if success_count > 0:
                    print(f"      💉 Procesados {success_count} registros.")
                    total_injected += success_count

    print(f"\n🏆 Misión Cumplida V3.4. Total procesados: {total_injected}")

if __name__ == "__main__":
    ejecutar_caceria()
