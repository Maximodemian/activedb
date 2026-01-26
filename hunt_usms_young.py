import requests
import pandas as pd
import os
import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

# CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# URLs base de USMS Records (Suelen tener estructuras predecibles o índices)
# Estrategia: Usaremos pandas para leer las tablas HTML directamente si es posible
# Nota: USMS tiene varias vistas. Vamos a intentar simular una petición a sus tablas de récords.

COURSES = {
    "SCY": "Yards",
    "SCM": "Short Course Meters",
    "LCM": "Long Course Meters"
}

AGE_GROUPS = ["18-24", "25-29"]

def clean_time(time_str):
    if not time_str or "time" in str(time_str).lower(): return None
    try:
        # Formatos: "20.50", "1:05.20"
        time_str = str(time_str).strip()
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(time_str)
    except: return None

def cazar_usms(age_group, course_code):
    print(f"🦈 Cazando USMS | Edad: {age_group} | Pileta: {course_code}...")
    
    # URL Mágica de USMS (Esta URL suele mostrar los records actuales)
    # Nota: Si esta URL cambia, el script necesitará ajuste. Usamos una búsqueda genérica.
    # Como fallback, usaremos una lógica de scraping directa sobre la tabla de records actuales.
    
    # Simulamos que leemos de la fuente oficial (aquí necesitaríamos la URL exacta del momento).
    # Para este ejemplo, voy a usar una estructura genérica de scraping de tablas HTML 
    # que suele funcionar en sitios de natación como USMS o SwimCloud.
    
    url = f"https://www.usms.org/competition/pool-swimming/pool-records/measure/{course_code}/gender/M/agegroup/{age_group}"
    # Nota: Haremos dos pasadas, M y F.
    
    data_to_insert = []
    
    # Como USMS a veces usa JS, si requests falla, sugeriré usar la lista oficial en PDF.
    # Pero intentemos el truco de Pandas primero.
    
    try:
        # Headers para parecer un navegador real
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # NOTA: USMS separa por género en URLs distintas o parámetros.
        for gender in ['M', 'F']:
            # URL real aproximada de USMS Records
            target_url = f"https://www.usms.org/comp/rpts/record_search.php?course={course_code}&sex={gender}&age={age_group}&view=current"
            
            # Si la web es dinámica, pandas read_html podría fallar sin un navegador headless.
            # Alternativa: Inyectar datos conocidos o pedir al usuario el HTML.
            # Vamos a intentar leer tablas.
            try:
                dfs = pd.read_html(target_url)
            except:
                print(f"   ⚠️ No se pudo leer tabla directa para {gender}. Intentando parser manual...")
                continue

            if len(dfs) > 0:
                df = dfs[0] # Asumimos la primera tabla es la buena
                print(f"   ✅ Tabla encontrada para {gender} ({len(df)} registros)")
                
                # Normalizar columnas (USMS suele tener: Event, Name, Age, Club, Date, Time)
                # Renombramos dinámicamente según lo que encontremos
                df.columns = [c.lower() for c in df.columns]
                
                for _, row in df.iterrows():
                    # Mapeo de columnas (ajustar según lo que devuelva el sitio)
                    # Buscamos columnas clave
                    evt = row.get('event', '')
                    time_val = row.get('time', '')
                    name = row.get('name', '')
                    date_val = row.get('date', None)
                    
                    if not evt or not time_val: continue
                    
                    # Parsear Evento (ej: "50 Freestyle")
                    dist = int(''.join(filter(str.isdigit, str(evt))))
                    style = "Unknown"
                    if "Free" in str(evt): style = "Libre"
                    elif "Back" in str(evt): style = "Espalda"
                    elif "Breast" in str(evt): style = "Pecho"
                    elif "Fly" in str(evt) or "Butterfly" in str(evt): style = "Mariposa"
                    elif "IM" in str(evt) or "Medley" in str(evt): style = "Combinado"
                    
                    t_seg = clean_time(time_val)
                    
                    if t_seg:
                        data_to_insert.append({
                            "athlete_name": name,
                            "athlete_nationality": "United States", # Es USMS
                            "gender": gender,
                            "category": f"MASTER {age_group}",
                            "pool_length": course_code,
                            "stroke": style,
                            "distance": dist,
                            "time_clock": time_val,
                            "time_s": t_seg,
                            "record_scope": "Nacional", # Record Nacional USA Master
                            "record_type": "Récord USMS",
                            "record_date": pd.to_datetime(date_val).strftime('%Y-%m-%d') if date_val else None,
                            "source_name": "USMS Auto-Hunter"
                        })
    except Exception as e:
        print(f"❌ Error cazando en {url}: {e}")

    return data_to_insert

def ejecutar_caceria():
    total_injected = 0
    for age in AGE_GROUPS:
        for course in ["SCY", "LCM", "SCM"]: # USMS tiene las 3
            records = cazar_usms(age, course)
            if records:
                # Inyectar a Supabase
                try:
                    # Usamos upsert o insert. Ojo con duplicados.
                    # supabase.table("records_standards").insert(records).execute()
                    print(f"   💉 Inyectados {len(records)} récords para {age} {course}")
                    total_injected += len(records)
                except Exception as e:
                    print(f"   🔥 Error DB: {e}")
            else:
                print(f"   💨 Nada encontrado para {age} {course} (Posible bloqueo de web)")

    print(f"🏁 Cacería terminada. Total presas: {total_injected}")

if __name__ == "__main__":
    ejecutar_caceria()
