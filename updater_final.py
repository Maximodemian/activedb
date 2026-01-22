import os
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

# 1. Configuración de conexión
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# 2. Diccionario de Traducción (Web -> Tu DB)
TRADUCCION_ESTILOS = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "INDIVIDUAL MEDLEY": "Combinado",
    "MEDLEY": "Combinado",
    "IM": "Combinado"
}

def clean_time_to_ms(t_str):
    """Convierte formatos '46.40', '01:42.00' o '14:30.67' a milisegundos"""
    try:
        t_str = t_str.strip()
        if ":" in t_str:
            parts = t_str.split(":")
            if len(parts) == 2: # MM:SS.cc
                m, rest = parts
                s, c = rest.split(".")
                return (int(m) * 60000) + (int(s) * 1000) + (int(c) * 10)
        else: # SS.cc
            s, c = t_str.split(".")
            return (int(s) * 1000) + (int(c) * 10)
    except: return None

def procesar_actualizacion():
    with sync_playwright() as p:
        print("🌐 Iniciando navegador invisible...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # URL de Récords Mundiales (Piscina 50m)
        url_wa = "https://www.worldaquatics.com/swimming/records?recordType=WR&piscina=50m"
        print(f"🚀 Navegando a {url_wa}...")
        page.goto(url_wa, wait_until="networkidle", timeout=60000)

        # Aceptar Cookies
        try:
            page.get_by_role("button", name="Accept Cookies").click(timeout=5000)
            print("🍪 Cookies aceptadas.")
            time.sleep(2)
        except: pass

        # Palabras clave para rastrear todas las pruebas de la tabla
        palabras_clave = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY", "IM"]
        
        records_procesados = set() # Para evitar duplicados en la misma ejecución

        for clave in palabras_clave:
            print(f"🔍 Buscando pruebas de tipo: {clave}...")
            # Buscamos elementos que contienen el texto de la prueba
            items = page.get_by_text(clave).all()
            
            for item in items:
                try:
                    # Subimos al contenedor padre para obtener toda la info de la tarjeta
                    card_text = item.locator("xpath=./..").inner_text()
                    parts = [p.strip() for p in card_text.split('\n') if p.strip()]
                    
                    if len(parts) < 4: continue
                    
                    header = parts[0] # Ej: "MEN 200M IM" o "WOMEN 100M BACKSTROKE"
                    
                    # Evitar procesar lo mismo varias veces
                    if header in records_procesados: continue
                    records_procesados.add(header)

                    # Determinar Género
                    genero = "M" if "MEN" in header and "WOMEN" not in header else "W"
                    
                    # Extraer Distancia (ej: de '400M' extrae 400)
                    distancia_str = header.split('M')[0].split(' ')[-1]
                    if not distancia_str.isdigit(): continue
                    distancia = int(distancia_str)
                    
                    # Traducir Estilo a tu formato de DB
                    estilo_db = next((v for k, v in TRADUCCION_ESTILOS.items() if k in header), None)
                    
                    if not estilo_db: continue

                    atleta = parts[2]
                    tiempo_clock = parts[3]
                    ms_web = clean_time_to_ms(tiempo_clock)
                    competencia = parts[4]

                    if ms_web is None: continue

                    # 3. BUSCAR EN SUPABASE
                    res = supabase.table("records_standards")\
                        .select("id, time_ms, athlete_name")\
                        .eq("gender", genero)\
                        .eq("distance", distancia)\
                        .eq("stroke", estilo_db)\
                        .eq("record_scope", "WR")\
                        .execute()

                    if res.data:
                        record_db = res.data[0]
                        # Si el tiempo de la web es menor al de la DB, actualizamos
                        if ms_web < record_db['time_ms']:
                            print(f"🔥 ¡NUEVO RÉCORD DETECTADO! {header}")
                            print(f"   Anterior: {record_db['time_ms']}ms | Nuevo: {ms_web}ms ({tiempo_clock})")
                            
                            supabase.table("records_standards").update({
                                "athlete_name": atleta,
                                "time_clock": tiempo_clock,
                                "time_ms": ms_web,
                                "competition_name": competencia,
                                "last_updated": datetime.now().strftime("%Y-%m-%d"),
                                "verified": True
                            }).eq("id", record_db['id']).execute()
                        else:
                            print(f"✅ {header}: {tiempo_clock} (Al día)")
                except Exception as e:
                    continue

        print("🏁 Proceso finalizado con éxito.")
        browser.close()

if __name__ == "__main__":
    procesar_actualizacion()
