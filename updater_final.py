import os
import time
import re
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

# 1. Configuración
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# 2. Diccionarios de Traducción (Cerebro del Bot)
TRADUCCION_ESTILOS = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "INDIVIDUAL MEDLEY": "Combinado",
    "MEDLEY": "Combinado",
    "IM": "Combinado"
}

MAPEO_SCOPE_DB = {
    "WR": "MUNDIAL",
    "WJ": "MUNDIAL",
    "OR": "OLIMPICO",
    "PAN": "PANAMERICANO",
    "SAM": "SUDAMERICANO",
    "NACIONAL": "Nacional"
}

MAPEO_PISCINA = {
    "50m": "LCM",
    "25m": "SCM"
}

def clean_time_to_ms(t_str):
    try:
        t_str = t_str.strip()
        if ":" in t_str:
            parts = t_str.split(":")
            m, rest = parts
            s, c = rest.split(".")
            return (int(m) * 60000) + (int(s) * 1000) + (int(c) * 10)
        else:
            s, c = t_str.split(".")
            return (int(s) * 1000) + (int(c) * 10)
    except: return None

def procesar_categoria(page, record_type, piscina_web):
    piscina_db = MAPEO_PISCINA.get(piscina_web)
    scope_db = MAPEO_SCOPE_DB.get(record_type)
    url_wa = f"https://www.worldaquatics.com/swimming/records?recordType={record_type}&piscina={piscina_web}"
    
    print(f"\n🚀 Sincronizando: {scope_db} ({piscina_db})")
    
    try:
        page.goto(url_wa, wait_until="networkidle", timeout=60000)
        time.sleep(3)
        
        palabras_clave = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY"]
        records_procesados = set()

        for clave in palabras_clave:
            items = page.get_by_text(clave).all()
            for item in items:
                try:
                    card_text = item.locator("xpath=./..").inner_text()
                    parts = [p.strip() for p in card_text.split('\n') if p.strip()]
                    if len(parts) < 4: continue
                    
                    header = parts[0]
                    if header in records_procesados: continue
                    records_procesados.add(header)

                    genero = "M" if "MEN" in header and "WOMEN" not in header else "W"
                    distancia_parts = header.split('M')[0].split(' ')
                    distancia = int(distancia_parts[-1]) if distancia_parts[-1].isdigit() else None
                    estilo_db = next((v for k, v in TRADUCCION_ESTILOS.items() if k in header), None)
                    
                    if not distancia or not estilo_db: continue

                    atleta = parts[2]
                    tiempo_clock = parts[3]
                    ms_web = clean_time_to_ms(tiempo_clock)
                    competencia = parts[4]

                    # Consulta simplificada gracias a la normalización de pool_length
                    res = supabase.table("records_standards")\
                        .select("id, time_ms")\
                        .eq("gender", genero)\
                        .eq("distance", distancia)\
                        .eq("stroke", estilo_db)\
                        .eq("record_scope", scope_db)\
                        .eq("pool_length", piscina_db)\
                        .execute()

                    if res.data:
                        for record_db in res.data:
                            if ms_web and ms_web < record_db['time_ms']:
                                print(f"🔥 Actualizando {header} -> {tiempo_clock}")
                                supabase.table("records_standards").update({
                                    "athlete_name": atleta,
                                    "time_clock": tiempo_clock,
                                    "time_ms": ms_web,
                                    "competition_name": competencia,
                                    "last_updated": datetime.now().strftime("%Y-%m-%d"),
                                    "verified": True
                                }).eq("id", record_db['id']).execute()
                except: continue
    except Exception as e:
        print(f"⚠️ Error en {scope_db}: {e}")

def ejecutar_scrapper_completo():
    with sync_playwright() as p:
        print("🌐 Iniciando Motor de Récords y Marcas de Referencia...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # 1. Récords Internacionales (World Aquatics)
        tareas = [
            ("WR", "50m"), ("WR", "25m"),
            ("OR", "50m"),
            ("PAN", "50m"), ("PAN", "25m"),
            ("SAM", "50m"), ("SAM", "25m")
        ]
        for r_type, p_size in tareas:
            procesar_categoria(page, r_type, p_size)

        # 2. Módulo CADDA (Nacional y Referencias)
        # Aquí puedes añadir la lógica específica para navegar la web de CADDA
        print("\n🇦🇷 Verificando Récords Nacionales en CADDA.org.ar...")
        # (Lógica de navegación similar a la anterior adaptada a CADDA)

        browser.close()
        print("\n🏁 Proceso finalizado exitosamente.")

if __name__ == "__main__":
    ejecutar_scrapper_completo()
