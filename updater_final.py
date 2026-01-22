import os
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

# 1. Configuración
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Mapeo: Texto de la Web -> ID en tu base de datos (Ejemplo)
MAPEO_RECORDS = {
    "MEN 50M FREESTYLE": 1,
    "MEN 100M FREESTYLE": 2,
    "MEN 200M FREESTYLE": 3,
    "MEN 400M FREESTYLE": 4,
    "MEN 800M FREESTYLE": 5,
    "MEN 1500M FREESTYLE": 6,
}

def clean_time_to_ms(t_str):
    """Convierte 46.40 o 01:42.00 a milisegundos"""
    try:
        if ":" in t_str:
            parts = t_str.split(":")
            m = int(parts[0])
            s, c = parts[1].split(".")
        else:
            m = 0
            s, c = t_str.split(".")
        return (m * 60000) + (int(s) * 1000) + (int(c) * 10)
    except: return None

def ejecutar_actualizacion_total():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.worldaquatics.com/swimming/records?recordType=WR&piscina=50m", wait_until="networkidle")
        
        # Aceptar cookies
        try: page.get_by_role("button", name="Accept Cookies").click(timeout=5000)
        except: pass

        items = page.get_by_text("FREESTYLE").all()
        
        for item in items:
            try:
                card_text = item.locator("xpath=./..").inner_text()
                parts = [p.strip() for p in card_text.split('\n') if p.strip()]
                
                # Estructura detectada: [PRUEBA, PAIS, ATLETA, TIEMPO, COMPETENCIA, ...]
                nombre_prueba = parts[0]
                atleta = parts[2]
                tiempo_clock = parts[3]
                competencia = parts[4]
                
                if nombre_prueba in MAPEO_RECORDS:
                    id_db = MAPEO_RECORDS[nombre_prueba]
                    ms_web = clean_time_to_ms(tiempo_clock)
                    
                    # Consultar DB
                    res = supabase.table("records_standards").select("time_ms").eq("id", id_db).execute()
                    if res.data:
                        ms_db = res.data[0]['time_ms']
                        
                        if ms_web < ms_db:
                            print(f"🔥 ¡NUEVO RÉCORD! {nombre_prueba}: {atleta} ({tiempo_clock})")
                            supabase.table("records_standards").update({
                                "athlete_name": atleta,
                                "time_clock": tiempo_clock,
                                "time_ms": ms_web,
                                "competition_name": competencia,
                                "last_updated": datetime.now().strftime("%Y-%m-%d")
                            }).eq("id", id_db).execute()
                        else:
                            print(f"✅ {nombre_prueba} está al día.")
            except Exception as e:
                continue
        
        browser.close()

if __name__ == "__main__":
    ejecutar_actualizacion_total()
