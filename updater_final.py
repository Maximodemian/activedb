import os
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

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
    try:
        t_str = t_str.strip()
        if ":" in t_str:
            parts = t_str.split(":")
            if len(parts) == 2:
                m, rest = parts
                s, c = rest.split(".")
                return (int(m) * 60000) + (int(s) * 1000) + (int(c) * 10)
        else:
            s, c = t_str.split(".")
            return (int(s) * 1000) + (int(c) * 10)
    except: return None

def scrap_categoria(page, record_type, piscina):
    url = f"https://www.worldaquatics.com/swimming/records?recordType={record_type}&piscina={piscina}"
    print(f"🚀 Navegando a {record_type} en {piscina}...")
    page.goto(url, wait_until="networkidle", timeout=60000)
    
    # Pequeño scroll para asegurar carga
    page.mouse.wheel(0, 1000)
    time.sleep(2)

    palabras_clave = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY", "IM"]
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
                distancia_str = header.split('M')[0].split(' ')[-1]
                if not distancia_str.isdigit(): continue
                distancia = int(distancia_str)
                estilo_db = next((v for k, v in TRADUCCION_ESTILOS.items() if k in header), None)
                
                if not estilo_db: continue

                atleta = parts[2]
                tiempo_clock = parts[3]
                ms_web = clean_time_to_ms(tiempo_clock)
                competencia = parts[4]

                # Ajustamos la consulta para incluir el alcance (WR/WJ) y la piscina
                # Asegúrate de tener estas columnas en tu tabla
                res = supabase.table("records_standards")\
                    .select("id, time_ms")\
                    .eq("gender", genero)\
                    .eq("distance", distancia)\
                    .eq("stroke", estilo_db)\
                    .eq("record_scope", record_type)\
                    .eq("pool_length", piscina)\
                    .execute()

                if res.data:
                    db_rec = res.data[0]
                    if ms_web < db_rec['time_ms']:
                        print(f"🔥 ¡NUEVO! {record_type} {piscina} - {header}: {tiempo_clock}")
                        supabase.table("records_standards").update({
                            "athlete_name": atleta,
                            "time_clock": tiempo_clock,
                            "time_ms": ms_web,
                            "competition_name": competencia,
                            "last_updated": datetime.now().strftime("%Y-%m-%d")
                        }).eq("id", db_rec['id']).execute()
            except: continue

def ejecutar_todo():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Configuraciones a recorrer
        tipos = ["WR", "WJ"] # World Records y World Junior
        piscinas = ["50m", "25m"] # LCM y SCM
        
        for t in tipos:
            for p_len in piscinas:
                scrap_categoria(page, t, p_len)
        
        browser.close()

if __name__ == "__main__":
    ejecutar_todo()
