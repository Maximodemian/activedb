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

def procesar_categoria(page, record_type, piscina):
    url_wa = f"https://www.worldaquatics.com/swimming/records?recordType={record_type}&piscina={piscina}"
    print(f"\n🚀 Navegando a: {record_type} | Piscina: {piscina}")
    
    try:
        page.goto(url_wa, wait_until="networkidle", timeout=60000)
        page.mouse.wheel(0, 1500)
        time.sleep(3)

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
                    distancia_parts = header.split('M')[0].split(' ')
                    distancia = int(distancia_parts[-1]) if distancia_parts[-1].isdigit() else None
                    if not distancia: continue

                    estilo_db = next((v for k, v in TRADUCCION_ESTILOS.items() if k in header), None)
                    if not estilo_db: continue

                    atleta = parts[2]
                    tiempo_clock = parts[3]
                    ms_web = clean_time_to_ms(tiempo_clock)
                    competencia = parts[4]

                    # CONSULTA A SUPABASE
                    res = supabase.table("records_standards")\
                        .select("id, time_ms")\
                        .eq("gender", genero)\
                        .eq("distance", distancia)\
                        .eq("stroke", estilo_db)\
                        .eq("record_scope", record_type)\
                        .eq("pool_length", piscina)\
                        .execute()

                    if res.data:
                        record_db = res.data[0]
                        if ms_web and ms_web < record_db['time_ms']:
                            print(f"🔥 ACTUALIZANDO {record_type} {piscina}: {header} -> {tiempo_clock}")
                            supabase.table("records_standards").update({
                                "athlete_name": atleta,
                                "time_clock": tiempo_clock,
                                "time_ms": ms_web,
                                "competition_name": competencia,
                                "last_updated": datetime.now().strftime("%Y-%m-%d")
                            }).eq("id", record_db['id']).execute()
                        else:
                            print(f"✅ {header} ({record_type}-{piscina}) OK.")
                except: continue
    except Exception as e:
        print(f"⚠️ Error en {record_type}-{piscina}: {e}")

def ejecutar_limpieza_total():
    with sync_playwright() as p:
        print("🌐 Iniciando motor de actualización total...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # MATRIZ DE CONFIGURACIONES
        configuraciones = [
            ("WR", "50m"), ("WR", "25m"),
            ("WJ", "50m"), ("WJ", "25m"),
            ("OR", "50m")  # Récords Olímpicos solo en 50m
        ]

        for r_type, p_size in configuraciones:
            procesar_categoria(page, r_type, p_size)

        browser.close()
        print("\n🏁 ¡Sincronización completa de toda la tabla!")

if __name__ == "__main__":
    ejecutar_limpieza_total()
