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

# 2. DICCIONARIOS DE TRADUCCIÓN (Cerebro del Bot)
TRADUCCION_ESTILOS = {
    "FREESTYLE": "Libre",
    "BACKSTROKE": "Espalda",
    "BREASTSTROKE": "Pecho",
    "BUTTERFLY": "Mariposa",
    "INDIVIDUAL MEDLEY": "Combinado",
    "MEDLEY": "Combinado",
    "IM": "Combinado"
}

# Mapeo de lo que dice la Web (URL) -> Lo que dice tu columna record_scope
MAPEO_SCOPE_DB = {
    "WR": "MUNDIAL",
    "WJ": "MUNDIAL",     # O 'JUNIOR' si lo separas
    "OR": "OLIMPICO",
    "PAN": "PANAMERICANO",
    "SAM": "SUDAMERICANO"
}

def clean_time_to_ms(t_str):
    """Convierte tiempos de la web a milisegundos para comparar con tu DB"""
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

def procesar_categoria(page, record_type, piscina):
    """Navega y actualiza una categoría específica (ej: PAN en 50m)"""
    scope_db = MAPEO_SCOPE_DB.get(record_type)
    url_wa = f"https://www.worldaquatics.com/swimming/records?recordType={record_type}&piscina={piscina}"
    
    print(f"\n🚀 Sincronizando: {scope_db} ({piscina})")
    
    try:
        page.goto(url_wa, wait_until="networkidle", timeout=60000)
        page.mouse.wheel(0, 1500) # Asegura carga de elementos
        time.sleep(3)

        # Buscamos las tarjetas de récords
        palabras_clave = ["FREESTYLE", "BACKSTROKE", "BREASTSTROKE", "BUTTERFLY", "MEDLEY", "IM"]
        records_procesados = set()

        for clave in palabras_clave:
            items = page.get_by_text(clave).all()
            for item in items:
                try:
                    card_text = item.locator("xpath=./..").inner_text()
                    parts = [p.strip() for p in card_text.split('\n') if p.strip()]
                    
                    if len(parts) < 4: continue
                    
                    header = parts[0] # Ej: "MEN 100M FREESTYLE"
                    if header in records_procesados: continue
                    records_procesados.add(header)

                    # Datos básicos
                    genero = "M" if "MEN" in header and "WOMEN" not in header else "W"
                    distancia_parts = header.split('M')[0].split(' ')
                    distancia = int(distancia_parts[-1]) if distancia_parts[-1].isdigit() else None
                    estilo_db = next((v for k, v in TRADUCCION_ESTILOS.items() if k in header), None)
                    
                    if not distancia or not estilo_db: continue

                    atleta = parts[2]
                    tiempo_clock = parts[3]
                    ms_web = clean_time_to_ms(tiempo_clock)
                    competencia = parts[4]

                    # BUSCAR EN TU TABLA DE SUPABASE
                    res = supabase.table("records_standards")\
                        .select("id, time_ms")\
                        .eq("gender", genero)\
                        .eq("distance", distancia)\
                        .eq("stroke", estilo_db)\
                        .eq("record_scope", scope_db)\
                        .eq("pool_length", piscina)\
                        .execute()

                    if res.data:
                        record_db = res.data[0]
                        # Solo actualiza si el tiempo de la web es más rápido
                        if ms_web and ms_web < record_db['time_ms']:
                            print(f"🔥 ¡NUEVO RÉCORD! {header} -> {tiempo_clock}")
                            supabase.table("records_standards").update({
                                "athlete_name": atleta,
                                "time_clock": tiempo_clock,
                                "time_ms": ms_web,
                                "competition_name": competencia,
                                "last_updated": datetime.now().strftime("%Y-%m-%d"),
                                "verified": True
                            }).eq("id", record_db['id']).execute()
                        else:
                            print(f"✅ {header}: {tiempo_clock} (Sin cambios)")
                except: continue
    except Exception as e:
        print(f"⚠️ Error en categoría {record_type}: {e}")

def ejecutar_scrapper_completo():
    with sync_playwright() as p:
        print("🌐 Iniciando Motor de Récords Multifuente...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Aceptar cookies al inicio
        try:
            page.goto("https://www.worldaquatics.com/swimming/records")
            page.get_by_role("button", name="Accept Cookies").click(timeout=5000)
        except: pass

        # Matriz de búsqueda: Cubre todo lo que hay en tu tabla
        # (World Aquatics centraliza PAN y SAM en su base de datos de récords)
        tareas = [
            ("WR", "50m"), ("WR", "25m"),
            ("OR", "50m"),
            ("PAN", "50m"), ("PAN", "25m"),
            ("SAM", "50m"), ("SAM", "25m")
        ]

        for r_type, p_size in tareas:
            procesar_categoria(page, r_type, p_size)

        browser.close()
        print("\n🏁 Proceso de actualización masiva finalizado.")

if __name__ == "__main__":
    ejecutar_scrapper_completo()
