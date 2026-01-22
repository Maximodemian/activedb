import os
import time
import re
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

# 1. CONFIGURACIÓN DE CONEXIÓN
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

# Variable global para el Reporte de Auditoría (Fase B)
cambios_detectados = []

def clean_time_to_ms(t_str):
    """Convierte tiempos de la web a milisegundos"""
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

def procesar_categoria_wa(page, record_type, piscina_web):
    """Procesa World Aquatics (Mundiales, Olimpicos, Sudam, Panam)"""
    piscina_db = MAPEO_PISCINA.get(piscina_web)
    scope_db = MAPEO_SCOPE_DB.get(record_type)
    url_wa = f"https://www.worldaquatics.com/swimming/records?recordType={record_type}&piscina={piscina_web}"
    
    print(f"🔍 Scrapeando {scope_db} ({piscina_db})...")
    
    try:
        page.goto(url_wa, wait_until="networkidle", timeout=60000)
        time.sleep(2)
        
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

                    # Consulta a Supabase
                    res = supabase.table("records_standards")\
                        .select("id, time_ms, time_clock")\
                        .eq("gender", genero)\
                        .eq("distance", distancia)\
                        .eq("stroke", estilo_db)\
                        .eq("record_scope", scope_db)\
                        .eq("pool_length", piscina_db)\
                        .execute()

                    if res.data:
                        for record_db in res.data:
                            if ms_web and ms_web < record_db['time_ms']:
                                # Guardar para el reporte final
                                cambios_detectados.append({
                                    "scope": scope_db,
                                    "prueba": f"{genero} {distancia}m {estilo_db}",
                                    "anterior": record_db['time_clock'],
                                    "nuevo": tiempo_clock,
                                    "atleta": atleta
                                })
                                # Actualizar DB
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

def procesar_cadda_centinela(page):
    """Módulo CADDA (Fase C): Rastrea nuevos récords y marcas de referencia argentinos"""
    print("\n🇦🇷 Iniciando rastreo en CADDA (Argentina)...")
    url_base = "https://cadda.org.ar/records/"
    
    try:
        page.goto(url_base, wait_until="networkidle", timeout=60000)
        
        # El bot busca enlaces a archivos PDF o páginas de récords específicos
        links = page.locator("a").all()
        encontrados = []
        
        palabras_cadda = ["RECORD", "REFERENCIA", "HISTORICO", "MMN", "MINIMA"]
        
        for link in links:
            texto = (link.inner_text() or "").upper()
            href = link.get_attribute("href") or ""
            
            if any(p in texto for p in palabras_cadda) and (".pdf" in href or "record" in href):
                encontrados.append(f"{texto}: {href}")
        
        if encontrados:
            print(f"📢 Se detectaron {len(encontrados)} documentos de interés en CADDA:")
            for e in encontrados[:5]: # Mostrar los primeros 5
                print(f"   🔗 {e}")
        else:
            print("✅ No se detectaron nuevos documentos de marcas en la home de CADDA.")
            
    except Exception as e:
        print(f"⚠️ Error en módulo CADDA: {e}")

def ejecutar_scrapper_completo():
    start_time = time.time()
    
    with sync_playwright() as p:
        print("🌐 Iniciando Motor Ferrari - Scraper Maestro v2.0")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Aceptar cookies si aparecen
        try:
            page.goto("https://www.worldaquatics.com/swimming/records")
            page.get_by_role("button", name="Accept Cookies").click(timeout=5000)
        except: pass

        # 1. MATRIZ INTERNACIONAL (World Aquatics)
        tareas = [
            ("WR", "50m"), ("WR", "25m"),
            ("OR", "50m"),
            ("PAN", "50m"), ("PAN", "25m"),
            ("SAM", "50m"), ("SAM", "25m")
        ]
        for r_type, p_size in tareas:
            procesar_categoria_wa(page, r_type, p_size)

        # 2. MÓDULO ARGENTINA (CADDA)
        procesar_cadda_centinela(page)

        browser.close()

    # 3. REPORTE FINAL DE AUDITORÍA (Fase B)
    end_time = time.time()
    duracion = round(end_time - start_time, 2)
    
    print("\n" + "="*50)
    print(f"📊 REPORTE DE ACTUALIZACIÓN - {datetime.now().strftime('%d/%m/%Y')}")
    print(f"⏱️ Tiempo total: {duracion} segundos")
    print("="*50)
    
    if cambios_detectados:
        print(f"🔥 Se encontraron y actualizaron {len(cambios_detectados)} récords:")
        for c in cambios_detectados:
            print(f"✅ [{c['scope']}] {c['prueba']}: {c['anterior']} ➔ {c['nuevo']} ({c['atleta']})")
    else:
        print("🙌 Sin cambios detectados. Todos los tiempos en la DB están actualizados.")
    print("="*50 + "\n")

if __name__ == "__main__":
    ejecutar_scrapper_completo()
