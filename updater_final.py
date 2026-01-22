import os
import time
import smtplib
from email.message import EmailMessage
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Credenciales de Email
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")

# Variables de auditoría
cambios_auditoria = []

# Mapeos de traducción
TRADUCCION_ESTILOS = {"FREESTYLE": "Libre", "BACKSTROKE": "Espalda", "BREASTSTROKE": "Pecho", "BUTTERFLY": "Mariposa", "MEDLEY": "Combinado", "IM": "Combinado"}
MAPEO_SCOPE_DB = {"WR": "MUNDIAL", "OR": "OLIMPICO", "PAN": "PANAMERICANO", "SAM": "SUDAMERICANO"}
MAPEO_PISCINA = {"50m": "LCM", "25m": "SCM"}

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

def enviar_reporte_mail(duracion):
    if not EMAIL_USER or not EMAIL_PASS:
        print("⚠️ No se enviará mail: Faltan credenciales.")
        return

    msg = EmailMessage()
    asunto = f"🏁 Reporte Scraper: {len(cambios_auditoria)} récords actualizados"
    msg['Subject'] = asunto
    msg['From'] = EMAIL_USER
    msg['To'] = "vorrabermauro@gmail.com"

    cuerpo = f"Hola Coach,\n\nEl motor terminó su recorrido en {duracion}s.\n\n"
    if cambios_auditoria:
        cuerpo += "DETALLE DE ACTUALIZACIONES:\n"
        for c in cambios_auditoria:
            cuerpo += f"✅ [{c['scope']}] {c['prueba']}: {c['atleta']} bajó el tiempo de {c['tiempo_anterior']} a {c['tiempo_nuevo']}\n"
    else:
        cuerpo += "No hubo cambios hoy. Los récords internacionales y nacionales coinciden con tu base de datos."
    
    cuerpo += "\n\nLos logs ya están disponibles en Supabase.\nAtentamente,\nTu Ferrari de Récords 🏎️💨"
    msg.set_content(cuerpo)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        print("📧 Mail enviado con éxito.")
    except Exception as e:
        print(f"❌ Error enviando mail: {e}")

def procesar_categoria_wa(page, record_type, piscina_web):
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

                    # Consulta Supabase
                    res = supabase.table("records_standards").select("*")\
                        .eq("gender", genero).eq("distance", distancia).eq("stroke", estilo_db)\
                        .eq("record_scope", scope_db).eq("pool_length", piscina_db).execute()

                    if res.data:
                        for record_db in res.data:
                            if ms_web and ms_web < record_db['time_ms']:
                                log_data = {
                                    "scope": scope_db,
                                    "prueba": f"{genero} {distancia}m {estilo_db}",
                                    "atleta": atleta,
                                    "tiempo_anterior": record_db['time_clock'],
                                    "tiempo_nuevo": tiempo_clock
                                }
                                cambios_auditoria.append(log_data)
                                
                                # Actualizar Registro
                                supabase.table("records_standards").update({
                                    "athlete_name": atleta, "time_clock": tiempo_clock, "time_ms": ms_web,
                                    "last_updated": datetime.now().strftime("%Y-%m-%d")
                                }).eq("id", record_db['id']).execute()
                                
                                # Guardar Log en Supabase
                                supabase.table("scraper_logs").insert(log_data).execute()
                except: continue
    except Exception as e:
        print(f"⚠️ Error en {scope_db}: {e}")

def ejecutar_scrapper_completo():
    start_time = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        tareas = [("WR", "50m"), ("WR", "25m"), ("OR", "50m"), ("PAN", "50m"), ("PAN", "25m"), ("SAM", "50m"), ("SAM", "25m")]
        for r_type, p_size in tareas:
            procesar_categoria_wa(page, r_type, p_size)

        browser.close()
    
    duracion = round(time.time() - start_time, 2)
    enviar_reporte_mail(duracion)

if __name__ == "__main__":
    ejecutar_scrapper_completo()
