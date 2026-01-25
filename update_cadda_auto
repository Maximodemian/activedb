import os
import requests
import pdfplumber
import io
import re
import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Email Config
EMAIL_SENDER = os.environ.get("MAIL_USERNAME") or os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("MAIL_PASSWORD") or os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = "vorrabermauro@gmail.com"

# Palabras Clave para Clasificación
KEYWORDS_MEET = {
    'NACIONAL_HAROLD': ['INFANTIL', 'MENOR', 'HAROLD', 'BARRIOS'],
    'NACIONAL_REPUBLICA': ['REPUBLICA', 'REPÚBLICA', 'JUVENIL', 'CADETE'],
    'NACIONAL_ABSOLUTO': ['ABSOLUTO', 'MAYORES', 'OPEN', 'JUNIOR'],
    'NACIONAL_MASTER': ['MASTER', 'PREMASTER']
}

STATS = {"found": 0, "processed": 0, "inserted": 0, "skipped": 0, "errors": 0, "new_pdfs": []}

def enviar_reporte_email(log_body, status="SUCCESS"):
    if not EMAIL_SENDER or not EMAIL_PASSWORD: return
    try:
        msg = MIMEMultipart()
        msg['From'] = f"Bot CADDA <{EMAIL_SENDER}>"
        msg['To'] = EMAIL_RECEIVER
        icon = "🇦🇷🟢" if status == "SUCCESS" else "🇦🇷🔴"
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        msg['Subject'] = f"{icon} Reporte Automático CADDA - {timestamp}"
        
        body = f"""
        Hola Mauro,
        
        El rastreador de CADDA ha finalizado su patrulla diaria.
        
        RESUMEN DE OPERACIÓN:
        ---------------------
        PDFs Encontrados: {STATS['found']}
        PDFs Nuevos Procesados: {STATS['processed']}
        Registros Inyectados: {STATS['inserted']}
        
        DETALLE DE ACTIVIDAD:
        {log_body}
        
        Atentamente,
        Tu Ferrari Autónoma 🏎️
        """
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Error enviando email: {e}")

def get_cadda_pdfs():
    """Rastrea la web de CADDA en busca de PDFs nuevos en Informativas"""
    url = "https://cadda.org.ar/informativas/"
    pdfs = []
    try:
        print(f"🕷️ Rastreando {url}...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Buscamos todos los links que terminen en .pdf
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text().upper()
            if href.lower().endswith('.pdf'):
                # Filtro Básico: Solo nos interesan Reglamentos, Marcas, Convocatorias
                if any(x in href.upper() or text for x in ['REGLAMENTO', 'MARCAS', 'MINIMAS', 'TIEMPOS']):
                    pdfs.append(href)
    except Exception as e:
        print(f"Error crawling: {e}")
    return list(set(pdfs)) # Deduplicar

def analizar_y_clasificar(pdf_bytes):
    """Abre el PDF y decide qué torneo, año y pileta es"""
    info = {"meet": None, "year": str(datetime.datetime.now().year), "course": "LCM", "gender": "AUTO"}
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages: return None
        text = pdf.pages[0].extract_text().upper()
        
        # 1. Detectar Torneo
        for meet, kw_list in KEYWORDS_MEET.items():
            if any(k in text for k in kw_list):
                info["meet"] = meet
                break
        
        # 2. Detectar Año
        match_year = re.search(r'202[4-9]', text)
        if match_year: info["year"] = match_year.group(0)
        
        # 3. Detectar Pileta (Curso)
        if "25 M" in text or "PISCINA CORTA" in text: info["course"] = "SCM"
        else: info["course"] = "LCM" # Default Nacionales es Larga
        
        # 4. Detectar Género Exclusivo (Raro pero posible)
        if "MUJERES" in text and "VARONES" not in text: info["gender"] = "F"
        elif "VARONES" in text and "MUJERES" not in text: info["gender"] = "M"

    return info

def clean_time(time_str):
    try:
        time_str = str(time_str).strip().replace('*', '').replace("'", "")
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2: return float(parts[0])*60 + float(parts[1])
            elif len(parts) == 3: return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        return float(time_str)
    except: return None

def parsear_contenido(pdf_bytes, meta):
    data = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                header_map = {}
                start_row = -1
                
                # Detectar cabeceras
                for r_idx, row in enumerate(table):
                    row_str = " ".join([str(c).upper() for c in row if c])
                    if any(x in row_str for x in ['MENOR', 'CADETE', 'JUVENIL', 'JUNIOR', 'PRIMERA', 'MAYORES', 'MASTER']):
                        start_row = r_idx
                        for c_idx, cell in enumerate(row):
                            if cell:
                                cell_clean = str(cell).replace('\n', ' ').strip().upper()
                                if any(x in cell_clean for x in ['MENOR', 'CADETE', 'JUVENIL', 'JUNIOR', 'PRIMERA', 'MAYORES', 'MASTER']):
                                    header_map[c_idx] = cell_clean
                        break
                
                if not header_map: continue

                # Extraer datos
                for row in table[start_row+1:]:
                    row_clean = [c if c else '' for c in row]
                    if len(row_clean) < 2: continue
                    
                    evt_text = str(row_clean[0]).upper()
                    # Detectar Estilo/Distancia (Simplificado)
                    dist = 0
                    m = re.search(r'\b(50|100|200|400|800|1500)\b', evt_text)
                    if m: dist = int(m.group(1))
                    else: continue

                    estilo = "Unknown"
                    if "LIBRE" in evt_text or "CROL" in evt_text: estilo = "Libre"
                    elif "ESPALDA" in evt_text: estilo = "Espalda"
                    elif "PECHO" in evt_text: estilo = "Pecho"
                    elif "MARIPOSA" in evt_text: estilo = "Mariposa"
                    elif "COMBINADO" in evt_text or "MEDLEY" in evt_text: estilo = "Combinado"
                    
                    for col_idx, cat in header_map.items():
                        if col_idx < len(row_clean):
                            t_val = clean_time(row_clean[col_idx])
                            if t_val:
                                data.append({
                                    "tipo_marca": "MINIMA", # Default para scraping auto
                                    "categoria": cat,
                                    "estilo": estilo,
                                    "distancia_m": dist,
                                    "curso": meta["course"],
                                    "tiempo_s": t_val,
                                    "target_meet": meta["meet"],
                                    "año": meta["year"],
                                    "genero": "M" if meta["gender"] == "AUTO" else meta["gender"] 
                                    # Nota: CADDA mezcla géneros. Asumimos M por defecto o requiere doble pasada.
                                })
    return data

def run_auto_spider():
    print("🕷️ Iniciando Spider CADDA...")
    log_messages = []
    
    # 1. Buscar Links
    pdf_links = get_cadda_pdfs()
    STATS['found'] = len(pdf_links)
    log_messages.append(f"- URLs encontradas: {len(pdf_links)}")
    
    for url in pdf_links:
        # 2. Verificar si ya se procesó
        res = supabase.table("processed_docs").select("*").eq("url", url).execute()
        if res.data:
            print(f"⏭️ Saltando (Ya procesado): {url.split('/')[-1]}")
            STATS['skipped'] += 1
            continue
            
        try:
            print(f"⬇️ Descargando: {url}...")
            resp = requests.get(url)
            if resp.status_code == 200:
                # 3. Clasificar
                meta = analizar_y_clasificar(resp.content)
                if not meta or not meta["meet"]:
                    print("⚠️ No se pudo clasificar el PDF (No parece reglamento relevante).")
                    supabase.table("processed_docs").insert({"url": url, "status": "IGNORED", "info": "No classification"}).execute()
                    continue
                
                print(f"✅ Clasificado: {meta['meet']} | {meta['year']} | {meta['course']}")
                
                # 4. Parsear
                datos = parsear_contenido(resp.content, meta)
                
                if datos:
                    # 5. Inyectar
                    # Opcional: Borrar datos previos de ese torneo/año si es una actualización
                    # supabase.table("standards_cadda").delete().match({"target_meet": meta["meet"], "año": meta["year"]}).execute()
                    
                    # Insertar en lotes
                    batch_size = 100
                    for i in range(0, len(datos), batch_size):
                        supabase.table("standards_cadda").insert(datos[i:i+batch_size]).execute()
                    
                    STATS['inserted'] += len(datos)
                    STATS['processed'] += 1
                    STATS['new_pdfs'].append(f"{meta['meet']} ({len(datos)} regs)")
                    
                    # Marcar como procesado
                    supabase.table("processed_docs").insert({"url": url, "status": "SUCCESS", "info": f"{meta['meet']} {meta['year']}"}).execute()
                    log_messages.append(f"🟢 Procesado: {url.split('/')[-1]} -> {meta['meet']}")
                else:
                    print("⚠️ PDF válido pero sin tabla de tiempos legible.")
                    supabase.table("processed_docs").insert({"url": url, "status": "EMPTY", "info": "No data rows"}).execute()
                    
        except Exception as e:
            print(f"❌ Error procesando {url}: {e}")
            STATS['errors'] += 1

    # Reporte Final
    final_log = "\n".join(log_messages)
    if STATS['processed'] > 0 or STATS['errors'] > 0:
        enviar_reporte_email(final_log, "SUCCESS" if STATS['errors'] == 0 else "WARNING")
    else:
        print("💤 Sin novedades en el frente.")

if __name__ == "__main__":
    run_auto_spider()
