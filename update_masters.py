import os
import requests
import pdfplumber
import io
import smtplib
import datetime
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Configuración de Email (Usa los nombres de secretos que ya tengas en GitHub)
EMAIL_SENDER = os.environ.get("MAIL_USERNAME") or os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("MAIL_PASSWORD") or os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = "vorrabermauro@gmail.com"  # Tu correo personal

# Estadísticas Globales para el Reporte
STATS = {
    "extracted": 0,
    "inserted": 0,
    "male": 0,
    "female": 0,
    "errors": 0
}

def enviar_reporte_email(log_body, status="SUCCESS"):
    """Envía el reporte final por email"""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        print("⚠️ No se encontraron credenciales de email. Saltando envío.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = "Bot de Natación <" + EMAIL_SENDER + ">"
        msg['To'] = EMAIL_RECEIVER

        icon = "🟢" if status == "SUCCESS" else "🔴"
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg['Subject'] = f"{icon} Reporte USMS Masters - {timestamp}"

        body = f"""
        Hola Mauro,

        Aquí tienes el resultado de la ejecución automática de marcas Masters (USMS).

        --------------------------------------------------
        REPORTE DE EJECUCIÓN
        --------------------------------------------------
        Version: USMS_MASTERS_v4.0_AUTO_GENDER
        Timestamp: {timestamp}

        {log_body}

        --------------------------------------------------
        Detalles Técnicos:
        - Origen: PDF Oficial USMS
        - Motor: PDFPlumber + VelocityCheck
        - Destino: Supabase (Tabla: standards_usa)

        Saludos,
        Tu Ferrari de Datos 🏎️
        """

        msg.attach(MIMEText(body, 'plain'))

        # Conexión a Gmail
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, text)
        server.quit()
        print("📧 Email de reporte enviado correctamente.")

    except Exception as e:
        print(f"❌ Error enviando email: {e}")
        traceback.print_exc()

def clean_time(time_str):
    """
    Normaliza y convierte un string de tiempo a segundos (float).
    Devuelve None si no puede parsear el valor.
    Maneja formatos: SS.sss, M:SS.ss, H:MM:SS, también acepta coma decimal.
    """
    try:
        if time_str is None:
            return None
        s = str(time_str).strip()
        if s == "":
            return None
        # Eliminar caracteres no deseados y normalizar coma decimal
        s = s.replace('*', '').replace('+', '').replace(',', '.')
        # Si contiene ':', tratar por partes
        if ':' in s:
            parts = [p.strip() for p in s.split(':') if p.strip() != ""]
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60.0 + seconds
            elif len(parts) == 3:
                hours = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                return hours * 3600.0 + minutes * 60.0 + seconds
            else:
                return None
        # Intentar conversión directa a float (segundos)
        return float(s)
    except Exception:
        traceback.print_exc()
        return None

def extraer_tiempo_testigo(table):
    """
    Intenta extraer un tiempo 'testigo' de la tabla para detectar género (ej: 50 Free de 18-24).
    Devuelve un float (segundos) o un valor grande por defecto si no se encuentra.
    """
    try:
        header_idx = -1
        for i, row in enumerate(table):
            row_str = " ".join([str(c) for c in row if c])
            if "18-24" in row_str:
                header_idx = i
                break

        if header_idx == -1:
            return 9999.0

        for row in table[header_idx+1:]:
            # Asegurar que la fila tenga al menos 2 columnas
            if not row or len(row) < 2:
                continue
            row_clean = [str(c).replace('\n', ' ').strip().upper() for c in row if c]
            if not row_clean:
                continue
            evt = row_clean[0]
            if "50" in evt and "FREE" in evt:
                time_val = row[1]
                t_seg = clean_time(time_val)
                if t_seg is not None:
                    return t_seg
    except Exception:
        traceback.print_exc()
    return 9999.0

def procesar_tablas_inteligente(pdf_bytes, curso):
    data_to_insert = []
    current_year = str(datetime.datetime.now().year)  # Año actual dinámico

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"   📄 Analizando PDF ({curso})...")

        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            mapa_generos = {}

            if len(tables) >= 2:
                t0 = extraer_tiempo_testigo(tables[0])
                t1 = extraer_tiempo_testigo(tables[1])

                print(f"      🥊 Comparando: Tabla 0 ({t0}s) vs Tabla 1 ({t1}s)")

                if t0 < t1 and t0 > 0:
                    mapa_generos[0], mapa_generos[1] = 'M', 'F'
                elif t1 < t0 and t1 > 0:
                    mapa_generos[0], mapa_generos[1] = 'F', 'M'
                else:
                    mapa_generos[0], mapa_generos[1] = 'F', 'M'
            else:
                page_text = (page.extract_text() or "").upper()
                if "WOMEN" in page_text and "MEN" not in page_text:
                    mapa_generos[0] = 'F'
                elif "MEN" in page_text and "WOMEN" not in page_text:
                    mapa_generos[0] = 'M'
                else:
                    mapa_generos[0] = 'F'

            for i, table in enumerate(tables):
                genero = mapa_generos.get(i, 'X')

                header_idx = -1
                age_groups = []
                for idx_row, row in enumerate(table):
                    row_str = " ".join([str(c) for c in row if c])
                    if "18-24" in row_str:
                        header_idx = idx_row
                        age_groups = row
                        break

                if header_idx == -1:
                    continue

                for row in table[header_idx+1:]:
                    # Normalizar fila
                    row = [col if col else '' for col in row]
                    if len(row) < 2:
                        continue

                    event_name = str(row[0]).replace('\n', ' ').strip()
                    if not event_name or "RELAY" in event_name.upper():
                        continue

                    parts = event_name.split()
                    if not parts or not parts[0].isdigit():
                        continue
                    distancia = int(parts[0])
                    estilo_raw = " ".join(parts[1:]).upper()

                    estilo = "Unknown"
                    if "FREE" in estilo_raw:
                        estilo = "Libre"
                    elif "BACK" in estilo_raw:
                        estilo = "Espalda"
                    elif "BREAST" in estilo_raw:
                        estilo = "Pecho"
                    elif "FLY" in estilo_raw:
                        estilo = "Mariposa"
                    elif "IM" in estilo_raw:
                        estilo = "Combinado"

                    for col_idx, time_val in enumerate(row):
                        if col_idx == 0:
                            continue
                        if col_idx >= len(age_groups):
                            break

                        age_range = str(age_groups[col_idx]).replace('\n', '').strip()
                        if not age_range or "NO TIME" in str(time_val).upper():
                            continue

                        t_seg = clean_time(time_val)
                        if t_seg is not None:
                            data_to_insert.append({
                                "ciclo": current_year,
                                "genero": genero,
                                "edad": age_range,
                                "estilo": estilo,
                                "distancia_m": distancia,
                                "curso": curso,
                                "nivel": "NQT",
                                "tiempo_s": t_seg,
                                "standard_type": "MASTERS",
                                "season_year": current_year
                            })
                            # Estadísticas de extracción
                            STATS["extracted"] += 1
                            if genero == 'M':
                                STATS["male"] += 1
                            elif genero == 'F':
                                STATS["female"] += 1

    return data_to_insert

def ejecutar_cazador():
    print("🦈 Iniciando Cazador de Masters USMS v4.0 (Con Reporte Email)...")
    # URL 2025
    pdf_url = "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/2025%20spring%20nationals/2025%20usms%20spring%20nationals%20nqts%20v2.pdf"

    try:
        response = requests.get(pdf_url)
        if response.status_code == 200:
            datos = procesar_tablas_inteligente(response.content, "SCY")

            if datos:
                # Borrado masivo anterior
                supabase.table("standards_usa").delete().eq("standard_type", "MASTERS").execute()
                print("🗑️  Datos antiguos eliminados.")

                # Inserción
                batch_size = 100
                total = len(datos)
                for i in range(0, total, batch_size):
                    batch = datos[i:i+batch_size]
                    supabase.table("standards_usa").insert(batch).execute()
                    print(f"   💉 Inyectado lote {i} a {min(i+batch_size, total)}")

                STATS["inserted"] = total

                # Generar Log Final para el Email
                log_final = f\"[USMS_MASTERS] Extracted={STATS['extracted']} | Inserted={STATS['inserted']} | Male={STATS['male']} | Female={STATS['female']} | Errors={STATS['errors']}\"
                print(\"\\n\" + log_final)

                # Enviar Email
                enviar_reporte_email(log_final, "SUCCESS")

            else:
                STATS['errors'] += 1
                enviar_reporte_email("No data extracted from PDF", "FAILURE")
        else:
            STATS['errors'] += 1
            error_msg = f"HTTP Error {response.status_code}"
            print(error_msg)
            enviar_reporte_email(error_msg, "FAILURE")

    except Exception as e:
        STATS['errors'] += 1
        print(f"⚠️ Error crítico: {e}")
        traceback.print_exc()
        enviar_reporte_email(f"Critical Exception: {str(e)}", "FAILURE")

if __name__ == "__main__":
    ejecutar_cazador()
