import os
import requests
import pdfplumber
import io
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def clean_time(time_str):
    """Convierte tiempos a segundos de forma robusta"""
    try:
        time_str = str(time_str).strip().replace('*', '').replace('+', '')
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3: 
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(time_str)
    except:
        return None

def extraer_tiempo_testigo(table):
    """
    Busca el tiempo de '50 Free' en la primera columna de edad (usualmente 18-24)
    para usarlo como comparador de velocidad.
    """
    try:
        # Buscamos índice de columna de edad y fila de evento
        header_idx = -1
        for i, row in enumerate(table):
            row_str = " ".join([str(c) for c in row if c])
            if "18-24" in row_str:
                header_idx = i
                break
        
        if header_idx == -1: return 9999.0 # Tabla inválida

        # Buscamos la fila "50 Free"
        for row in table[header_idx+1:]:
            row_clean = [str(c).replace('\n', ' ').strip().upper() for c in row if c]
            if not row_clean: continue
            
            # Nombre del evento (columna 0)
            evt = row_clean[0]
            if "50" in evt and "FREE" in evt:
                # Tomamos el primer tiempo (columna 1, que suele ser 18-24)
                time_val = row[1] # Asumimos columna 1 es la primera edad
                t_seg = clean_time(time_val)
                if t_seg: return t_seg
                
    except:
        pass
    return 9999.0 # Si falla, devolvemos un tiempo muy alto

def procesar_tablas_inteligente(pdf_bytes, curso):
    data_to_insert = []
    current_year = "2025"
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"   📄 Analizando PDF ({curso})...")
        
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables: continue
            
            # --- ESTRATEGIA: BIOLOGÍA VS TEXTO ---
            # Si hay 2 tablas en la página, aplicamos la Ley de Velocidad
            mapa_generos = {} # {indice_tabla: 'M' o 'F'}
            
            if len(tables) >= 2:
                t0_testigo = extraer_tiempo_testigo(tables[0])
                t1_testigo = extraer_tiempo_testigo(tables[1])
                
                print(f"      🥊 Comparando velocidades: Tabla 0 ({t0_testigo}s) vs Tabla 1 ({t1_testigo}s)")
                
                if t0_testigo < t1_testigo and t0_testigo > 0:
                    mapa_generos[0] = 'M'
                    mapa_generos[1] = 'F'
                    print("      ✅ Conclusión: Tabla 0 es HOMBRES (Más rápido)")
                elif t1_testigo < t0_testigo and t1_testigo > 0:
                    mapa_generos[0] = 'F'
                    mapa_generos[1] = 'M'
                    print("      ✅ Conclusión: Tabla 0 es MUJERES (Más lento)")
                else:
                    print("      ⚠️ No se pudo determinar por velocidad. Usando orden por defecto.")
                    mapa_generos[0] = 'F'
                    mapa_generos[1] = 'M'
            else:
                # Si hay solo 1 tabla, buscamos texto en la página
                page_text = page.extract_text().upper()
                if "WOMEN" in page_text and "MEN" not in page_text:
                    mapa_generos[0] = 'F'
                elif "MEN" in page_text and "WOMEN" not in page_text:
                    mapa_generos[0] = 'M'
                else:
                    mapa_generos[0] = 'F' # Default conservador
            
            # --- PROCESAMIENTO ---
            for i, table in enumerate(tables):
                genero = mapa_generos.get(i, 'X') # 'X' si falló todo (para auditar)
                
                # (Aquí va la misma lógica de extracción de filas que ya tenías)
                # ... Lógica de cabecera ...
                header_idx = -1
                age_groups = []
                for idx_row, row in enumerate(table):
                    row_str = " ".join([str(c) for c in row if c])
                    if "18-24" in row_str:
                        header_idx = idx_row
                        age_groups = row 
                        break
                
                if header_idx == -1: continue

                for row in table[header_idx+1:]:
                    row = [col if col else '' for col in row]
                    if len(row) < 2: continue
                    
                    event_name = str(row[0]).replace('\n', ' ').strip()
                    if not event_name or "RELAY" in event_name.upper(): continue
                    
                    # Parsing Evento
                    parts = event_name.split()
                    if not parts[0].isdigit(): continue
                    distancia = int(parts[0])
                    estilo_raw = " ".join(parts[1:]).upper()
                    
                    estilo = "Unknown"
                    if "FREE" in estilo_raw: estilo = "Libre"
                    elif "BACK" in estilo_raw: estilo = "Espalda"
                    elif "BREAST" in estilo_raw: estilo = "Pecho"
                    elif "FLY" in estilo_raw: estilo = "Mariposa"
                    elif "IM" in estilo_raw: estilo = "Combinado"
                    
                    # Parsing Tiempos
                    for col_idx, time_val in enumerate(row):
                        if col_idx == 0: continue
                        if col_idx >= len(age_groups): break
                        
                        age_range = str(age_groups[col_idx]).replace('\n', '').strip()
                        if not age_range or "NO TIME" in str(time_val).upper(): continue
                        
                        t_seg = clean_time(time_val)
                        if t_seg:
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

    return data_to_insert

def ejecutar_cazador():
    print("🦈 Iniciando Cazador de Masters USMS v3.0 (Check de Velocidad)...")
    pdf_url = "https://www-usms-hhgdctfafngha6hr.z01.azurefd.net/-/media/usms/pdfs/pool%20national%20championships/2025%20spring%20nationals/2025%20usms%20spring%20nationals%20nqts%20v2.pdf"
    
    try:
        response = requests.get(pdf_url)
        if response.status_code == 200:
            datos = procesar_tablas_inteligente(response.content, "SCY")
            
            # Filtro de seguridad: Si quedó alguna 'X', avisamos
            sin_genero = [d for d in datos if d['genero'] == 'X']
            if sin_genero:
                print(f"⚠️ ALERTA: {len(sin_genero)} registros no pudieron clasificarse (Género X).")
            
            print(f"✅ Se extrajeron {len(datos)} registros clasificados.")
            
            if datos:
                # Borrón y cuenta nueva de los masters para evitar duplicados
                supabase.table("standards_usa").delete().eq("standard_type", "MASTERS").execute()
                print("🗑️  Limpieza de datos Masters anteriores completada.")

                # Inserción
                batch_size = 100
                total = len(datos)
                for i in range(0, total, batch_size):
                    batch = datos[i:i+batch_size]
                    supabase.table("standards_usa").insert(batch).execute()
                    print(f"   💉 Inyectado lote {i} a {min(i+batch_size, total)}")
    except Exception as e:
        print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    ejecutar_cazador()
