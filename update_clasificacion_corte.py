import os
import pandas as pd
import re
import datetime
from dotenv import load_dotenv
from supabase import create_client

# 1. CONFIGURACIÓN
load_dotenv()
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Mapeo de Eventos (URL -> Nombre en DB)
TARGETS = [
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2024_World_Aquatics_Championships_–_Qualification", 
        "name": "Mundial Doha 2024",
        "pool": "LCM"
    },
    {
        "url": "https://en.wikipedia.org/wiki/Swimming_at_the_2024_Summer_Olympics_–_Qualification",
        "name": "JJOO Paris 2024",
        "pool": "LCM"
    }
    # Puedes agregar Singapur 2025 cuando la página de Wiki esté completa con la tabla
]

def clean_wiki_time(time_str):
    """Limpia tiempos de Wiki (ej: '22.12' o '1:46.70') -> segundos."""
    if pd.isna(time_str) or str(time_str).strip() == "": return None
    # Eliminar referencias de notas tipo [a], [1]
    clean = re.sub(r'\[.*?\]', '', str(time_str)).strip()
    
    try:
        if ':' in clean:
            parts = clean.split(':')
            return float(parts[0]) * 60 + float(parts[1])
        return float(clean)
    except:
        return None

def normalize_event_wiki(event_name):
    """Normaliza 'Men's 50 metre freestyle' -> '50 Libre'"""
    e = event_name.upper()
    
    # Género
    gender = 'X'
    if "MEN" in e and "WOMEN" not in e: gender = 'M'
    if "WOMEN" in e: gender = 'F'
    
    # Distancia y Estilo
    dist = re.search(r'(\d+)', e)
    distance = dist.group(1) if dist else ""
    
    style = "LIBRE"
    if "BACK" in e: style = "ESPALDA"
    if "BREAST" in e: style = "PECHO"
    if "BUTTER" in e or "FLY" in e: style = "MARIPOSA"
    if "MEDLEY" in e or "INDIVIDUAL" in e: style = "IM"
    
    # Prueba final
    prueba_clean = f"{distance} {style}"
    
    return gender, prueba_clean

def scrape_wiki_standards(target):
    print(f"🌍 Scrapeando Wiki: {target['name']}...")
    
    try:
        # Pandas lee todas las tablas de la URL
        tables = pd.read_html(target['url'])
    except Exception as e:
        print(f"❌ Error leyendo HTML: {e}")
        return

    records = []
    
    for df in tables:
        # Heurística: Buscar tablas que tengan columnas de tiempos ("Time", "Standard", "OQT")
        # Y filas con nombres de pruebas ("Freestyle", "Backstroke")
        
        # Convertir todo a string para buscar keywords
        df_str = df.to_string().upper()
        
        if "FREE" in df_str and ("OQT" in df_str or "STANDARD" in df_str or "TIME" in df_str):
            print("   -> Tabla de tiempos detectada.")
            
            # Iterar filas
            # La estructura de Wiki suele ser: Event | Men OQT | Men OCT | Women OQT | Women OCT
            # O a veces: Event | Time (Men) | Time (Women)
            
            # Detectamos columnas
            cols = [str(c).upper() for c in df.columns]
            
            # Caso 1: Columnas separadas por Género y Tipo (La más común en JJOO/Mundiales)
            # Buscamos índices
            idx_men_a = -1
            idx_women_a = -1
            
            for i, col in enumerate(cols):
                if "MEN" in col and ("OQT" in col or "A STANDARD" in col or "TIME" in col): idx_men_a = i
                if "WOMEN" in col and ("OQT" in col or "A STANDARD" in col or "TIME" in col): idx_women_a = i
            
            if idx_men_a == -1 and idx_women_a == -1: continue

            for index, row in df.iterrows():
                raw_event = str(row[0]).upper() # Asumimos col 0 es el evento
                
                # Ignorar filas basura
                if "EVENT" in raw_event or "METRE" not in raw_event and "FREESTYLE" not in raw_event: 
                    # A veces wiki pone solo "50 m freestyle", validamos
                    if not re.search(r'\d+', raw_event): continue

                # Normalizar nombre base (sin genero, el genero lo da la columna)
                _, prueba = normalize_event_wiki(raw_event)
                
                # Extraer Hombres
                if idx_men_a != -1:
                    t_val = row[idx_men_a]
                    sec = clean_wiki_time(t_val)
                    if sec:
                        records.append({
                            "nombre_evento": target['name'],
                            "tipo_corte": "OQT / Marca A", # O "Marca A"
                            "categoria": "OPEN",
                            "genero": "M",
                            "prueba": prueba,
                            "piscina": target['pool'],
                            "tiempo_s": sec,
                            "tiempo_display": str(t_val),
                            "temporada": datetime.datetime.now().year
                        })

                # Extraer Mujeres
                if idx_women_a != -1:
                    t_val = row[idx_women_a]
                    sec = clean_wiki_time(t_val)
                    if sec:
                        records.append({
                            "nombre_evento": target['name'],
                            "tipo_corte": "OQT / Marca A",
                            "categoria": "OPEN",
                            "genero": "F",
                            "prueba": prueba,
                            "piscina": target['pool'],
                            "tiempo_s": sec,
                            "tiempo_display": str(t_val),
                            "temporada": datetime.datetime.now().year
                        })

    # Inserción (Upsert lógico)
    if records:
        print(f"🚀 Insertando {len(records)} marcas internacionales...")
        # Borrar viejos para este evento
        supabase.table("clasificacion_corte").delete().eq("nombre_evento", target['name']).execute()
        # Insertar nuevos
        supabase.table("clasificacion_corte").insert(records).execute()
        print("✅ Hecho.")
    else:
        print("⚠️ No se extrajeron datos. Revisa la estructura de la tabla Wiki.")

if __name__ == "__main__":
    for t in TARGETS:
        scrape_wiki_standards(t)
