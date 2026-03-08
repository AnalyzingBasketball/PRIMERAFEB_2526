# Módulo 0: CONFIGURACIÓN INICIAL, AUTO-INSTALADOR Y LIBRERÍAS
# ==============================================================================
import sys
import subprocess
import importlib

REQUIRED_PACKAGES = {
    'pandas': 'pandas',
    'numpy': 'numpy',
    'requests': 'requests',
    'gspread': 'gspread',
    'gspread_dataframe': 'gspread-dataframe',
    'beautifulsoup4': 'bs4'
}

print("⏳ Comprobando el entorno y las librerías necesarias...")

for module_name, pip_name in REQUIRED_PACKAGES.items():
    try:
        importlib.import_module(module_name)
    except ImportError:
        print(f"   📦 Instalando '{pip_name}' por primera vez...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pip_name, "--quiet"])
        print(f"   ✅ '{pip_name}' instalado correctamente.")

import pandas as pd
import requests
import json
import os
import time
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from bs4 import BeautifulSoup
import re
import unicodedata

try:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 1000)

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Connection': 'keep-alive'
    }
    
    BASE_URL_API = "https://intrafeb.feb.es/LiveStats.API/api/v1"
    print("\n✅ Módulo 0 ejecutado con éxito: Ecosistema preparado, librerías cargadas y Headers establecidos.")

except Exception as e:
    print(f"\n❌ Error crítico en la ejecución del Módulo 0. Detalles: {e}")

# Módulo 1: EXTRACCIÓN MAESTRA (CALENDARIO Y ROSTER ACUMULATIVO)
# ==============================================================================
BASE_DIR = "."
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_API_DIR = os.path.join(DATA_DIR, "raw_api")
ARCHIVO_CALENDARIO = os.path.join(DATA_DIR, "CALENDAR_PRIMERAFEB_2526.csv")
ARCHIVO_ROSTER = os.path.join(DATA_DIR, "ROSTER_PRIMERAFEB_2526.csv")
ARCHIVO_JSON_DICT = os.path.join(RAW_API_DIR, "PLAYER_NAMES_DICT.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_API_DIR, exist_ok=True)

BASE_URL = "https://www.feb.es"
HEADERS_WEB = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'}

def clean_string(texto):
    if pd.isna(texto): return ""
    return "".join(c for c in unicodedata.normalize('NFKD', str(texto)) if not unicodedata.combining(c)).strip()

def clean_position_english(pos_text):
    if not pos_text: return ""
    t = clean_string(pos_text).lower().replace("-", " ").replace(".", "").strip()
    if 'base' in t: return 'PG'
    if 'escolta' in t: return 'SG'
    if 'ala' in t and 'piv' in t: return 'PF'
    if 'a piv' in t: return 'PF'
    if 'alero' in t: return 'SF'
    if 'piv' in t: return 'C'
    return ""

def formatear_nombre_basico(nombre):
    if not isinstance(nombre, str): return nombre
    nombre = nombre.strip()
    if ',' in nombre:
        p = nombre.split(',', 1)
        if len(p) == 2 and len(p[1].strip()) > 0:
            return f"{p[1].strip()[0].upper()}. {p[0].title().strip()}"
    return nombre.title()

def extraer_calendario():
    print("⏳ Conectando a FEB para extraer calendario...")
    url = f"{BASE_URL}/competiciones/calendario/primerafeb/1/2025"
    r = requests.get(url, headers=HEADERS_WEB)
    soup = BeautifulSoup(r.text, 'html.parser')
    datos = []
    
    for col in soup.find_all('div', class_='columna'):
        h1 = col.find('h1', class_='titulo-modulo')
        if not h1: continue
        match_cab = re.search(r'(Jornada\s+\d+)\s+(.*)', h1.get_text(strip=True), re.IGNORECASE)
        jornada_num = re.search(r'\d+', match_cab.group(1)).group() if match_cab else "0"
        
        tabla = col.find('table')
        if not tabla: continue
            
        for fila in tabla.find_all('tr'):
            if fila.find('th') or 'LOCAL' in fila.get_text(strip=True).upper(): continue
            a_eq = fila.find_all('a', href=re.compile(r'Equipo\.aspx', re.IGNORECASE))
            a_p = fila.find('a', href=re.compile(r'Partido\.aspx\?p=', re.IGNORECASE))
            
            if a_p and len(a_eq) >= 2:
                resultado = a_p.get_text(strip=True)
                datos.append({
                    "MATCHID": re.search(r'p=(\d+)', a_p['href']).group(1),
                    "ROUND": int(jornada_num),
                    "HOME_TEAM_ID": re.search(r'i=(\d+)', a_eq[0]['href']).group(1),
                    "HOME_TEAM": clean_string(a_eq[0].get_text(strip=True)).upper(),
                    "AWAY_TEAM_ID": re.search(r'i=(\d+)', a_eq[-1]['href']).group(1),
                    "AWAY_TEAM": clean_string(a_eq[-1].get_text(strip=True)).upper(),
                    "SCORE_STR": resultado,
                    "STATUS": "PLAYED" if "-" in resultado and any(c.isdigit() for c in resultado) else "PENDING"
                })
                
    df = pd.DataFrame(datos).drop_duplicates(subset=['MATCHID']).sort_values(by=['ROUND', 'MATCHID'])
    df.to_csv(ARCHIVO_CALENDARIO, index=False, encoding='utf-8-sig')
    return df

def actualizar_master_roster():
    print("⏳ Conectando a FEB para extraer plantillas e inyectar PLAYER_ID reales...")
    
    diccionario_ids = {}
    if os.path.exists(ARCHIVO_JSON_DICT):
        try:
            with open(ARCHIVO_JSON_DICT, 'r', encoding='utf-8') as f:
                diccionario_ids = json.load(f)
        except Exception as e:
            print(f"⚠️ Error al leer {ARCHIVO_JSON_DICT}: {e}")

    df_historico = pd.DataFrame()
    if os.path.exists(ARCHIVO_ROSTER):
        df_historico = pd.read_csv(ARCHIVO_ROSTER, dtype=str)

    r = requests.get(f"{BASE_URL}/primerafeb/equipos.aspx", headers=HEADERS_WEB)
    soup = BeautifulSoup(r.text, 'html.parser')
    equipos = []
    for n in soup.find_all('div', class_='equipo'):
        a = n.find('a', href=True)
        if a:
            url_eq = a['href'] if a['href'].startswith('http') else BASE_URL + a['href']
            match_id_equipo = re.search(r'i=(\d+)', url_eq)
            team_id = match_id_equipo.group(1) if match_id_equipo else ""
            equipos.append((clean_string(a.get_text(strip=True)).upper(), team_id, url_eq))

    lista_jugadores = []
    lista_nuevos = []
    
    for nombre_equipo, team_id, url in equipos:
        time.sleep(0.3)
        try:
            rt = requests.get(url, headers=HEADERS_WEB)
            st = BeautifulSoup(rt.text, 'html.parser')
            
            for tbl in st.find_all('table'):
                for row in tbl.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        
                        player_id = ""
                        for a_tag in row.find_all('a', href=True):
                            match_pid = re.search(r'c=(\d+)', a_tag['href'].lower())
                            if match_pid:
                                player_id = match_pid.group(1)
                                break
                                
                        if not player_id: continue

                        texts = []
                        for c in cols:
                            for s in c.get_text(separator="|", strip=True).split('|'):
                                t = s.strip()
                                if not t: continue
                                texts.append(t)
                            
                        n_raw, p_raw, nac_raw, alt = "", "", "-", None
                        for t in texts:
                            tl = clean_string(t).lower()
                            
                            if re.search(r'\b(base|escolta|alero|p[ií]vot|pivot)\b', tl): 
                                p_raw = t
                                continue
                                
                            nums = re.findall(r'\b(1\d{2}|2\d{2})\b', t)
                            if (nums and 'cm' in tl) or (nums and len(t)<=4): 
                                alt = int(nums[0])
                                continue
                                
                            if re.match(r'\d{2}/\d{2}/\d{4}', t) or (t.isdigit() and len(t)<=2) or "nombre" in tl: 
                                continue
                                
                            if not n_raw and len(t)>=3: n_raw = t
                            elif n_raw and len(t)>=3: nac_raw = t
                                
                        if not n_raw: continue
                        
                        pos_limpia = clean_position_english(p_raw)
                        if not pos_limpia:
                            if alt:
                                if alt <= 190: pos_limpia = "PG"
                                elif alt <= 196: pos_limpia = "SG"
                                elif alt <= 200: pos_limpia = "SF"
                                elif alt <= 204: pos_limpia = "PF"
                                else: pos_limpia = "C"
                            else: pos_limpia = "SF" 
                        
                        nac_limpia = nac_raw.title() if nac_raw != "-" else "-"
                        player_raw_title = n_raw.title().strip()
                        
                        if str(player_id) not in diccionario_ids:
                            lista_nuevos.append((player_id, player_raw_title))
                            
                        lista_jugadores.append({
                            'TEAM_ID': str(team_id),
                            'TEAM': nombre_equipo, 
                            'PLAYER_ID': str(player_id),
                            'PLAYER': player_raw_title,
                            'PLAYER_NAME': "",
                            'POSITION': pos_limpia, 
                            'HEIGHT_CM': alt, 
                            'NATIONALITY': nac_limpia
                        })
        except Exception as e:
            pass

    df_nuevos = pd.DataFrame(lista_jugadores)
    
    if not df_historico.empty:
        df_final = pd.concat([df_historico, df_nuevos], ignore_index=True)
        df_final = df_final.drop_duplicates(subset=['PLAYER_ID', 'TEAM_ID'], keep='last')
    else:
        df_final = df_nuevos

    df_final['PLAYER_NAME'] = df_final.apply(
        lambda row: diccionario_ids.get(str(row['PLAYER_ID']), formatear_nombre_basico(row['PLAYER'])), axis=1
    )
    
    cols_ordenadas = ['TEAM_ID', 'TEAM', 'PLAYER_ID', 'PLAYER', 'PLAYER_NAME', 'POSITION', 'HEIGHT_CM', 'NATIONALITY']
    df_final = df_final[cols_ordenadas]
    
    df_final.to_csv(ARCHIVO_ROSTER, index=False, encoding='utf-8-sig')
    return df_final, lista_nuevos

try:
    df_cal = extraer_calendario()
    print(f"✅ Calendario OK: {len(df_cal)} partidos procesados. Guardado como CALENDAR_PRIMERAFEB_2526.csv")
    
    df_roster, ids_nuevos = actualizar_master_roster()
    print(f"✅ Master Roster OK: {len(df_roster)} jugadores guardados (Históricos conservados).")
    
    if len(ids_nuevos) > 0:
        print(f"\n⚠️ ATENCIÓN: Se detectaron {len(ids_nuevos)} jugadores cuyo ID no está en PLAYER_NAMES_DICT.json:")
        for pid, nombre in set(ids_nuevos): 
            print(f"   -> ID: {pid} | Nombre: {nombre}")
    else:
        print(f"\n✅ Auditoría perfecta: Todos los nombres provienen de tu Diccionario JSON.")
        
    print("\n✅ MÓDULO 1 EJECUTADO CORRECTAMENTE.")

except Exception as e:
    import traceback
    print(f"\n❌ Error crítico en el Módulo 1. Detalles:\n{traceback.format_exc()}")


# Módulo 2: DESCARGA RAW (API LIVESTATS)
# ==============================================================================
def descargar_datos_partidos():
    if not os.path.exists(ARCHIVO_CALENDARIO):
        raise FileNotFoundError(f"❌ Error: No se encuentra el calendario en {ARCHIVO_CALENDARIO}. Ejecuta el Módulo 1 primero.")
        
    df_cal = pd.read_csv(ARCHIVO_CALENDARIO, dtype=str)
    partidos_jugados = df_cal[df_cal['STATUS'] == 'PLAYED']['MATCHID'].tolist()
    
    descargados_hoy = 0
    errores = 0

    print(f"⏳ Analizando {len(partidos_jugados)} partidos jugados en el calendario...")

    session = requests.Session()
    session.headers.update(HEADERS_WEB)

    for match_id in partidos_jugados:
        match_id = str(match_id)
        box_path = os.path.join(RAW_API_DIR, f"raw_boxscore_{match_id}.json")
        pbp_path = os.path.join(RAW_API_DIR, f"raw_pbp_{match_id}.json")
        team_path = os.path.join(RAW_API_DIR, f"raw_teamstats_{match_id}.json")

        if os.path.exists(box_path) and os.path.exists(pbp_path) and os.path.exists(team_path):
            continue

        print(f" 📡 Descargando datos del partido {match_id}...")
        
        try:
            url_web = f"{BASE_URL}/competiciones/partido/{match_id}"
            res_web = session.get(url_web)
            soup = BeautifulSoup(res_web.text, 'html.parser')
            
            token_input = soup.find('input', id='_ctl0_token')
            if not token_input:
                print(f"   ❌ No se encontró el token de seguridad para {match_id}. Saltando...")
                errores += 1
                continue
                
            token = token_input['value'].strip()
            
            api_headers = HEADERS_WEB.copy()
            api_headers.update({
                "Authorization": f"Bearer {token}",
                "Origin": BASE_URL,
                "Referer": f"{BASE_URL}/",
                "Accept": "application/json"
            })
            
        except Exception as e:
            print(f"   ❌ Error obteniendo token del partido {match_id}: {e}")
            errores += 1
            continue

        try:
            if not os.path.exists(box_path):
                r_box = requests.get(f"{BASE_URL_API}/BoxScore/{match_id}", headers=api_headers)
                if r_box.status_code == 200:
                    with open(box_path, 'w', encoding='utf-8') as f:
                        json.dump(r_box.json(), f, ensure_ascii=False, indent=4)
            
            if not os.path.exists(pbp_path):
                r_pbp = requests.get(f"{BASE_URL_API}/KeyFacts/{match_id}", headers=api_headers)
                if r_pbp.status_code == 200:
                    with open(pbp_path, 'w', encoding='utf-8') as f:
                        json.dump(r_pbp.json(), f, ensure_ascii=False, indent=4)
                        
            if not os.path.exists(team_path):
                r_team = requests.get(f"{BASE_URL_API}/TeamStats/{match_id}", headers=api_headers)
                if r_team.status_code == 200:
                    with open(team_path, 'w', encoding='utf-8') as f:
                        json.dump(r_team.json(), f, ensure_ascii=False, indent=4)
                    
            descargados_hoy += 1
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"   ❌ Error descargando JSONs del partido {match_id}: {e}")
            errores += 1

    return descargados_hoy, errores

try:
    descargas, errores = descargar_datos_partidos()
    if descargas > 0:
        print(f"✅ Descarga Raw OK: {descargas} nuevos partidos extraídos exitosamente de la API.")
    else:
        print(f"✅ Descarga Raw OK: Todos los partidos jugados ya estaban descargados en local.")
        
    if errores > 0:
        print(f"⚠️ Hubo {errores} errores puntuales durante la descarga.")
        
    print("\n✅ MÓDULO 2 EJECUTADO CORRECTAMENTE. JSONs listos en la carpeta raw_api.")

except Exception as e:
    import traceback
    print(f"\n❌ Error crítico en el Módulo 2. Detalles:\n{traceback.format_exc()}")


# Módulo 3: MOTOR MATEMÁTICO ETL (DEFINITIVO + AUDITORÍA TOTAL)
# ==============================================================================
OUT_BOXSCORE = os.path.join(DATA_DIR, "BOXSCORE_PRIMERAFEB_2526.csv")
OUT_TEAMSTATS = os.path.join(DATA_DIR, "TEAMSTATS_PRIMERAFEB_2526.csv")
OUT_PBP = os.path.join(DATA_DIR, "PBP_PRIMERAFEB_2526.csv")
OUT_LINEUPS = os.path.join(DATA_DIR, "LINEUPS_PRIMERAFEB_2526.csv")

def to_float(val):
    try:
        if val is None or str(val).strip() == "": return 0.0
        return round(float(str(val).replace(',', '.')), 1)
    except: return 0.0

def safe_div(n, d, default=0.0):
    try:
        den = float(d)
        if den == 0.0 or pd.isna(den): return default
        return round(float(n) / den, 1)
    except: return default

def parse_minutos(time_str):
    if pd.isna(time_str) or not isinstance(time_str, str): return 0.0
    try:
        if ':' in time_str:
            m, s = time_str.split(':')
            return round(float(m) + (float(s) / 60.0), 1)
        return round(float(str(time_str).replace(',', '.')), 1)
    except: return 0.0

def get_5_players_flat(player_ids_set, match_roster_dict):
    pos_order = {'PG': 1, 'SG': 2, 'SF': 3, 'PF': 4, 'C': 5}
    players_data = []
    for pid in player_ids_set:
        if pd.isna(pid) or str(pid).strip() == "": continue
        pid_str = str(pid).strip()
        info = match_roster_dict.get(pid_str, {})
        name = info.get('PLAYER_NAME', 'Unknown')
        pos = info.get('POSITION', 'SF')
        rank = pos_order.get(pos, 6)
        players_data.append((pid_str, name, pos, rank))
        
    players_data.sort(key=lambda x: x[3])
    while len(players_data) < 5: players_data.append(("", "", "", 7))
    players_data = players_data[:5]
    
    flat_list = []
    for p in players_data:
        flat_list.extend([p[0], p[1], p[2]])
    return flat_list

def translate_pbp_action(raw_action, text):
    a = str(raw_action).lower().strip()
    t = str(text).lower().strip()
    
    if 'subst' in a or 'substitution' in a:
        if 'entra' in t or 'in' in t: return 'Sub In'
        if 'sale' in t or 'out' in t: return 'Sub Out'
        
    if 'tiro de 2' in t or '2pt' in a:
        if 'anotado' in t or 'made' in t or 'm' in a: return '2PT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return '2PT Missed'
    if 'tiro de 3' in t or '3pt' in a:
        if 'anotado' in t or 'made' in t or 'm' in a: return '3PT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return '3PT Missed'
    if 'tiro de 1' in t or 'tiro libre' in t or '1pt' in a or 'fthrow' in a:
        if 'anotado' in t or 'made' in t or 'm' in a: return 'FT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return 'FT Missed'
        
    if 'turnover' in a or 'pérdida' in t or 'to' == a: return 'Turnover'
    if 'steal' in a or 'st' == a or 'robo' in t: return 'Steal'
    if 'assist' in a or 'asistencia' in t: return 'Assist'
    if 'block' in a or 'bs' == a or 'tc' == a or 'tapón' in t: return 'Block'
    if 'foul' in a or 'falta' in t or 'pf' == a: return 'Foul'
    if 'rebound' in a or 'rebote' in t or 'ro' == a or 'rd' == a:
        return 'Def. Reb' 
        
    return raw_action.title()

def procesar_estadisticas_acumuladas():
    print("⏳ Iniciando Motor Matemático ETL (Definitivo)...")
    
    if not os.path.exists(ARCHIVO_ROSTER): raise FileNotFoundError(f"Falta el Roster: {ARCHIVO_ROSTER}")
    df_roster = pd.read_csv(ARCHIVO_ROSTER, dtype=str)
    
    dict_roster_id = {}
    for _, row in df_roster.iterrows():
        if pd.notna(row['PLAYER_ID']) and str(row['PLAYER_ID']).strip() != "":
            dict_roster_id[str(row['PLAYER_ID']).strip()] = {
                'PLAYER': str(row['PLAYER']),
                'PLAYER_NAME': str(row['PLAYER_NAME']),
                'POSITION': str(row['POSITION'])
            }

    if not os.path.exists(ARCHIVO_CALENDARIO): raise FileNotFoundError(f"Falta el Calendario: {ARCHIVO_CALENDARIO}")
    df_cal = pd.read_csv(ARCHIVO_CALENDARIO, dtype=str)
    dict_calendar = df_cal.set_index('MATCHID')['ROUND'].to_dict()

    procesados_previos = set()
    if os.path.exists(OUT_BOXSCORE):
        try:
            df_prev = pd.read_csv(OUT_BOXSCORE, usecols=['MATCHID'], dtype=str)
            procesados_previos = set(df_prev['MATCHID'].unique())
        except Exception: pass

    archivos_json = [f for f in os.listdir(RAW_API_DIR) if f.startswith('raw_boxscore_') and f.endswith('.json')]
    partidos_totales = set([f.split('_')[2].split('.')[0] for f in archivos_json])
    partidos_a_procesar = partidos_totales - procesados_previos

    print(f"📊 Partidos en raw_api: {len(partidos_totales)} | Ya procesados: {len(procesados_previos)} | Nuevos a procesar: {len(partidos_a_procesar)}")

    all_boxscores = []
    all_teamstats = []
    all_pbp = []
    all_lineups = []
    
    errores = 0
    procesados_ahora = 0

    for match_id in partidos_a_procesar:
        try:
            box_path = os.path.join(RAW_API_DIR, f"raw_boxscore_{match_id}.json")
            pbp_path = os.path.join(RAW_API_DIR, f"raw_pbp_{match_id}.json")
            
            if not os.path.exists(box_path): continue
            with open(box_path, 'r', encoding='utf-8') as f: data_box = json.load(f)
            
            match_round = dict_calendar.get(str(match_id), "0")
            teams = data_box.get('BOXSCORE', {}).get('TEAM', [])
            if len(teams) != 2: continue

            dict_team_locs = {}
            dict_team_ids = {}
            pbp_name_resolver = {}
            local_match_roster = dict_roster_id.copy()
            
            for i, t in enumerate(teams):
                t_name = str(t.get('name', f'Team_{i}')).upper().strip()
                t_id = str(t.get('id', ''))
                loc = 'HOME' if str(t.get('isHome', '')).strip().lower() in ['1', 'true', 'yes'] else 'AWAY'
                dict_team_locs[t_name] = loc
                dict_team_ids[t_name] = t_id
                
                for p in t.get('PLAYER', []):
                    pid = str(p.get('id', '')).strip()
                    api_name = str(p.get('name', '')).strip()
                    
                    if pid and pid not in local_match_roster:
                        local_match_roster[pid] = {'PLAYER': api_name.title(), 'PLAYER_NAME': api_name.title(), 'POSITION': 'SF'}
                    if pid:
                        pbp_name_resolver[api_name.upper()] = pid

            if list(dict_team_locs.values()).count('HOME') != 1:
                t1, t2 = list(dict_team_locs.keys())[0], list(dict_team_locs.keys())[1]
                dict_team_locs[t1] = 'HOME'
                dict_team_locs[t2] = 'AWAY'

            match_players = []
            for t in teams:
                t_name = str(t.get('name', '')).upper().strip()
                t_id = dict_team_ids.get(t_name, '')
                loc = dict_team_locs.get(t_name, 'AWAY')
                
                for p in t.get('PLAYER', []):
                    pid = str(p.get('id', '')).strip()
                    if not pid: continue
                    
                    player_bruto = local_match_roster[pid]['PLAYER']
                    player_limpio = local_match_roster[pid]['PLAYER_NAME']
                        
                    min_dec = parse_minutos(p.get('minFormatted', '00:00'))
                    min_secs = to_float(p.get('min', 0))
                    pts_num = to_float(p.get('pts', 0))
                    is_starter = 1 if str(p.get('inn', '0')).strip() in ['1', 'true', '*'] else 0
                    
                    if min_dec == 0 and pts_num == 0 and is_starter == 0: continue
                    
                    match_players.append({
                        'MATCHID': match_id, 'ROUND': match_round,
                        'TEAM_ID': t_id, 'TEAM': t_name, 'LOCATION': loc,
                        'PLAYER_ID': pid, 'PLAYER': player_bruto, 'PLAYER_NAME': player_limpio,
                        'IS_STARTER': is_starter, 'MIN': min_dec, 'MIN_SECS': min_secs,
                        'PTS': pts_num,
                        'FGM_2': to_float(p.get('p2m', 0)), 'FGA_2': to_float(p.get('p2a', 0)),
                        'FGM_3': to_float(p.get('p3m', 0)), 'FGA_3': to_float(p.get('p3a', 0)),
                        'FGM': to_float(p.get('fgm', 0)), 'FGA': to_float(p.get('fga', 0)),
                        'FTM': to_float(p.get('p1m', 0)), 'FTA': to_float(p.get('p1a', 0)),
                        'ORB': to_float(p.get('ro', 0)), 'DRB': to_float(p.get('rd', 0)), 'TRB': to_float(p.get('rt', 0)),
                        'AST': to_float(p.get('assist', 0)), 'TOV': to_float(p.get('to', 0)),
                        'STL': to_float(p.get('st', 0)), 'BLK': to_float(p.get('bs', 0)), 'BLKA': to_float(p.get('tc', 0)),
                        'PF': to_float(p.get('pf', 0)), 'PFD': to_float(p.get('rf', 0)),
                        'PIR': to_float(p.get('val', 0)), 'PLUS_MINUS': to_float(p.get('pllss', 0))
                    })
            
            df_match = pd.DataFrame(match_players)
            if df_match.empty: continue

            t_stats = df_match.groupby('TEAM').sum(numeric_only=True).reset_index()
            if len(t_stats) != 2: continue
            
            t1, t2 = t_stats['TEAM'].iloc[0], t_stats['TEAM'].iloc[1]
            dict_team_totals = t_stats.set_index('TEAM').to_dict('index')

            stats_avanzadas = []
            for _, row in df_match.iterrows():
                mi_t = dict_team_totals.get(row['TEAM'])
                riv_t = dict_team_totals.get(t2 if row['TEAM'] == t1 else t1)
                
                min_eq = safe_div(mi_t['MIN'], 5.0)
                pts, fgm, fga, fta, tov = row['PTS'], row['FGM'], row['FGA'], row['FTA'], row['TOV']
                opp_poss = riv_t['FGA'] - riv_t['ORB'] + riv_t['TOV'] + (0.44 * riv_t['FTA'])
                
                stats_avanzadas.append({
                    'TS%': safe_div(pts * 100, 2 * (fga + 0.44 * fta)), 'eFG%': safe_div((fgm + 0.5 * row['FGM_3']) * 100, fga),
                    '3PAr': safe_div(row['FGA_3'] * 100, fga), 'FTr': safe_div(fta * 100, fga),
                    'PPS_2': safe_div(row['FGM_2'] * 2, row['FGA_2']), 'PPS_3': safe_div(row['FGM_3'] * 3, row['FGA_3']),
                    'FTA_PER_PFD': safe_div(fta, row['PFD']),
                    'ORB%': safe_div(row['ORB'] * min_eq * 100, row['MIN'] * (mi_t['ORB'] + riv_t['DRB'])),
                    'DRB%': safe_div(row['DRB'] * min_eq * 100, row['MIN'] * (mi_t['DRB'] + riv_t['ORB'])),
                    'TRB%': safe_div(row['TRB'] * min_eq * 100, row['MIN'] * (mi_t['TRB'] + riv_t['TRB'])),
                    'AST%': safe_div(row['AST'] * 100, (safe_div(row['MIN'], min_eq) * mi_t['FGM']) - fgm),
                    'STL%': safe_div(row['STL'] * min_eq * 100, row['MIN'] * opp_poss),
                    'BLK%': safe_div(row['BLK'] * min_eq * 100, row['MIN'] * riv_t['FGA_2']),
                    'TOV%': safe_div(tov * 100, fga + 0.44 * fta + tov),
                    'USG%': safe_div((fga + 0.44*fta + tov) * min_eq * 100, row['MIN'] * (mi_t['FGA'] + 0.44*mi_t['FTA'] + mi_t['TOV']))
                })
                
            df_match_final = pd.concat([df_match.reset_index(drop=True), pd.DataFrame(stats_avanzadas)], axis=1)
            all_boxscores.append(df_match_final)

            team_adv = []
            for tm in [t1, t2]:
                mi_t = dict_team_totals[tm]
                riv_t = dict_team_totals[t2 if tm == t1 else t1]
                t_id = dict_team_ids.get(tm, '')
                
                poss = mi_t['FGA'] - mi_t['ORB'] + mi_t['TOV'] + (0.44 * mi_t['FTA'])
                poss_riv = riv_t['FGA'] - riv_t['ORB'] + riv_t['TOV'] + (0.44 * riv_t['FTA'])
                
                team_adv.append({
                    'MATCHID': match_id, 'ROUND': match_round, 'TEAM_ID': t_id, 'TEAM': tm, 'LOCATION': dict_team_locs.get(tm, 'AWAY'),
                    'POSS': round(poss, 1), 'PACE': safe_div((poss + poss_riv) * 40, 2 * (mi_t['MIN'] / 5.0)),
                    'O_RTG': safe_div(mi_t['PTS'] * 100, poss), 'D_RTG': safe_div(riv_t['PTS'] * 100, poss_riv),
                    'NET_RTG': round(safe_div(mi_t['PTS'] * 100, poss) - safe_div(riv_t['PTS'] * 100, poss_riv), 1),
                    'TS%': safe_div(mi_t['PTS'] * 100, 2 * (mi_t['FGA'] + 0.44 * mi_t['FTA'])),
                    'eFG%': safe_div((mi_t['FGM'] + 0.5 * mi_t['FGM_3']) * 100, mi_t['FGA']),
                    'TOV%': safe_div(mi_t['TOV'] * 100, mi_t['FGA'] + 0.44 * mi_t['FTA'] + mi_t['TOV']),
                    'ORB%': safe_div(mi_t['ORB'] * 100, mi_t['ORB'] + riv_t['DRB']),
                    'DRB%': safe_div(mi_t['DRB'] * 100, mi_t['DRB'] + riv_t['ORB']),
                    'TRB%': safe_div(mi_t['TRB'] * 100, mi_t['TRB'] + riv_t['TRB']),
                    'AST_TOV_RATIO': safe_div(mi_t['AST'], mi_t['TOV']), 'FTr': safe_div(mi_t['FTA'] * 100, mi_t['FGA'])
                })
                
            df_team = pd.merge(pd.DataFrame(team_adv), t_stats, on='TEAM')
            df_team = df_team.drop(columns=['IS_STARTER'], errors='ignore')
            cols_base = ['MATCHID', 'ROUND', 'TEAM_ID', 'TEAM', 'LOCATION']
            cols_resto = [c for c in df_team.columns if c not in cols_base]
            if 'MIN' in df_team.columns: df_team['MIN'] = df_team['MIN'].round(0).astype(int)
            all_teamstats.append(df_team[cols_base + cols_resto])

            if os.path.exists(pbp_path):
                with open(pbp_path, 'r', encoding='utf-8') as f: data_pbp = json.load(f)
                
                home_on_court = set(df_match[(df_match['LOCATION'] == 'HOME') & (df_match['IS_STARTER'] == 1)]['PLAYER_ID'].tolist())
                away_on_court = set(df_match[(df_match['LOCATION'] == 'AWAY') & (df_match['IS_STARTER'] == 1)]['PLAYER_ID'].tolist())

                lines = data_pbp.get('PLAYBYPLAY', {}).get('LINES', [])
                if not lines: continue
                
                df_lines = pd.DataFrame(lines)
                df_lines['quarter'] = pd.to_numeric(df_lines.get('quarter', 1), errors='coerce').fillna(1)
                df_lines['time'] = df_lines.get('time', '00:00').fillna('00:00')
                df_lines['SECONDS_REMAINING'] = pd.to_timedelta('00:' + df_lines['time'].astype(str), errors='coerce').dt.total_seconds().fillna(0)
                df_lines['ACTION_TYPE'] = df_lines.apply(lambda x: translate_pbp_action(x.get('action'), x.get('text')), axis=1)
                
                df_lines['SORT_PRIORITY'] = 3
                df_lines.loc[df_lines['ACTION_TYPE'] == 'Sub Out', 'SORT_PRIORITY'] = 1
                df_lines.loc[df_lines['ACTION_TYPE'] == 'Sub In', 'SORT_PRIORITY'] = 2
                df_lines = df_lines.sort_values(by=['quarter', 'SECONDS_REMAINING', 'SORT_PRIORITY'], ascending=[True, False, True]).reset_index(drop=True)

                pbp_records = []
                prev_true_action = ""
                prev_true_team_id = ""
                
                for _, row in df_lines.iterrows():
                    action = row['ACTION_TYPE']
                    text = str(row.get('text', ''))
                    
                    action_team = ""
                    action_team_id = ""
                    action_team_loc = ""
                    p_id, p_bruto, p_limpio, p_pos = "", "", "", ""
                    
                    match_team = re.search(r'^\((.*?)\)', text)
                    if match_team:
                        team_abbrev = match_team.group(1).upper().strip()
                        for tm in dict_team_locs.keys():
                            if tm.startswith(team_abbrev) or team_abbrev in tm:
                                action_team = tm
                                action_team_id = dict_team_ids[tm]
                                action_team_loc = dict_team_locs[tm]
                                break
                    
                    if action == 'Def. Reb':
                        if prev_true_action in ['2PT Missed', '3PT Missed', 'FT Missed']:
                            if action_team_id == prev_true_team_id and action_team_id != "":
                                action = 'Off. Reb'
                        elif prev_true_action == 'Block':
                            if action_team_id == prev_true_team_id and action_team_id != "":
                                action = 'Def. Reb' 
                            else:
                                action = 'Off. Reb' 

                    raw_id = str(row.get('id', '')).strip()
                    if raw_id and raw_id != 'None':
                        p_id = raw_id
                    else:
                        m = re.search(r'^\(.*?\) (.*?)(?::|\s+(?:Substitution|Sustitución|Entra|Sale|in|out))', text, re.IGNORECASE)
                        if m:
                            api_name_extracted = m.group(1).upper().strip()
                            p_id = pbp_name_resolver.get(api_name_extracted, "")
                            
                    if not p_id and 'Reb' in action:
                        p_id = 'TEAM'
                        p_bruto = 'Team Rebound'
                        p_limpio = 'Team Rebound'
                        p_pos = 'TEAM'

                    if p_id and p_id != 'TEAM':
                        info = local_match_roster.get(p_id, {})
                        p_bruto = info.get('PLAYER', '')
                        p_limpio = info.get('PLAYER_NAME', '')
                        p_pos = info.get('POSITION', '')

                    if action == 'Sub Out' and p_id and p_id != 'TEAM':
                        if action_team_loc == 'HOME': home_on_court.discard(p_id)
                        elif action_team_loc == 'AWAY': away_on_court.discard(p_id)
                    elif action == 'Sub In' and p_id and p_id != 'TEAM':
                        if action_team_loc == 'HOME': home_on_court.add(p_id)
                        elif action_team_loc == 'AWAY': away_on_court.add(p_id)
                        
                    if action not in ['Sub In', 'Sub Out', 'Timeout', 'Period']:
                        prev_true_action = action
                        prev_true_team_id = action_team_id

                    cx, cy = None, None
                    pos_str = str(row.get('position', row.get('Position', '')))
                    if '|' in pos_str:
                        parts = pos_str.split('|')
                        if len(parts) >= 2: cx, cy = parts[0], parts[1]

                    # --- TRANSFORMACIÓN DEFINITIVA PARA MEDIA PISTA VERTICAL (ARO ABAJO) ---
                    shot_x_calc, shot_y_calc = None, None
                    try:
                        if cx is not None and cy is not None:
                            orig_x = float(cx)
                            orig_y = float(cy)
                            
                            # 1. Doblar el largo (X) para concentrar todo en el aro del punto 0
                            fold_x = orig_x if orig_x <= 50 else (100 - orig_x)
                            
                            # 2. La nueva X (horizontal) es el ancho original (Y). El aro queda centrado en 50.
                            shot_x_calc = orig_y
                            
                            # 3. La nueva Y (vertical) es el largo doblado. Lo escalamos al 100%. El aro queda en 0 (abajo).
                            shot_y_calc = fold_x * 2
                    except:
                        pass
                    
                    h_flat = get_5_players_flat(home_on_court, local_match_roster)
                    a_flat = get_5_players_flat(away_on_court, local_match_roster)

                    pbp_records.append({
                        'MATCHID': match_id, 'ROUND': match_round,
                        'PERIOD': row['quarter'], 'TIME': row['time'], 'SECONDS_REMAINING': row['SECONDS_REMAINING'],
                        'TEAM_ID': action_team_id, 'ACTION_TEAM': action_team, 'ACTION_TEAM_LOC': action_team_loc, 
                        'PLAYER_ID': p_id, 'PLAYER': p_bruto, 'PLAYER_NAME': p_limpio, 'PLAYER_POSITION': p_pos,
                        'ACTION_TYPE': action, 'ACTION_TEXT': text, 'COORD_X': cx, 'COORD_Y': cy,
                        'SHOT_X': shot_x_calc,
                        'SHOT_Y': shot_y_calc,
                        'SCORE_H': row.get('scoreA', ''), 'SCORE_A': row.get('scoreB', ''),
                        'H1_PLAYER_ID': h_flat[0], 'H1_PLAYER_NAME': h_flat[1], 'H1_PLAYER_POS': h_flat[2],
                        'H2_PLAYER_ID': h_flat[3], 'H2_PLAYER_NAME': h_flat[4], 'H2_PLAYER_POS': h_flat[5],
                        'H3_PLAYER_ID': h_flat[6], 'H3_PLAYER_NAME': h_flat[7], 'H3_PLAYER_POS': h_flat[8],
                        'H4_PLAYER_ID': h_flat[9], 'H4_PLAYER_NAME': h_flat[10], 'H4_PLAYER_POS': h_flat[11],
                        'H5_PLAYER_ID': h_flat[12], 'H5_PLAYER_NAME': h_flat[13], 'H5_PLAYER_POS': h_flat[14],
                        'A1_PLAYER_ID': a_flat[0], 'A1_PLAYER_NAME': a_flat[1], 'A1_PLAYER_POS': a_flat[2],
                        'A2_PLAYER_ID': a_flat[3], 'A2_PLAYER_NAME': a_flat[4], 'A2_PLAYER_POS': a_flat[5],
                        'A3_PLAYER_ID': a_flat[6], 'A3_PLAYER_NAME': a_flat[7], 'A3_PLAYER_POS': a_flat[8],
                        'A4_PLAYER_ID': a_flat[9], 'A4_PLAYER_NAME': a_flat[10], 'A4_PLAYER_POS': a_flat[11],
                        'A5_PLAYER_ID': a_flat[12], 'A5_PLAYER_NAME': a_flat[13], 'A5_PLAYER_POS': a_flat[14]
                    })
                    
                df_pbp_match = pd.DataFrame(pbp_records)

                df_pbp_match['SCORE_H'] = pd.to_numeric(df_pbp_match['SCORE_H'], errors='coerce').ffill().fillna(0)
                df_pbp_match['SCORE_A'] = pd.to_numeric(df_pbp_match['SCORE_A'], errors='coerce').ffill().fillna(0)

                df_pbp_match['SCORE_H_MAX'] = df_pbp_match['SCORE_H'].cummax()
                df_pbp_match['SCORE_A_MAX'] = df_pbp_match['SCORE_A'].cummax()

                df_pbp_match['PTS_H'] = (df_pbp_match['SCORE_H_MAX'] - df_pbp_match['SCORE_H_MAX'].shift(1).fillna(0)).clip(lower=0)
                df_pbp_match['PTS_A'] = (df_pbp_match['SCORE_A_MAX'] - df_pbp_match['SCORE_A_MAX'].shift(1).fillna(0)).clip(lower=0)

                df_pbp_match['H_HASH'] = df_pbp_match['H1_PLAYER_ID'].astype(str) + df_pbp_match['H2_PLAYER_ID'].astype(str) + df_pbp_match['H3_PLAYER_ID'].astype(str) + df_pbp_match['H4_PLAYER_ID'].astype(str) + df_pbp_match['H5_PLAYER_ID'].astype(str)
                df_pbp_match['A_HASH'] = df_pbp_match['A1_PLAYER_ID'].astype(str) + df_pbp_match['A2_PLAYER_ID'].astype(str) + df_pbp_match['A3_PLAYER_ID'].astype(str) + df_pbp_match['A4_PLAYER_ID'].astype(str) + df_pbp_match['A5_PLAYER_ID'].astype(str)
                
                df_pbp_match['LINEUP_CHANGE'] = (df_pbp_match['H_HASH'] != df_pbp_match['H_HASH'].shift()) | \
                                                (df_pbp_match['A_HASH'] != df_pbp_match['A_HASH'].shift()) | \
                                                (df_pbp_match['PERIOD'] != df_pbp_match['PERIOD'].shift())
                
                df_pbp_match['STINT_ID'] = df_pbp_match['LINEUP_CHANGE'].cumsum()

                df_pbp_match['NEXT_PERIOD'] = df_pbp_match['PERIOD'].shift(-1)
                df_pbp_match['NEXT_TIME'] = df_pbp_match['SECONDS_REMAINING'].shift(-1)
                
                df_pbp_match['DURATION'] = np.where(
                    df_pbp_match['PERIOD'] == df_pbp_match['NEXT_PERIOD'],
                    df_pbp_match['SECONDS_REMAINING'] - df_pbp_match['NEXT_TIME'],
                    df_pbp_match['SECONDS_REMAINING'] 
                )
                df_pbp_match['DURATION'] = df_pbp_match['DURATION'].clip(lower=0)

                cols_pbp_limpio = [
                    'MATCHID', 'ROUND', 'PERIOD', 'TIME', 'SECONDS_REMAINING', 'TEAM_ID', 'ACTION_TEAM', 'ACTION_TEAM_LOC', 
                    'PLAYER_ID', 'PLAYER', 'PLAYER_NAME', 'PLAYER_POSITION', 'ACTION_TYPE', 'ACTION_TEXT', 
                    'COORD_X', 'COORD_Y', 'SHOT_X', 'SHOT_Y', 'SCORE_H', 'SCORE_A', 
                    'H1_PLAYER_ID', 'H1_PLAYER_NAME', 'H1_PLAYER_POS', 'H2_PLAYER_ID', 'H2_PLAYER_NAME', 'H2_PLAYER_POS',
                    'H3_PLAYER_ID', 'H3_PLAYER_NAME', 'H3_PLAYER_POS', 'H4_PLAYER_ID', 'H4_PLAYER_NAME', 'H4_PLAYER_POS',
                    'H5_PLAYER_ID', 'H5_PLAYER_NAME', 'H5_PLAYER_POS',
                    'A1_PLAYER_ID', 'A1_PLAYER_NAME', 'A1_PLAYER_POS', 'A2_PLAYER_ID', 'A2_PLAYER_NAME', 'A2_PLAYER_POS',
                    'A3_PLAYER_ID', 'A3_PLAYER_NAME', 'A3_PLAYER_POS', 'A4_PLAYER_ID', 'A4_PLAYER_NAME', 'A4_PLAYER_POS',
                    'A5_PLAYER_ID', 'A5_PLAYER_NAME', 'A5_PLAYER_POS', 'STINT_ID'
                ]
                all_pbp.append(df_pbp_match[cols_pbp_limpio].copy())

                for loc_val, p_cols_id, p_cols_name, p_cols_pos in [
                    ('HOME', ['H1_PLAYER_ID','H2_PLAYER_ID','H3_PLAYER_ID','H4_PLAYER_ID','H5_PLAYER_ID'], 
                             ['H1_PLAYER_NAME','H2_PLAYER_NAME','H3_PLAYER_NAME','H4_PLAYER_NAME','H5_PLAYER_NAME'], 
                             ['H1_PLAYER_POS','H2_PLAYER_POS','H3_PLAYER_POS','H4_PLAYER_POS','H5_PLAYER_POS']),
                    ('AWAY', ['A1_PLAYER_ID','A2_PLAYER_ID','A3_PLAYER_ID','A4_PLAYER_ID','A5_PLAYER_ID'], 
                             ['A1_PLAYER_NAME','A2_PLAYER_NAME','A3_PLAYER_NAME','A4_PLAYER_NAME','A5_PLAYER_NAME'], 
                             ['A1_PLAYER_POS','A2_PLAYER_POS','A3_PLAYER_POS','A4_PLAYER_POS','A5_PLAYER_POS'])]:
                    
                    tm = next((t_n for t_n, l in dict_team_locs.items() if l == loc_val), None)
                    if not tm: continue
                    t_id = dict_team_ids.get(tm, "")

                    stints = df_pbp_match.groupby('STINT_ID').agg({
                        p_cols_id[0]: 'first', p_cols_id[1]: 'first', p_cols_id[2]: 'first', p_cols_id[3]: 'first', p_cols_id[4]: 'first',
                        p_cols_name[0]: 'first', p_cols_name[1]: 'first', p_cols_name[2]: 'first', p_cols_name[3]: 'first', p_cols_name[4]: 'first',
                        p_cols_pos[0]: 'first', p_cols_pos[1]: 'first', p_cols_pos[2]: 'first', p_cols_pos[3]: 'first', p_cols_pos[4]: 'first',
                        'DURATION': 'sum', 'PTS_H': 'sum', 'PTS_A': 'sum'
                    })

                    for _, s_row in stints.iterrows():
                        pid1, pid2, pid3, pid4, pid5 = s_row[p_cols_id[0]], s_row[p_cols_id[1]], s_row[p_cols_id[2]], s_row[p_cols_id[3]], s_row[p_cols_id[4]]
                        if "" in [pid1, pid2, pid3, pid4, pid5]: continue 
                        
                        duration = s_row['DURATION']
                        
                        if duration == 0 and s_row['PTS_H'] == 0 and s_row['PTS_A'] == 0: continue

                        pts_for = s_row['PTS_H'] if loc_val == 'HOME' else s_row['PTS_A']
                        pts_agt = s_row['PTS_A'] if loc_val == 'HOME' else s_row['PTS_H']
                        
                        all_lineups.append({
                            'MATCHID': match_id, 'ROUND': match_round, 'TEAM_ID': t_id, 'TEAM': tm, 'LOCATION': loc_val, 
                            'P1_ID': pid1, 'P1_NAME': s_row[p_cols_name[0]], 'P1_POS': s_row[p_cols_pos[0]],
                            'P2_ID': pid2, 'P2_NAME': s_row[p_cols_name[1]], 'P2_POS': s_row[p_cols_pos[1]],
                            'P3_ID': pid3, 'P3_NAME': s_row[p_cols_name[2]], 'P3_POS': s_row[p_cols_pos[2]],
                            'P4_ID': pid4, 'P4_NAME': s_row[p_cols_name[3]], 'P4_POS': s_row[p_cols_pos[3]],
                            'P5_ID': pid5, 'P5_NAME': s_row[p_cols_name[4]], 'P5_POS': s_row[p_cols_pos[4]],
                            'MINUTES': round(duration / 60.0, 1), 'SECONDS': duration,
                            'PTS_FOR': pts_for, 'PTS_AGAINST': pts_agt, 'PLUS_MINUS': pts_for - pts_agt,
                        })
            procesados_ahora += 1
        except Exception as e:
            errores += 1
            print(f"⚠️ Error procesando partido {match_id}: {e}")
            continue

    def append_and_save(new_data_list, filepath):
        if not new_data_list: return
        df_new = pd.concat(new_data_list, ignore_index=True) if isinstance(new_data_list[0], pd.DataFrame) else pd.DataFrame(new_data_list)
        if os.path.exists(filepath):
            try:
                df_old = pd.read_csv(filepath, dtype=str)
                df_final = pd.concat([df_old, df_new.astype(str)], ignore_index=True)
            except: df_final = df_new
        else: df_final = df_new
            
        df_final['ROUND_NUM'] = pd.to_numeric(df_final['ROUND'], errors='coerce').fillna(0).astype(int)
        df_final = df_final.sort_values(by=['ROUND_NUM', 'MATCHID']).drop(columns=['ROUND_NUM'])
        df_final.to_csv(filepath, index=False, encoding='utf-8-sig', float_format='%.1f')

    if all_boxscores: append_and_save(all_boxscores, OUT_BOXSCORE)
    if all_teamstats: append_and_save(all_teamstats, OUT_TEAMSTATS)
    if all_pbp: append_and_save(all_pbp, OUT_PBP)
    
    if all_lineups:
        df_lu = pd.DataFrame(all_lineups)
        agrupadores = ['MATCHID', 'ROUND', 'TEAM_ID', 'TEAM', 'LOCATION', 
                       'P1_ID', 'P1_NAME', 'P1_POS', 'P2_ID', 'P2_NAME', 'P2_POS', 
                       'P3_ID', 'P3_NAME', 'P3_POS', 'P4_ID', 'P4_NAME', 'P4_POS', 'P5_ID', 'P5_NAME', 'P5_POS']
        df_lu_final = df_lu.groupby(agrupadores).sum(numeric_only=True).reset_index()
        df_lu_final['PTS_FOR_PER40'] = (df_lu_final['PTS_FOR'] * 2400 / df_lu_final['SECONDS'].replace(0, np.nan)).round(1).fillna(0)
        df_lu_final['PTS_AGT_PER40'] = (df_lu_final['PTS_AGAINST'] * 2400 / df_lu_final['SECONDS'].replace(0, np.nan)).round(1).fillna(0)
        df_lu_final['NET_PER40'] = (df_lu_final['PLUS_MINUS'] * 2400 / df_lu_final['SECONDS'].replace(0, np.nan)).round(1).fillna(0)
        append_and_save([df_lu_final], OUT_LINEUPS)
        
    return procesados_ahora, errores

def auditoria_calidad():
    try:
        print("\n📊 --- AUDITORÍA TOTAL DE CALIDAD DE DATOS ---")
        df_cal = pd.read_csv(ARCHIVO_CALENDARIO) if os.path.exists(ARCHIVO_CALENDARIO) else pd.DataFrame()
        
        if os.path.exists(OUT_LINEUPS):
            df_lu = pd.read_csv(OUT_LINEUPS)
            tiempos = df_lu.groupby(['MATCHID', 'TEAM'])['SECONDS'].sum().reset_index()
            tiempos_invalidos = tiempos[~tiempos['SECONDS'].isin([2400, 2700, 3000, 3300, 3600])]
            if not tiempos_invalidos.empty:
                print(f"⚠️ AVISO LINEUPS (MINUTOS): {len(tiempos_invalidos)} equipos tienen sumas inusuales.")
            else:
                print("✅ LINEUPS (MINUTOS): Todos suman 40/45/50 mins.")

            if not df_cal.empty:
                pts_lu = df_lu.groupby(['MATCHID', 'LOCATION'])['PTS_FOR'].sum().unstack()
                if 'HOME' in pts_lu and 'AWAY' in pts_lu:
                    pts_lu['LU_SCORE'] = pts_lu['HOME'].fillna(0).astype(int).astype(str) + "-" + pts_lu['AWAY'].fillna(0).astype(int).astype(str)
                    cruce_lu = pd.merge(pts_lu.reset_index(), df_cal[['MATCHID', 'SCORE_STR']], on='MATCHID', how='inner')
                    cruce_lu['SCORE_STR_CLEAN'] = cruce_lu['SCORE_STR'].astype(str).str.replace(' ', '')
                    errores_lu = cruce_lu[cruce_lu['LU_SCORE'] != cruce_lu['SCORE_STR_CLEAN']]
                    if not errores_lu.empty:
                        print(f"⚠️ AVISO LINEUPS (PUNTOS): {len(errores_lu)} no cuadran con Oficial.")
                    else:
                        print("✅ LINEUPS (PUNTOS): Cuadran con Oficial.")

        if os.path.exists(OUT_PBP):
            df_pbp = pd.read_csv(OUT_PBP)
            fin_partidos = df_pbp.groupby('MATCHID')['SECONDS_REMAINING'].min().reset_index()
            incompletos = fin_partidos[fin_partidos['SECONDS_REMAINING'] > 0]
            if not incompletos.empty:
                print(f"⚠️ AVISO PBP (MINUTOS): {len(incompletos)} no terminan en 00:00.")
            else:
                print("✅ PBP (MINUTOS): Todos finalizan en 00:00.")

            if not df_cal.empty:
                scores_pbp = df_pbp.groupby('MATCHID').agg({'SCORE_H': 'max', 'SCORE_A': 'max'}).reset_index()
                scores_pbp['PBP_SCORE'] = scores_pbp['SCORE_H'].fillna(0).astype(int).astype(str) + "-" + scores_pbp['SCORE_A'].fillna(0).astype(int).astype(str)
                df_cal['MATCHID'] = df_cal['MATCHID'].astype(str) 
                scores_pbp['MATCHID'] = scores_pbp['MATCHID'].astype(str)
                cruce_pbp = pd.merge(scores_pbp, df_cal[['MATCHID', 'SCORE_STR']], on='MATCHID', how='inner')
                cruce_pbp['SCORE_STR_CLEAN'] = cruce_pbp['SCORE_STR'].astype(str).str.replace(' ', '')
                errores_pbp = cruce_pbp[cruce_pbp['PBP_SCORE'] != cruce_pbp['SCORE_STR_CLEAN']]
                if not errores_pbp.empty:
                    print(f"⚠️ AVISO PBP (PUNTOS): {len(errores_pbp)} descuadran con Oficial.")
                else:
                    print("✅ PBP (PUNTOS): Cuadran con Oficial.")

    except Exception as e:
        print(f"⚠️ Fallo crítico en la auditoría: {e}")

try:
    procesados, fails = procesar_estadisticas_acumuladas()
    print(f"\n✅ MÓDULO 3 COMPLETADO. Procesados: {procesados} | Errores: {fails}")
    auditoria_calidad()
except Exception as e:
    import traceback
    print(f"\n❌ Error Crítico en Módulo 3. Detalles:\n{traceback.format_exc()}")


# Módulo 4: SINCRONIZACIÓN AUTOMÁTICA CON GOOGLE SHEETS EN ÚNICO DOCUMENTO
# ==============================================================================
SPREADSHEET_NAME = "BD_PRIMERAFEB_2526"

ARCHIVOS_A_SUBIR = {
    OUT_BOXSCORE: "BOXSCORE",
    OUT_TEAMSTATS: "TEAMSTATS",
    OUT_PBP: "PBP",
    OUT_LINEUPS: "LINEUPS",
    ARCHIVO_ROSTER: "ROSTER",
    ARCHIVO_CALENDARIO: "CALENDAR"
}

def subir_a_google_sheets():
    print("⏳ Conectando con Google Sheets usando tu Bot de Servicio de GitHub Secrets...")
    try:
        credenciales_str = os.environ.get('GOOGLE_CREDENTIALS')
        if not credenciales_str:
            print("❌ ERROR: No se ha encontrado el secreto GOOGLE_CREDENTIALS en el entorno.")
            return

        credenciales_dict = json.loads(credenciales_str)
        gc = gspread.service_account_from_dict(credenciales_dict)
        print("✅ Autenticación exitosa.")

        try:
            sh = gc.open(SPREADSHEET_NAME)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"❌ ERROR: No encuentro el archivo principal '{SPREADSHEET_NAME}' en Drive. ¿Tiene el bot permisos de Editor?")
            return

        for ruta_csv, nombre_pestana in ARCHIVOS_A_SUBIR.items():
            if os.path.exists(ruta_csv):
                print(f"🔄 Sincronizando datos hacia pestaña -> {nombre_pestana}...")
                
                try:
                    worksheet = sh.worksheet(nombre_pestana)
                except gspread.exceptions.WorksheetNotFound:
                    print(f"   ⚠️ No existe la pestaña '{nombre_pestana}'. Creándola automáticamente...")
                    worksheet = sh.add_worksheet(title=nombre_pestana, rows="100", cols="20")
                
                df = pd.read_csv(ruta_csv, dtype=str) 
                worksheet.clear() 
                set_with_dataframe(worksheet, df) 
                print(f"   ✅ Pestaña '{nombre_pestana}' actualizada con éxito ({len(df)} filas).")
            else:
                print(f"   ⚠️ No se encontró el archivo local: {ruta_csv}. Saltando...")

        print("\n🚀 ¡Sincronización masiva completada al 100%!")

    except Exception as e:
        print(f"❌ Error crítico al subir a la nube: {e}")

if __name__ == "__main__":
    subir_a_google_sheets()
