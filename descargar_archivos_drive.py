import os
import json
import io
import pandas as pd  # <--- Librería necesaria para el procesamiento
import tkinter as tk
from tkinter import filedialog
from urllib.parse import unquote
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# --- CONFIGURACIÓN ---
SCOPES = ['https://www.googleapis.com/auth/drive']
CONFIG_FILE = 'config_app.json'
FILTRO_EMAIL = "alejandra.aguero@dato.com.py"

def obtener_ruta_local():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            return config.get('ruta_descarga')
    
    root = tk.Tk()
    root.withdraw()
    ruta = filedialog.askdirectory(title="Selecciona la carpeta local")
    root.destroy()
    
    if ruta:
        with open(CONFIG_FILE, 'w') as f:
            json.dump({'ruta_descarga': ruta}, f)
    return ruta

def autenticar():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def obtener_categorias_dinamicas(service):
    query = "mimeType = 'application/vnd.google-apps.folder' and sharedWithMe = true and trashed = false"
    results = service.files().list(
        q=query,
        fields="files(id, name, sharingUser)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    items = results.get('files', [])
    return {item['name']: item['id'] for item in items 
            if item.get('sharingUser', {}).get('emailAddress', '').lower() == FILTRO_EMAIL.lower()}


def procesar_y_guardar_csv_limpio(byte_content, ruta_final, nombre_categoria):
    try:
        # 1. Leer forzando que TODO sea string (dtype=str) para evitar el ".0"
        # Usamos sep=None para detectar si el origen viene con , o ;
        try:
            df = pd.read_csv(io.BytesIO(byte_content), encoding='utf-8', sep=None, engine='python', dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(byte_content), encoding='latin-1', sep=None, engine='python', dtype=str)

        # 2. Limpiar encabezados (quitar saltos de línea o espacios molestos)
        df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]

        lineas_formateadas = []

        # 3. Formatear Encabezados
        # Resultado esperado: "col1,""col2"",""col3"""
        cols = df.columns.tolist()
        header_str = f'"{cols[0]},' + ",".join([f'""{c}""' for c in cols[1:]]) + '"'
        lineas_formateadas.append(header_str)

        # 4. Formatear Filas
        for _, row in df.iterrows():
            vals = row.values
            # Al leer con dtype=str, vals[0] ya no tendrá ".0"
            # Limpiamos posibles valores nulos (NaN) por si acaso
            vals = [str(v) if pd.notna(v) else "" for v in vals]
            
            # Formato: "valor1,""valor2"",""valor3"""
            row_str = f'"{vals[0]},' + ",".join([f'""{v}""' for v in vals[1:]]) + '"'
            lineas_formateadas.append(row_str)

        # 5. Guardar el archivo con la codificación adecuada para Paraguay/Windows
        with open(ruta_final, 'w', encoding='utf-8-sig') as f:
            f.write("\n".join(lineas_formateadas))
            
        print(f"  [Procesado] CSV formateado con éxito: {os.path.basename(ruta_final)}")
        return True

    except Exception as e:
        print(f"  [Error Procesamiento] Error en {nombre_categoria}: {e}")
        return False

def ejecutar_descarga():
    # ... (Configuración inicial igual) ...
    ruta_destino = obtener_ruta_local()
    if not ruta_destino: return
    service = autenticar()
    categorias = obtener_categorias_dinamicas(service)
    hoy = datetime.now(timezone.utc).date()

    for nombre_raw, id_raiz in categorias.items():
        nombre_categoria = unquote(nombre_raw)
        prefijo = nombre_categoria.replace(" ", "").replace("%20", "").replace(".", "")
        
        try:
            # 1. Buscar carpeta "2026" (Igual)
            q_2026 = f"'{id_raiz}' in parents and name = '2026' and trashed = false"
            res_2026 = service.files().list(q=q_2026, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            if not res_2026.get('files'): continue
            id_2026 = res_2026['files'][0]['id']

            # 2. Listar archivos (Igual)
            q_csv = f"'{id_2026}' in parents and trashed = false"
            res_csv = service.files().list(q=q_csv, fields="files(id, name, modifiedTime)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            archivos = res_csv.get('files', [])

            for archivo in archivos:
                if not archivo['name'].lower().endswith('.csv'): continue
                
                fecha_mod_str = archivo['modifiedTime'].split('T')[0]
                fecha_mod = datetime.strptime(fecha_mod_str, '%Y-%m-%d').date()

                if fecha_mod == hoy: #or fecha_mod < hoy:
                    nuevo_nombre = f"{prefijo}-{archivo['name']}"
                    ruta_final = os.path.join(ruta_destino, nuevo_nombre)

                    print(f"Descargando {nuevo_nombre}...")
                    
                    # --- CAMBIO AQUÍ: Descarga a memoria en lugar de archivo directo ---
                    request = service.files().get_media(fileId=archivo['id'], supportsAllDrives=True)
                    fh = io.BytesIO() # Buffer en memoria
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                    
                    # --- PROCESAMIENTO ---
                    # Enviamos los bytes acumulados en memoria a nuestra función de formato
                    procesar_y_guardar_csv_limpio(fh.getvalue(), ruta_final, nombre_categoria)
                    
                else:
                    print(f"Saltando {archivo['name']}: No actualizado hoy.")

        except Exception as e:
            print(f"Error en {nombre_categoria}: {e}")

if __name__ == '__main__':
    ejecutar_descarga()