import os
import json
import io
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
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
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

def ejecutar_descarga():
    ruta_destino = obtener_ruta_local()
    if not ruta_destino: return
    
    service = autenticar()
    categorias = obtener_categorias_dinamicas(service)
    
    # Fecha de hoy en formato YYYY-MM-DD para comparar
    hoy = datetime.now().date()

    for nombre_raw, id_raiz in categorias.items():
        nombre_categoria = unquote(nombre_raw)
        prefijo = nombre_categoria.replace(" ", "").replace("%20", "")
        
        try:
            # 1. Buscar carpeta "2026"
            q_2026 = f"'{id_raiz}' in parents and name = '2026' and trashed = false"
            res_2026 = service.files().list(q=q_2026, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            
            if not res_2026.get('files'): continue
            id_2026 = res_2026['files'][0]['id']

            # 2. Listar archivos con metadatos de modificación
            q_csv = f"'{id_2026}' in parents and trashed = false"
            res_csv = service.files().list(
                q=q_csv, 
                fields="files(id, name, modifiedTime)", 
                supportsAllDrives=True, 
                includeItemsFromAllDrives=True
            ).execute()
            
            archivos = res_csv.get('files', [])
            
            if not archivos:
                print(f"--- {nombre_categoria}: Carpeta 2026 vacía o sin archivos.")
                continue

            for archivo in archivos:
                # VALIDACIÓN 1: ¿Es un archivo .csv?
                if not archivo['name'].lower().endswith('.csv'):
                    continue
                
                # VALIDACIÓN 2: ¿Fue modificado hoy?
                # modifiedTime viene como '2026-02-10T10:30:00.000Z'
                fecha_mod_str = archivo['modifiedTime'].split('T')[0]
                fecha_mod = datetime.strptime(fecha_mod_str, '%Y-%m-%d').date()

                #if fecha_mod == hoy or fecha_mod < hoy:
                if fecha_mod == hoy:
                    nuevo_nombre = f"{prefijo}-{archivo['name']}"
                    ruta_final = os.path.join(ruta_destino, nuevo_nombre)

                    print(f"Descargando {nuevo_nombre} (Modificado hoy: {fecha_mod_str})")
                    
                    request = service.files().get_media(fileId=archivo['id'], supportsAllDrives=True)
                    with io.FileIO(ruta_final, 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                else:
                    print("No existe archivos actualizados hoy en la carpeta 2026.")

        except Exception as e:
            print(f"Error en {nombre_categoria}: {e}")

if __name__ == '__main__':
    ejecutar_descarga()