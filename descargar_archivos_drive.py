import os
import json
import io
import tkinter as tk
from tkinter import filedialog
from urllib.parse import unquote
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
    ruta = filedialog.askdirectory(title="Selecciona la carpeta local para guardar los CSV")
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
    """Busca carpetas compartidas filtrando por el email de la propiedad sharingUser."""
    print(f"Buscando categorías compartidas por: {FILTRO_EMAIL}...")
    
    # Nota: En unidades compartidas, a veces 'owners' está vacío. 
    # Listamos archivos compartidos y filtramos en Python para mayor precisión.
    query = "mimeType = 'application/vnd.google-apps.folder' and sharedWithMe = true and trashed = false"
    
    results = service.files().list(
        q=query,
        pageSize=100, 
        fields="files(id, name, sharingUser)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    items = results.get('files', [])
    categorias = {}

    for item in items:
        # Extraemos el email del objeto sharingUser que nos mostraste
        sharing_user = item.get('sharingUser', {})
        email_sharing = sharing_user.get('emailAddress', '').lower()
        
        if email_sharing == FILTRO_EMAIL.lower():
            categorias[item['name']] = item['id']
    
    print(f"Se encontraron {len(categorias)} categorías de {FILTRO_EMAIL}.")
    return categorias

def ejecutar_descarga():
    ruta_destino = obtener_ruta_local()
    if not ruta_destino:
        return

    service = autenticar()
    categorias = obtener_categorias_dinamicas(service)

    for nombre_raw, id_raiz in categorias.items():
        # Decodificar %20 y limpiar nombre para el prefijo
        nombre_categoria = unquote(nombre_raw)
        prefijo = nombre_categoria.replace(" ", "").replace("%20", "")
        
        print(f"\nProcesando: {nombre_categoria}")

        try:
            # 1. Buscar la subcarpeta "2026"
            # Importante: En Shared Drives hay que mantener supportsAllDrives=True siempre
            q_2026 = f"'{id_raiz}' in parents and name = '2026' and trashed = false"
            res_2026 = service.files().list(
                q=q_2026, 
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            carpetas_2026 = res_2026.get('files', [])

            if not carpetas_2026:
                print(f"  - Sin carpeta '2026'.")
                continue

            id_2026 = carpetas_2026[0]['id']

            # 2. Listar archivos CSV dentro de 2026
            q_csv = f"'{id_2026}' in parents and name contains '.csv' and trashed = false"
            res_csv = service.files().list(
                q=q_csv, 
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            archivos = res_csv.get('files', [])

            for archivo in archivos:
                # Nombre final: CATEGORIA-NombreOriginal.csv
                # El archivo original ya viene como '2026-Enero.csv'
                nuevo_nombre = f"{prefijo}-{archivo['name']}"
                ruta_final = os.path.join(ruta_destino, nuevo_nombre)

                # 3. Descarga
                request = service.files().get_media(fileId=archivo['id'])
                with io.FileIO(ruta_final, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                
                print(f"  [OK] {nuevo_nombre}")

        except Exception as e:
            print(f"  [Error] en {nombre_categoria}: {e}")

if __name__ == '__main__':
    ejecutar_descarga()