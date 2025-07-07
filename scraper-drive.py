# ===============================
# 1. Instalar dependencias necesarias
# ===============================
!pip install -q google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client gspread gspread_dataframe pandas

# ===============================
# 2. Autenticación y configuración inicial
# ===============================
from google.colab import auth
auth.authenticate_user()

from googleapiclient.discovery import build
from google.auth import default
import pandas as pd
import re

# Obtener credenciales y construir servicio de Drive
creds, _ = default()
drive_service = build('drive', 'v3', credentials=creds)

# Configura tu carpeta de inicio y hoja de cálculo
FOLDER_ID = "1JkCxG-XXHf9SWed5FtpL-KgJTGW-rz3s"
SHEET_ID_OR_URL = "1HuXReILfKCsjEbK7m1DgBD2NIeQ7f0ltrxz0ThU4tf8"

# ===============================
# 3. Extraer ID de hoja y verificar acceso a la carpeta
# ===============================
def extract_sheet_id(input_string):
    if not input_string.startswith("http"):
        return input_string.strip()
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', input_string)
    if match:
        return match.group(1)
    raise ValueError("❌ No se pudo extraer el ID de la hoja.")

SHEET_ID = extract_sheet_id(SHEET_ID_OR_URL)

# Verificar acceso a carpeta
try:
    folder = drive_service.files().get(fileId=FOLDER_ID, fields="name").execute()
    print(f"✅ Acceso a carpeta confirmado: '{folder['name']}'")
    print(f"🔗 https://drive.google.com/drive/folders/{FOLDER_ID}")
except Exception as e:
    raise RuntimeError(f"❌ No se pudo acceder a la carpeta. Detalle: {e}")


# ===============================
# 4. Función recursiva para explorar Google Drive
# ===============================
results = []
total = 0
archivos = 0
carpetas = 0

def list_drive_files(folder_id, path=""):
    global total, archivos, carpetas
    page_token = None
    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, parents, owners, fileExtension, description, version)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        
        for file in response.get('files', []):
            file_path = f"{path}/{file['name']}"
            parent_id = file['parents'][0] if 'parents' in file and file['parents'] else "Sin padre"
            parent_link = f"https://drive.google.com/drive/folders/{parent_id}" if parent_id != "Sin padre" else "N/A"

            results.append({
                "Tipo MIME": file.get('mimeType', ''),
                "Nombre": file.get('name', ''),
                "ID": file.get('id', ''),
                "Ruta": file_path,
                "Link de vista": f"https://drive.google.com/file/d/{file['id']}/view",
                "ID Carpeta Contenedora": parent_id,
                "Link Carpeta Contenedora": parent_link,
                "Descripción": file.get('description', ''),
                "Tamaño (bytes)": file.get('size', 'N/A'),
                "Extensión": file.get('fileExtension', ''),
                "Creado": file.get('createdTime', ''),
                "Modificado": file.get('modifiedTime', ''),
                "Propietario": file['owners'][0]['displayName'] if 'owners' in file else 'Desconocido',
                "Email del propietario": file['owners'][0]['emailAddress'] if 'owners' in file else 'Desconocido',
                "Versión": file.get('version', ''),
            })

            total += 1
            if total % 100 == 0:
                print(f"⏳ Procesados {total} elementos...")

            if file['mimeType'] == 'application/vnd.google-apps.folder':
                carpetas += 1
                list_drive_files(file['id'], file_path)
            else:
                archivos += 1

        page_token = response.get('nextPageToken', None)
        if not page_token:
            break


# ===============================
# 5. Ejecutar el escaneo
# ===============================
print("🚀 Iniciando escaneo...")
list_drive_files(FOLDER_ID)
print("✅ Escaneo finalizado.")
print(f"📁 Carpetas: {carpetas}")
print(f"📄 Archivos: {archivos}")
print(f"📊 Total: {total}")


# ===============================
# 6. Cargar en Google Sheets
# ===============================
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.authorize(creds)

try:
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.get_worksheet(0)
    worksheet.clear()
    df = pd.DataFrame(results)
    set_with_dataframe(worksheet, df)
    print(f"✅ Consolidado cargado en: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
except Exception as e:
    raise RuntimeError(f"❌ No se pudo escribir en la hoja. Detalle: {e}")