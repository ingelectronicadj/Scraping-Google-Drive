# ===============================
# 1. Instalar dependencias necesarias
# ===============================
!pip install -q PyDrive pandas gspread gspread_dataframe

# ===============================
# 2. Autenticación con Google Drive y Google Sheets
# ===============================
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

auth.authenticate_user()
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)

print("✅ Autenticado correctamente en Google Drive y Google Sheets.")

# ===============================
# 3. Configuración de carpeta a analizar y hoja de cálculo
# ===============================
# 👇 Coloca el ID de la carpeta específica que quieres escanear (NO uses "root")
FOLDER_ID = "ID_DE_TU_CARPETA_INICIAL"  # por ejemplo: "1PAEXqtyaSJU8Uc84tDodXpZmjQqxQchO"

# 👇 Coloca el ID o la URL de la hoja de cálculo de Google Sheets donde quieres cargar el consolidado
SHEET_ID_OR_URL = "COLOCA_AQUI_ID_O_URL_DE_LA_HOJA"

# ===============================
# 4. Función para extraer ID de Google Sheets desde URL
# ===============================
import re

def extract_sheet_id(input_string):
    """
    Extrae el ID de una hoja de cálculo de Google Sheets desde un ID puro o desde su URL.
    """
    # Si el input es solo el ID (no tiene https), lo devolvemos directo
    if not input_string.startswith("http"):
        return input_string.strip()
    # Si es una URL, buscamos el patrón del ID
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', input_string)
    if match:
        return match.group(1)
    raise ValueError("No se pudo extraer el ID de la hoja de cálculo. Verifica el enlace o ID.")

SHEET_ID = extract_sheet_id(SHEET_ID_OR_URL)
print(f"✅ ID de la hoja de cálculo extraído correctamente: {SHEET_ID}")

# ===============================
# 4.1 Verificar acceso a la carpeta inicial
# ===============================
try:
    folder = drive.CreateFile({'id': FOLDER_ID})
    folder.FetchMetadata()  # obtener metadatos
    folder_name = folder['title']
    print(f"✅ Acceso a carpeta inicial confirmado: '{folder_name}'")
    print(f"🔗 Link a la carpeta: https://drive.google.com/drive/folders/{FOLDER_ID}")
except Exception as e:
    raise RuntimeError(f"❌ Error: no se pudo acceder a la carpeta con ID '{FOLDER_ID}'. "
                       f"Verifica que exista y tengas permisos. Detalle: {e}")

# ===============================
# 5. Recorrido recursivo y recolección de metadatos
# ===============================
import pandas as pd

results = []
num_archivos = 0
num_carpetas = 0
num_total = 0  # contador global de elementos procesados

def list_files_recursive(parent_id, path=""):
    global num_archivos, num_carpetas, num_total
    query = f"'{parent_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    
    for file in file_list:
        file_path = f"{path}/{file['title']}"
        
        owner = (
            file['owners'][0]['displayName'] if 'owners' in file else "Desconocido"
        )
        owner_email = (
            file['owners'][0]['emailAddress'] if 'owners' in file else "Desconocido"
        )

        parent_id = file['parents'][0]['id'] if 'parents' in file and file['parents'] else "Sin padre"
        parent_link = (
            f"https://drive.google.com/drive/folders/{parent_id}" if parent_id != "Sin padre" else "N/A"
        )
        
        # Reordenar: Tipo MIME será el primer campo
        results.append({
            "Tipo MIME": file['mimeType'],
            "Nombre": file['title'],
            "ID": file['id'],
            "Ruta": file_path,
            "Link de vista": f"https://drive.google.com/file/d/{file['id']}/view",
            "ID Carpeta Contenedora": parent_id,
            "Link Carpeta Contenedora": parent_link,
            "Descripción": file.get('description', ""),
            "Tamaño (bytes)": file.get('fileSize', "N/A"),
            "Extensión": file.get('fileExtension', "N/A"),
            "Creado": file.get('createdDate', "N/A"),
            "Modificado": file.get('modifiedDate', "N/A"),
            "Propietario": owner,
            "Email del propietario": owner_email,
            "Versión": file.get('version', "N/A"),
        })
        
        num_total += 1
        if num_total % 100 == 0:
            print(f"⏳ Procesados {num_total} elementos hasta ahora...")

        if file['mimeType'] == "application/vnd.google-apps.folder":
            num_carpetas += 1
            list_files_recursive(file['id'], path=file_path)
        else:
            num_archivos += 1

# ===============================
# 6. Ejecutar el escaneo
# ===============================
print("🚀 Iniciando escaneo de la carpeta...")
list_files_recursive(FOLDER_ID)
print("✅ Escaneo completo.")
print(f"📁 Carpetas encontradas: {num_carpetas}")
print(f"📄 Archivos encontrados: {num_archivos}")
print(f"📊 Total de elementos: {num_archivos + num_carpetas}")

# ===============================
# 7. Cargar el consolidado en Google Sheets
# ===============================
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.authorize(gauth.credentials)

try:
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.get_worksheet(0)  # primera hoja
    worksheet.clear()
    df = pd.DataFrame(results)
    set_with_dataframe(worksheet, df)
    print(f"✅ Consolidado cargado en Google Sheets: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
except Exception as e:
    raise RuntimeError(f"❌ Error: no se pudo cargar el consolidado en la hoja de cálculo. "
                       f"Verifica el ID o permisos. Detalle: {e}")
