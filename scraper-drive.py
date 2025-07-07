# ===============================
# 1. Instalar dependencias necesarias
# ===============================
!pip install -q PyDrive pandas openpyxl

# ===============================
# 2. Autenticación con Google Drive
# ===============================
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

auth.authenticate_user()
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)

print("✅ Autenticado correctamente en Google Drive.")

# ===============================
# 3. Configuración de carpetas
# ===============================
FOLDER_ID = "root"  # carpeta a escanear; usa "root" para toda tu unidad
UPLOAD_FOLDER_ID = "ID_DE_TU_CARPETA_DESTINO"  # 👈 reemplaza con el ID real

# ===============================
# 4. Recorrido recursivo y recolección de metadatos
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
        
        # Contadores y testigo de proceso
        num_total += 1
        if num_total % 100 == 0:
            print(f"⏳ Procesados {num_total} elementos hasta ahora...")

        if file['mimeType'] == "application/vnd.google-apps.folder":
            num_carpetas += 1
            list_files_recursive(file['id'], path=file_path)
        else:
            num_archivos += 1

# ===============================
# 5. Ejecutar el escaneo
# ===============================
print("🚀 Iniciando escaneo de la carpeta...")
list_files_recursive(FOLDER_ID)
print("✅ Escaneo completo.")
print(f"📁 Carpetas encontradas: {num_carpetas}")
print(f"📄 Archivos encontrados: {num_archivos}")
print(f"📊 Total de elementos: {num_archivos + num_carpetas}")

# ===============================
# 6. Guardar el consolidado en un archivo Excel
# ===============================
df = pd.DataFrame(results)
local_output_file = "/content/drive_scraping_consolidado.xlsx"
df.to_excel(local_output_file, index=False)
print(f"✅ Archivo Excel generado localmente: {local_output_file}")

# ===============================
# 7. Subir el Excel a tu Google Drive en la carpeta deseada
# ===============================
uploaded_file = drive.CreateFile({
    'title': 'drive_scraping_consolidado.xlsx',
    'parents': [{'id': UPLOAD_FOLDER_ID}]
})
uploaded_file.SetContentFile(local_output_file)
uploaded_file.Upload()

print(f"✅ Archivo subido a tu Drive: https://drive.google.com/file/d/{uploaded_file['id']}/view")
