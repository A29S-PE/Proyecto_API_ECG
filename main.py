from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os
import json

# --- Configuración ---
FOLDER_ID = "1i_L7rPHuCHDt2heyeeKhTJA7wdz9-LOQ"          # <-- Cambia por tu carpeta en Google Drive
INDEX_FILE_ID = "1YSHcmYipoYfb4Wv7CnzaV2uiqF9tG9A7"         # <-- Cambia por tu archivo records_index.json en Drive

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# --- Inicialización de credenciales desde variable de entorno ---
service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

app = FastAPI()
records_index = []

# --- Cargar índice al iniciar la app ---
@app.on_event("startup")
def load_index():
    global records_index
    request = drive_service.files().get_media(fileId=INDEX_FILE_ID)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    records_index = json.loads(fh.read().decode("utf-8"))
    print(f"Índice cargado: {len(records_index)} registros")


# --- Endpoint: listar registros ---
@app.get("/records")
def get_records_index():
    return JSONResponse(content=records_index)


# --- Función auxiliar para buscar archivo por nombre ---
def find_file_id(record_id: str, ext: str):
    filename = f"{record_id}.{ext}"
    query = f"name = '{filename}' and '{FOLDER_ID}' in parents and trashed = false"
    print(query)
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        fields="files(id, name)",
        pageSize=1
    ).execute()
    print(results)
    files = results.get('files', [])
    if files:
        return files[0]['id']
    return None


# --- Endpoint: descargar archivo como stream ---
@app.get("/record/{record_id}/{ext}")
def get_record_file(record_id: str, ext: str):
    print(f"[REQUEST] Obteniendo archivo: {record_id}.{ext}")
    if ext not in ["hea", "mat"]:
        print(f"[ERROR] Extensión inválida: {ext}")
        raise HTTPException(status_code=400, detail="Extensión no válida")

    file_id = find_file_id(record_id, ext)
    if not file_id:
        print(f"[ERROR] Archivo no encontrado: {record_id}.{ext}")
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    request = drive_service.files().get_media(fileId=file_id)

    def file_stream():
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
            fh.seek(0)
            chunk = fh.read()
            yield chunk
            fh.seek(0)
            fh.truncate(0)

    return StreamingResponse(file_stream(), media_type="application/octet-stream")
