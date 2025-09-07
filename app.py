import json
import time
import boto3
import requests
import pymysql
import os
from datetime import datetime

# Configuración
s3 = boto3.client("s3")
BUCKET_NAME = "dolar-raw-cmjm"
URL = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

# Variables de entorno para RDS
rds_host = os.environ.get('RDS_HOST')
rds_user = os.environ.get('RDS_USER')
rds_password = os.environ.get('RDS_PASSWORD')
rds_db = os.environ.get('RDS_DB')

# ========= Función 1: Obtener datos del dólar y guardarlos en S3 =========

def fetch_dollar_data():
    """Obtiene los datos del dólar desde el servicio REST del Banco de la República."""
    response = requests.get(URL)
    response.raise_for_status()
    return response.json()

def get_timestamp_filename():
    """Genera el nombre del archivo basado en el timestamp actual."""
    timestamp = int(time.time())
    return f"dolar-{timestamp}.json"

def save_to_s3(data, filename):
    """Guarda los datos en un bucket S3."""
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(data),
        ContentType="application/json"
    )

def f(event, context):
    """Función Lambda programada por cron: descarga datos del dólar y los guarda en S3."""
    try:
        data = fetch_dollar_data()
        filename = get_timestamp_filename()
        save_to_s3(data, filename)
        print(f"Archivo guardado en S3: {filename}")
        return {"status": "success", "filename": filename}
    except Exception as e:
        print(f"Error en función f: {e}")
        return {"status": "error", "message": str(e)}

# ========= Función 2: Procesar archivo nuevo en S3 y guardar en RDS =========

def get_db_connection():
    """Obtiene la conexión a la base de datos RDS."""
    return pymysql.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def process_file(event, context):
    """Procesa el archivo S3 subido y guarda los datos en la base de datos."""
    connection = None
    try:
        # Obtener el nombre del archivo y el bucket desde el evento
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Procesando archivo: {key} del bucket: {bucket}")

        # Descargar el archivo desde S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        # Procesar los datos JSON
        data = json.loads(file_content)

        # DEBUG: mostrar estructura del archivo
        print("Contenido del archivo JSON:", data)

        # Validar formato esperado: lista de listas
        if not isinstance(data, list) or not data or not isinstance(data[0], list):
            msg = "El archivo JSON no tiene el formato esperado (lista de listas)"
            print(msg)
            return {"status": "error", "message": msg}

        # Tomar el último registro (puedes cambiar a data[0] para el primero)
        timestamp_ms, valor_str = data[-1]

        # Convertir timestamp (milisegundos) a datetime legible
        fechahora = datetime.fromtimestamp(int(timestamp_ms) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        valor = float(valor_str)

        print(f"Insertando en base de datos: {fechahora} - {valor}")

        # Insertar en base de datos
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
            cursor.execute(sql, (fechahora, valor))
            connection.commit()

        print("Insertado correctamente.")
        return {"status": "success", "message": f"Insertado: {fechahora} - {valor}"}

    except Exception as e:
        print(f"ERROR en process_file: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        if connection:
            try:
                connection.close()
            except Exception as close_err:
                print(f"Error cerrando conexión: {close_err}")
