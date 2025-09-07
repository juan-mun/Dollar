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
        return {"status": "success", "filename": filename}
    except Exception as e:
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
    try:
        # Obtener el nombre del archivo y el bucket desde el evento
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        # Descargar el archivo desde S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        # Procesar los datos JSON
        data = json.loads(file_content)

        # VALIDACIÓN: Asegúrate de que el valor esté donde esperas
        # Ejemplo: {'series': [{'valor': 3890.45}]}
        valor = None
        if isinstance(data, dict) and "series" in data and data["series"]:
            primer_serie = data["series"][0]
            valor = primer_serie.get("valor")

        if valor is None:
            return {"status": "error", "message": "No se encontró el valor del dólar en el archivo"}

        # Obtener fecha/hora actual
        fechahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insertar en base de datos
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
            cursor.execute(sql, (fechahora, valor))
            connection.commit()

        return {"status": "success", "message": "Datos insertados en la base de datos RDS"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        try:
            if connection:
                connection.close()
        except:
            pass
