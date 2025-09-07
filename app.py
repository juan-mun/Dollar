import json
import time
import boto3
import requests
from datetime import datetime
import pytz

s3 = boto3.client("s3")
BUCKET_NAME = "dolar-raw-cmjm"
URL = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

def fetch_dollar_data():
    """Obtiene los datos del dólar desde el servicio REST del Banco de la República"""
    response = requests.get(URL)
    response.raise_for_status()
    return response.json()

def get_timestamp_filename():
    """Genera el nombre del archivo basado en timestamp actual"""
    timestamp = int(time.time())
    return f"dolar-{timestamp}.json"

def save_to_s3(data, filename):
    """Guarda los datos en un bucket S3"""
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(data),
        ContentType="application/json"
    )

def f(event, context):
    """Función principal que será ejecutada por AWS Lambda"""
    data = fetch_dollar_data()
    filename = get_timestamp_filename()
    save_to_s3(data, filename)
    return {"status": "success", "filename": filename}
