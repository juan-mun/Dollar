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

def obtener_y_guardar_dolar():
    """
    Obtiene los datos del mercado cambiario de la URL y los guarda en un JSON en S3.
    """
    try:
        print(">>> Consultando API del Banco de la República...")
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f">>> Datos obtenidos: {len(data) if isinstance(data, list) else 'N/A'} registros")
        
        # Crear nombre con timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"dolar-{timestamp}.json"
        
        # Subir a S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        print(f">>> Archivo guardado en s3://{BUCKET_NAME}/{filename}")
        return {"status": "ok", "file": filename}
    
    except Exception as e:
        print(f">>> ERROR en obtener_y_guardar_dolar: {e}")
        return {"status": "error", "message": str(e)}

def f(event, context):
    """
    Función Lambda programada por cron: descarga datos del dólar y los guarda en S3.
    Incluye límite de fecha como el ejemplo de referencia.
    """
    print(">>> Lambda f ejecutada")
    print(f">>> Event: {json.dumps(event, default=str)}")
    
    # Límite de fecha (opcional, como en el ejemplo)
    hoy = datetime.utcnow().date()
    limite = datetime(2026, 12, 31).date()  # Ajusta según necesites
    
    if hoy > limite:
        print(">>> El proceso ya no se ejecuta después del límite establecido.")
        return {"status": "skipped", "message": "fuera de rango"}
    
    print(">>> Iniciando proceso de obtención de datos...")
    result = obtener_y_guardar_dolar()
    
    print(f">>> Resultado: {result}")
    return result

# ========= Función 2: Procesar archivo nuevo en S3 y guardar en RDS =========

def process_file(event, context):
    """
    Lambda que carga SOLO el último valor de un archivo JSON en S3 a MySQL (RDS).
    """
    db_conn = getattr(context, "db_conn", None)

    try:
        print(">>> Iniciando Lambda process_file...")
        
        
        print(">>> Validando Variables...")
        # 1. Validar variables de entorno si no hay conexión inyectada
        if db_conn is None:
            required_vars = ['RDS_HOST', 'RDS_USER', 'RDS_PASSWORD', 'RDS_DB']
            missing_vars = [var for var in required_vars if not os.environ.get(var)]
            if missing_vars:
                error_msg = f"Variables de entorno faltantes: {missing_vars}"
                print(f">>> ERROR: {error_msg}")
                return {"status": "error", "message": error_msg}

        # 2. Extraer info del evento S3
        if 'Records' not in event or not event['Records']:
            raise ValueError("Evento S3 inválido: no se encontraron Records")
        
        print(">>> Leyendo Bucket...")
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f">>> Procesando archivo {key} desde bucket {bucket}")
        
        print(">>> Descargando...")
        # 3. Descargar archivo desde S3
        s3 = boto3.client("s3", region_name="us-east-1")
        print(f">>> s3: {s3}...")
        obj = s3.get_object(Bucket=bucket, Key=key)
        print(f">>> obj: {obj}...")
        body = obj["Body"].read()
        print(f">>> Archivo descargado, {len(body)} bytes")

        # 4. Cargar JSON
        data = json.loads(body)
        print(f">>> JSON cargado correctamente: {type(data)} con {len(data)} registros")

        # Validar que sea lista y no esté vacía
        if not isinstance(data, list) or not data:
            raise ValueError("El archivo JSON no contiene una lista de datos válida")

        # 5. Tomar solo el último valor
        ultimo = data[-1]
        if not isinstance(ultimo, list) or len(ultimo) < 2:
            raise ValueError(f"Formato inválido en el último registro: {ultimo}")

        timestamp_ms, valor = ultimo[0], ultimo[1]
        fechahora = datetime.fromtimestamp(int(timestamp_ms) / 1000)
        valor = float(valor)

        print(f">>> Último valor encontrado: {fechahora} → {valor}")

        # 6. Conectar a la DB si no hay conexión inyectada
        cerrar_conexion = False
        if db_conn is None:
            print(">>> Conectando a DB real...")
            db_conn = pymysql.connect(
                host=rds_host,
                port=3306,
                user=rds_user,
                password=rds_password,
                database=rds_db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.Cursor,
                connect_timeout=10,
                read_timeout=10,
                write_timeout=10
            )
            cerrar_conexion = True
            print(">>> Conexión establecida")

        # 7. Crear tabla si no existe
        cursor = db_conn.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dolar (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fechahora DATETIME NOT NULL,
            valor DECIMAL(10,4) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fechahora (fechahora)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        db_conn.commit()
        print(">>> Tabla verificada/creada")

        # 8. Insertar solo un registro
        insert_query = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        cursor.execute(insert_query, (fechahora, valor))
        db_conn.commit()

        cursor.close()
        if cerrar_conexion:
            db_conn.close()
            print(">>> Conexión cerrada")

        print(">>> Inserción completada: 1 registro insertado")

        return {
            "status": "ok",
            "rows_processed": 1,
            "rows_inserted": 1,
            "file": key
        }

    except Exception as e:
        print(f">>> ERROR en Lambda process_file: {e}")
        return {"status": "error", "message": str(e)}
