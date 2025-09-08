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
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print("ERROR: Timeout al consultar la API del Banco de la República")
        raise
    except requests.exceptions.RequestException as e:
        print(f"ERROR en la consulta HTTP: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"ERROR decodificando JSON: {e}")
        raise

def get_timestamp_filename():
    """Genera el nombre del archivo basado en el timestamp actual."""
    timestamp = int(time.time())
    return f"dolar-{timestamp}.json"

def save_to_s3(data, filename):
    """Guarda los datos en un bucket S3."""
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"Archivo {filename} guardado exitosamente en bucket {BUCKET_NAME}")
    except Exception as e:
        print(f"ERROR guardando archivo en S3: {e}")
        raise

def f(event, context):
    """Función Lambda programada por cron: descarga datos del dólar y los guarda en S3."""
    print("=== INICIANDO FUNCIÓN FETCH ===")
    print(f"Event: {json.dumps(event, default=str)}")
    
    try:
        print("Consultando datos del Banco de la República...")
        data = fetch_dollar_data()
        
        print(f"Datos obtenidos: {len(data) if isinstance(data, list) else 'N/A'} registros")
        
        filename = get_timestamp_filename()
        save_to_s3(data, filename)
        
        result = {"status": "success", "filename": filename, "records": len(data) if isinstance(data, list) else None}
        print(f"=== FUNCIÓN FETCH COMPLETADA: {result} ===")
        return result
        
    except Exception as e:
        error_result = {"status": "error", "message": str(e)}
        print(f"=== ERROR EN FUNCIÓN FETCH: {error_result} ===")
        return error_result

# ========= Función 2: Procesar archivo nuevo en S3 y guardar en RDS =========

def validate_environment_variables():
    """Valida que todas las variables de entorno requeridas estén presentes."""
    required_vars = ['RDS_HOST', 'RDS_USER', 'RDS_PASSWORD', 'RDS_DB']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        error_msg = f"Variables de entorno faltantes: {missing_vars}"
        print(f"ERROR: {error_msg}")
        return False, error_msg
    
    # Log de variables (sin mostrar la password)
    print(f"Variables de entorno configuradas:")
    print(f"  RDS_HOST: {rds_host}")
    print(f"  RDS_USER: {rds_user}")
    print(f"  RDS_PASSWORD: {'*' * len(rds_password) if rds_password else 'NO CONFIGURADA'}")
    print(f"  RDS_DB: {rds_db}")
    
    return True, "OK"

def get_db_connection():
    """Obtiene la conexión a la base de datos RDS."""
    print(f"Intentando conectar a RDS: {rds_host}:{3306}/{rds_db}")
    
    try:
        connection = pymysql.connect(
            host=rds_host,
            port=3306,
            user=rds_user,
            password=rds_password,
            database=rds_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=10,
            write_timeout=10
        )
        print("Conexión a RDS establecida exitosamente")
        return connection
    except pymysql.Error as e:
        print(f"Error conectando a RDS: {e}")
        raise
    except Exception as e:
        print(f"Error inesperado conectando a RDS: {e}")
        raise

def create_table_if_not_exists(connection):
    """Crea la tabla dolar si no existe."""
    try:
        with connection.cursor() as cursor:
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
            connection.commit()
            print("Tabla 'dolar' verificada/creada")
    except Exception as e:
        print(f"Error creando tabla: {e}")
        raise

def process_dollar_data(data):
    """Procesa los datos del dólar y extrae el registro más reciente."""
    print(f"Procesando datos del dólar:")
    print(f"  Tipo de datos: {type(data)}")
    print(f"  Número de registros: {len(data) if isinstance(data, list) else 'N/A'}")
    
    if isinstance(data, list) and len(data) >= 3:
        print(f"  Primeros 3 registros: {data[:3]}")
        print(f"  Últimos 3 registros: {data[-3:]}")
    
    # Validar formato esperado: lista de listas
    if not isinstance(data, list) or not data:
        raise ValueError("El archivo JSON no contiene una lista de datos")
    
    if not isinstance(data[0], list):
        raise ValueError("El archivo JSON no tiene el formato esperado (lista de listas)")
    
    # Tomar el último registro (más reciente)
    latest_record = data[-1]
    
    if len(latest_record) < 2:
        raise ValueError(f"Registro inválido: {latest_record}. Se esperan al menos 2 elementos [timestamp, valor]")
    
    timestamp_ms, valor_str = latest_record[0], latest_record[1]
    
    # Convertir timestamp (milisegundos) a datetime
    try:
        fechahora = datetime.fromtimestamp(int(timestamp_ms) / 1000)
        fechahora_str = fechahora.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError) as e:
        raise ValueError(f"Error convirtiendo timestamp {timestamp_ms}: {e}")
    
    # Convertir valor a float
    try:
        valor = float(valor_str)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Error convirtiendo valor {valor_str}: {e}")
    
    print(f"Datos procesados: {fechahora_str} - ${valor}")
    return fechahora_str, valor

def insert_dollar_data(connection, fechahora, valor):
    """Inserta los datos del dólar en la base de datos."""
    try:
        with connection.cursor() as cursor:
            # Verificar si ya existe un registro con la misma fecha
            check_sql = "SELECT COUNT(*) as count FROM dolar WHERE fechahora = %s"
            cursor.execute(check_sql, (fechahora,))
            result = cursor.fetchone()
            
            if result['count'] > 0:
                print(f"Ya existe un registro para {fechahora}. Omitiendo inserción.")
                return "skipped"
            
            # Insertar nuevo registro
            insert_sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
            cursor.execute(insert_sql, (fechahora, valor))
            connection.commit()
            
            print(f"Registro insertado: {fechahora} - ${valor}")
            return "inserted"
            
    except pymysql.Error as e:
        print(f"Error en base de datos: {e}")
        connection.rollback()
        raise
    except Exception as e:
        print(f"Error inesperado insertando datos: {e}")
        connection.rollback()
        raise

def process_file(event, context):
    """Procesa el archivo S3 subido y guarda los datos en la base de datos."""
    print("=== INICIANDO FUNCIÓN PROCESS_FILE ===")
    print(f"Event: {json.dumps(event, default=str)}")
    
    connection = None
    
    try:
        # 1. Validar variables de entorno
        is_valid, validation_message = validate_environment_variables()
        if not is_valid:
            return {"status": "error", "message": validation_message}
        
        # 2. Obtener información del evento S3
        if 'Records' not in event or not event['Records']:
            raise ValueError("Evento S3 inválido: no se encontraron Records")
        
        s3_record = event['Records'][0]
        bucket = s3_record['s3']['bucket']['name']
        key = s3_record['s3']['object']['key']
        
        print(f"Procesando archivo: {key} del bucket: {bucket}")
        
        # 3. Descargar archivo desde S3
        print("Descargando archivo desde S3...")
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')
            print(f"Archivo descargado: {len(file_content)} bytes")
        except Exception as e:
            raise Exception(f"Error descargando archivo de S3: {e}")
        
        # 4. Procesar datos JSON
        try:
            data = json.loads(file_content)
        except json.JSONDecodeError as e:
            raise Exception(f"Error decodificando JSON: {e}")
        
        # 5. Procesar datos del dólar
        fechahora, valor = process_dollar_data(data)
        
        # 6. Conectar a la base de datos
        connection = get_db_connection()
        
        # 7. Crear tabla si no existe
        create_table_if_not_exists(connection)
        
        # 8. Insertar datos
        insert_result = insert_dollar_data(connection, fechahora, valor)
        
        result = {
            "status": "success", 
            "message": f"Procesado: {fechahora} - ${valor}",
            "action": insert_result,
            "file": key
        }
        
        print(f"=== FUNCIÓN PROCESS_FILE COMPLETADA: {result} ===")
        return result

    except Exception as e:
        error_result = {
            "status": "error", 
            "message": str(e),
            "error_type": type(e).__name__
        }
        print(f"=== ERROR EN FUNCIÓN PROCESS_FILE: {error_result} ===")
        return error_result

    finally:
        if connection:
            try:
                connection.close()
                print("Conexión a RDS cerrada")
            except Exception as close_err:
                print(f"Error cerrando conexión: {close_err}")