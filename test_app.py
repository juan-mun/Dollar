import pytest
import os
import json
import time
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
from app import (
    obtener_y_guardar_dolar,
    f,
    process_file
)

# ========= Tests para la función fetch (obtener datos) =========

@patch('app.requests.get')
@patch('app.s3.put_object')
def test_obtener_y_guardar_dolar_success(mock_s3_put, mock_requests_get):
    """Test exitoso de obtención y guardado de datos"""
    # Mock de la respuesta de la API
    mock_response = MagicMock()
    mock_response.json.return_value = [["1725753600000", "4200.50"], ["1725757200000", "4201.25"]]
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response
    
    # Mock de S3
    mock_s3_put.return_value = None
    
    # Ejecutar función
    result = obtener_y_guardar_dolar()
    
    # Verificar resultado
    assert result["status"] == "ok"
    assert "file" in result
    assert result["file"].startswith("dolar-")
    assert result["file"].endswith(".json")
    
    # Verificar que se llamó a la API
    mock_requests_get.assert_called_once_with(
        "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario",
        timeout=30
    )
    
    # Verificar que se guardó en S3
    mock_s3_put.assert_called_once()
    call_args = mock_s3_put.call_args
    assert call_args[1]['Bucket'] == 'dolar-raw-cmjm'
    assert call_args[1]['ContentType'] == 'application/json'

@patch('app.requests.get')
def test_obtener_y_guardar_dolar_api_error(mock_requests_get):
    """Test manejo de error en la API"""
    mock_requests_get.side_effect = Exception("API Error")
    
    result = obtener_y_guardar_dolar()
    
    assert result["status"] == "error"
    assert "API Error" in result["message"]

@patch('app.obtener_y_guardar_dolar')
def test_f_function_success(mock_obtener):
    """Test exitoso de la función lambda f"""
    mock_obtener.return_value = {"status": "ok", "file": "dolar-test.json"}
    
    event = {}
    context = MagicMock()
    
    result = f(event, context)
    
    assert result["status"] == "ok"
    assert "file" in result
    mock_obtener.assert_called_once()

def test_f_function_date_limit():
    """Test que la función f respete el límite de fecha"""
    with patch('app.datetime') as mock_datetime:
        # Simular fecha posterior al límite
        mock_datetime.utcnow.return_value.date.return_value = datetime(2027, 1, 1).date()
        mock_datetime.return_value = datetime(2026, 12, 31)  # límite
        
        result = f({}, MagicMock())
        
        assert result["status"] == "skipped"
        assert "fuera de rango" in result["message"]

# ========= Tests para la función process_file =========

def create_s3_event(bucket="dolar-raw-cmjm", key="dolar-test.json"):
    """Crea un evento S3 mock para testing"""
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key}
            }
        }]
    }

def create_mock_context(db_conn=None):
    """Crea un contexto mock para testing"""
    context = MagicMock()
    if db_conn:
        context.db_conn = db_conn
    else:
        # Si no hay db_conn, el getattr debería retornar None
        context.configure_mock(**{'db_conn': None})
        def getattr_side_effect(obj, attr, default=None):
            if attr == 'db_conn':
                return default
            return getattr(obj, attr, default)
        
    return context

@patch('app.boto3.client')
def test_process_file_invalid_event(mock_boto3):
    """Test manejo de evento S3 inválido"""
    # Evento vacío
    result = process_file({}, create_mock_context())
    assert result["status"] == "error"
    assert "Evento S3 inválido" in result["message"]
    
    # Evento sin Records
    result = process_file({"Records": []}, create_mock_context())
    assert result["status"] == "error"
    assert "Evento S3 inválido" in result["message"]

@patch('app.boto3.client')
def test_process_file_missing_env_vars(mock_boto3):
    """Test manejo de variables de entorno faltantes"""
    with patch.dict(os.environ, {}, clear=True):
        event = create_s3_event()
        context = create_mock_context()
        
        result = process_file(event, context)
        
        assert result["status"] == "error"
        assert "Variables de entorno faltantes" in result["message"]

@patch('app.boto3.client')
def test_process_file_s3_download_error(mock_boto3):
    """Test manejo de error al descargar de S3"""
    # Mock S3 client que falla
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("S3 Error")
    mock_boto3.return_value = mock_s3
    
    # Mock variables de entorno
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-pass',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        event = create_s3_event()
        context = create_mock_context()
        
        result = process_file(event, context)
        
        assert result["status"] == "error"
        assert "S3 Error" in result["message"]

@patch('app.boto3.client')
def test_process_file_success_with_injected_db(mock_boto3):
    """Test exitoso con conexión de DB inyectada"""
    # Mock S3
    mock_s3 = MagicMock()
    mock_response = MagicMock()
    test_data = [["1725753600000", "4200.50"], ["1725757200000", "4201.25"]]
    mock_response.read.return_value = json.dumps(test_data).encode()
    mock_s3.get_object.return_value = {"Body": mock_response}
    mock_boto3.return_value = mock_s3
    
    # Mock DB connection
    mock_db_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 2  # Simular 2 filas insertadas
    mock_db_conn.cursor.return_value = mock_cursor
    
    # Context con DB inyectada
    context = create_mock_context(db_conn=mock_db_conn)
    
    event = create_s3_event()
    result = process_file(event, context)
    
    # Verificaciones
    assert result["status"] == "ok"
    assert result["rows_processed"] == 2
    assert result["rows_inserted"] == 2
    assert result["file"] == "dolar-test.json"
    
    # Verificar que se usó la conexión inyectada
    mock_db_conn.cursor.assert_called()
    mock_cursor.execute.assert_called()  # Para crear tabla
    mock_cursor.executemany.assert_called()  # Para insertar datos

@patch('app.boto3.client')
@patch('app.pymysql.connect')
def test_process_file_success_with_real_db_connection(mock_pymysql_connect, mock_boto3):
    """Test exitoso con conexión real a DB"""
    # Mock S3
    mock_s3 = MagicMock()
    mock_response = MagicMock()
    test_data = [["1725753600000", "4200.50"], ["1725757200000", "4201.25"], ["1725760800000", "4202.75"]]
    mock_response.read.return_value = json.dumps(test_data).encode()
    mock_s3.get_object.return_value = {"Body": mock_response}
    mock_boto3.return_value = mock_s3
    
    # Mock DB connection
    mock_db_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 3  # Simular 3 filas insertadas
    mock_db_conn.cursor.return_value = mock_cursor
    mock_pymysql_connect.return_value = mock_db_conn
    
    # Variables de entorno
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-pass',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        event = create_s3_event()
        context = create_mock_context()  # Sin DB inyectada
        
        result = process_file(event, context)
        
        # Verificaciones
        assert result["status"] == "ok"
        assert result["rows_processed"] == 3
        assert result["rows_inserted"] == 3
        
        # Verificar conexión a DB
        mock_pymysql_connect.assert_called_once_with(
            host='test-host',
            port=3306,
            user='test-user',
            password='test-pass',
            database='test-db',
            charset='utf8mb4',
            cursorclass=pytest.mock.ANY,
            connect_timeout=10,
            read_timeout=10,
            write_timeout=10
        )
        
        # Verificar que se cerró la conexión
        mock_db_conn.close.assert_called_once()

@patch('app.boto3.client')
def test_process_file_invalid_json_data(mock_boto3):
    """Test manejo de datos JSON inválidos"""
    # Mock S3 con datos inválidos
    mock_s3 = MagicMock()
    mock_response = MagicMock()
    
    # Diferentes casos de datos inválidos
    test_cases = [
        "not a list",  # String en lugar de lista
        [],  # Lista vacía
        {"key": "value"},  # Objeto en lugar de lista
    ]
    
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-pass',
        'RDS_DB': 'test-db'
    }
    
    for invalid_data in test_cases:
        mock_response.read.return_value = json.dumps(invalid_data).encode()
        mock_s3.get_object.return_value = {"Body": mock_response}
        mock_boto3.return_value = mock_s3
        
        with patch.dict(os.environ, env_vars):
            event = create_s3_event()
            context = create_mock_context()
            
            result = process_file(event, context)
            
            assert result["status"] == "error"
            assert "lista de datos" in result["message"] or "No se pudieron procesar datos" in result["message"]

# ========= Tests de conexión RDS =========

@pytest.mark.skipif(
    not all([
        os.environ.get('RDS_HOST'),
        os.environ.get('RDS_USER'),
        os.environ.get('RDS_PASSWORD'),
        os.environ.get('RDS_DB')
    ]), 
    reason="Variables de entorno RDS no configuradas para testing"
)
def test_real_rds_connection():
    """Test real de conexión a RDS (solo si las variables están configuradas)"""
    result = test_connection()
    assert result["status"] == "ok"
    assert "Conexión a RDS exitosa" in result["message"]

@patch('app.pymysql.connect')
def test_test_connection_success(mock_connect):
    """Test exitoso de la función test_connection"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-pass',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        result = test_connection()
        
        assert result["status"] == "ok"
        assert "Conexión a RDS exitosa" in result["message"]
        
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_with("SELECT 1 as test")
        mock_conn.close.assert_called_once()

def test_test_connection_missing_vars():
    """Test de test_connection con variables faltantes"""
    with patch.dict(os.environ, {}, clear=True):
        result = test_connection()
        
        assert result["status"] == "error"
        assert "Variables faltantes" in result["message"]

@patch('app.pymysql.connect')
def test_test_connection_db_error(mock_connect):
    """Test de test_connection con error de DB"""
    mock_connect.side_effect = Exception("Connection failed")
    
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-pass',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        result = test_connection()
        
        assert result["status"] == "error"
        assert "Connection failed" in result["message"]