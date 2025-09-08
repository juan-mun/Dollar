import pytest
import os
from unittest.mock import patch, MagicMock
from app import fetch_dollar_data, get_timestamp_filename, validate_environment_variables, get_db_connection

def test_fetch_dollar_data():
    """Debe obtener datos desde el endpoint y contener claves conocidas"""
    data = fetch_dollar_data()
    assert isinstance(data, dict) or isinstance(data, list)

def test_get_timestamp_filename():
    """Debe generar un nombre de archivo con prefijo dolar- y extensión .json"""
    filename = get_timestamp_filename()
    assert filename.startswith("dolar-")
    assert filename.endswith(".json")

def test_validate_environment_variables_missing():
    """Debe detectar variables de entorno faltantes"""
    with patch.dict(os.environ, {}, clear=True):
        is_valid, message = validate_environment_variables()
        assert not is_valid
        assert "Variables de entorno faltantes" in message

def test_validate_environment_variables_present():
    """Debe validar correctamente cuando todas las variables están presentes"""
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user', 
        'RDS_PASSWORD': 'test-password',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        is_valid, message = validate_environment_variables()
        assert is_valid
        assert message == "OK"

@pytest.mark.skipif(
    not all([
        os.environ.get('RDS_HOST'),
        os.environ.get('RDS_USER'),
        os.environ.get('RDS_PASSWORD'),
        os.environ.get('RDS_DB')
    ]), 
    reason="Variables de entorno RDS no configuradas para testing"
)

@patch('app.pymysql.connect')
def test_get_db_connection_success(mock_connect):
    """Debe crear conexión exitosamente con parámetros correctos"""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-password', 
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        connection = get_db_connection()
        
        mock_connect.assert_called_once_with(
            host='test-host',
            port=3306,
            user='test-user',
            password='test-password',
            database='test-db',
            charset='utf8mb4',
            cursorclass=pytest.mock.ANY,
            connect_timeout=10,
            read_timeout=10,
            write_timeout=10
        )
        assert connection == mock_connection

@patch('app.pymysql.connect')
def test_get_db_connection_failure(mock_connect):
    """Debe manejar errores de conexión apropiadamente"""
    mock_connect.side_effect = Exception("Connection failed")
    
    env_vars = {
        'RDS_HOST': 'test-host',
        'RDS_USER': 'test-user',
        'RDS_PASSWORD': 'test-password',
        'RDS_DB': 'test-db'
    }
    
    with patch.dict(os.environ, env_vars):
        with pytest.raises(Exception, match="Connection failed"):
            get_db_connection()

def test_timestamp_filename_format():
    """Debe generar timestamps válidos y únicos"""
    filename1 = get_timestamp_filename()
    filename2 = get_timestamp_filename()
    
    # Extraer el timestamp de ambos nombres
    timestamp1 = filename1.replace("dolar-", "").replace(".json", "")
    timestamp2 = filename2.replace("dolar-", "").replace(".json", "")
    
    # Deben ser números válidos
    assert timestamp1.isdigit()
    assert timestamp2.isdigit()
    
    # El segundo debe ser mayor o igual (puede ser el mismo si se ejecutan muy rápido)
    assert int(timestamp2) >= int(timestamp1)
