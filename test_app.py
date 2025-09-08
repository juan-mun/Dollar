import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import app  # tu archivo con el código


# ========= TESTS PARA obtener_y_guardar_dolar =========

@patch("app.requests.get")
@patch("app.s3.put_object")
def test_obtener_y_guardar_dolar_exito(mock_put, mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{"dato": 1}, {"dato": 2}]
    
    result = app.obtener_y_guardar_dolar()
    
    assert result["status"] == "ok"
    assert result["file"].startswith("dolar-")
    mock_put.assert_called_once()
    mock_get.assert_called_once()


@patch("app.requests.get", side_effect=Exception("fallo API"))
def test_obtener_y_guardar_dolar_error(mock_get):
    result = app.obtener_y_guardar_dolar()
    assert result["status"] == "error"
    assert "fallo API" in result["message"]


# ========= TESTS PARA f (Lambda principal) =========

@patch("app.obtener_y_guardar_dolar", return_value={"status": "ok"})
def test_f_ejecucion_exitosa(mock_func):
    event = {}
    context = {}
    result = app.f(event, context)
    assert result["status"] == "ok"
    mock_func.assert_called_once()


def test_f_fuera_de_rango():
    event = {}
    context = {}
    # Mock de fecha futura (posterior al límite)
    with patch("app.datetime") as mock_dt:
        mock_dt.utcnow.return_value = datetime(2027, 1, 1)
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)  # para strftime
        result = app.f(event, context)
    assert result["status"] == "skipped"


# ========= TESTS PARA process_file =========

@patch("app.boto3.client")
def test_process_file_exito(mock_boto):
    # Mock S3 get_object
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3
    data = [
        [1672531200000, "4800.50"],
        [1672617600000, "4820.75"]
    ]
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(data).encode("utf-8"))
    }
    
    # Mock conexión DB
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 2  # Simular 2 filas insertadas
    
    event = {
        "Records": [{
            "s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "archivo.json"}}
        }]
    }
    context = type("obj", (), {"db_conn": mock_conn})()
    
    result = app.process_file(event, context)
    
    # Validaciones
    assert result["status"] == "ok"
    assert result["rows_processed"] == 2
    assert result["rows_inserted"] == 2
    
    # Validar que se creó la tabla
    args, _ = mock_cursor.execute.call_args_list[0]
    assert "CREATE TABLE IF NOT EXISTS dolar" in args[0]
    
    # Validar que se hizo el insert
    mock_cursor.executemany.assert_called_once()
def test_process_file_evento_invalido():
    event = {"Records": []}
    context = type("obj", (), {"db_conn": MagicMock()})()
    result = app.process_file(event, context)
    assert result["status"] == "error"
    assert "Evento S3 inválido" in result["message"]


@patch("app.boto3.client")
def test_process_file_json_invalido(mock_boto):
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"{}")  # No es lista
    }
    event = {
        "Records": [{
            "s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "archivo.json"}}
        }]
    }
    context = type("obj", (), {"db_conn": MagicMock()})()
    result = app.process_file(event, context)
    assert result["status"] == "error"
    assert "No contiene una lista" or "No se pudieron procesar" in result["message"]
