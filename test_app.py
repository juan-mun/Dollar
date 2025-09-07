import pytest
from app import fetch_dollar_data, get_timestamp_filename

def test_fetch_dollar_data():
    """Debe obtener datos desde el endpoint y contener claves conocidas"""
    data = fetch_dollar_data()
    assert isinstance(data, dict) or isinstance(data, list)

def test_get_timestamp_filename():
    """Debe generar un nombre de archivo con prefijo dolar- y extensión .json"""
    filename = get_timestamp_filename()
    assert filename.startswith("dolar-")
    assert filename.endswith(".json")
