# src/export.py
"""
Funciones para exportar resultados a archivos o formatos requeridos.
"""
import json
import os
import pandas as pd


def export_to_json(df: pd.DataFrame, output_path: str,
                   orient: str = 'records', indent: int = 2) -> None:
    """
    Exporta un DataFrame a un archivo JSON.

    Parámetros:
    - df: DataFrame a exportar.
    - output_path: Ruta del archivo JSON de salida.
    - orient: Formato de to_json (por defecto 'records').
    - indent: Espacios para sangría en el JSON.
    """
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Convertir DataFrame a lista de dicts
    data = json.loads(df.to_json(orient=orient, force_ascii=False))

    # Guardar JSON en archivo
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def export_to_console_json(df: pd.DataFrame,
                           orient: str = 'records', indent: int = None) -> None:
    """
    Imprime el DataFrame en formato JSON en consola.

    Parámetros:
    - df: DataFrame a imprimir.
    - orient: Formato de to_json (por defecto 'records').
    - indent: Sangría para la salida (None = sin formateo).
    """
    print(df.to_json(orient=orient, force_ascii=False, indent=indent))
