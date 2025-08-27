# src/scrload.py
"""
Script para cargar datos de oportunidades desde CSV local y obtener la tabla de tarifas vía Lambda.
Incluye vista previa preliminar de oportunidades y tarifas.
"""
import pandas as pd
import os
import sys
import requests

# Configuración fija del Lambda de tarifas
LAMBDA_URL = 'https://vdok732ielv7dwmia5lmxyd6je0frxpz.lambda-url.us-west-2.on.aws/'
TARIFF_RESOURCE_ID = 15480


def fetch_tariffs(lambda_url: str, resource_id: int) -> pd.DataFrame:
    """
    Llama al endpoint Lambda para obtener la URL del CSV de tarifas asociado al resource_id.
    Carga el CSV resultante en un DataFrame.
    """
    payload = {'resource_id': resource_id}
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}

    response = requests.post(lambda_url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Extraer URL de la respuesta
    if 'iframeUrl' in data:
        csv_url = data['iframeUrl']
    elif 'csv_url' in data:
        csv_url = data['csv_url']
    elif 'url' in data:
        csv_url = data['url']
    else:
        raise KeyError(f"No se encontró clave de URL en respuesta: {data}")

    # Cargar CSV de tarifas
    df_tariffs = pd.read_csv(csv_url)
    return df_tariffs


def load_opportunities(path: str) -> pd.DataFrame:
    """
    Carga un archivo CSV de oportunidades en un DataFrame de pandas.
    - Verifica existencia de columnas mínimas.
    - Limpia y convierte columnas numéricas.
    """
    path = path.strip().strip('"').strip("'")
    if not os.path.exists(path):
        raise FileNotFoundError(f"El archivo de oportunidades {path} no existe.")

    df = pd.read_csv(path)

    expected_columns = [
        'Oportunidad', 'Cliente',
        'Calculadora Payback/kWh Promedio / Mes',
        'Calculadora Payback/Region',
        'Costo Total Opex Oportunity',
        'Costo Total Capex Oportunity',
        'Tarifa B',
        'Calculadora Payback/Modem & Medidor',
        'Calculadora Payback/Ciiu'
    ]
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas obligatorias en oportunidades: {missing}")

    # Limpieza y conversión de columnas numéricas
    numeric_cols = [
        'Calculadora Payback/kWh Promedio / Mes',
        'Calculadora Payback/Modem & Medidor',
        'Costo Total Opex Oportunity',
        'Costo Total Capex Oportunity',
        'Tarifa B'
    ]
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '').str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


def get_csv_path() -> str:
    """
    Obtiene la ruta al CSV de oportunidades: argumento en línea de comandos o input.
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    return input("Ingrese la ruta al CSV de oportunidades: ")


if __name__ == '__main__':
    path = get_csv_path()

    # 1) Vista previa de oportunidades
    df_opp = load_opportunities(path)
    pd.set_option('display.max_columns', None)
    print("--- Vista previa de Oportunidades ---")
    print(df_opp.head(10))
    print(f"Total filas de oportunidades: {len(df_opp)}")

    # 2) Vista previa de tarifas
    try:
        df_tariffs = fetch_tariffs(LAMBDA_URL, TARIFF_RESOURCE_ID)
        print("\n--- Vista previa de Tarifas ---")
        print(df_tariffs.head(10))
        print(f"Total filas de tarifas: {len(df_tariffs)}")
    except Exception as e:
        print(f"Error al cargar tarifas: {e}")