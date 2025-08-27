# src/compute.py
"""
Funciones para unir datos de oportunidades con tarifas y calcular métricas comerciales.
"""
import pandas as pd


def merge_data(df_opp: pd.DataFrame, df_tariffs: pd.DataFrame, on_fields: dict) -> pd.DataFrame:
    """
    Une el DataFrame de oportunidades con el de tarifas usando campos especificados.

    on_fields: mapeo de columnas en df_opp a columnas en df_tariffs, ej:
        {'Calculadora Payback/Region': 'region',
         'Calculadora Payback/Nivel Tension': 'tension'}

    Retorna un nuevo DataFrame con columnas combinadas.
    """
    left_on = list(on_fields.keys())
    right_on = list(on_fields.values())

    df_merged = df_opp.merge(
        df_tariffs,
        how='left',
        left_on=left_on,
        right_on=right_on,
        suffixes=('_opp', '_tariff')
    )
    return df_merged


def calculate_revenue(df: pd.DataFrame, kwh_col: str, rate_col: str, output_col: str = 'ingreso_estimado') -> pd.DataFrame:
    """
    Añade al DataFrame la columna de ingreso estimado:
        ingreso_estimado = kWh_por_mes * tarifa_unitaria
    """
    if kwh_col not in df.columns or rate_col not in df.columns:
        raise KeyError(f"Columnas {kwh_col} o {rate_col} no encontradas en el DataFrame")
    df[output_col] = df[kwh_col] * df[rate_col]
    return df


def calculate_payback(df: pd.DataFrame, capex_col: str, opex_col: str, revenue_col: str, output_col: str = 'payback_meses') -> pd.DataFrame:
    """
    Calcula el payback en meses según:
        payback = (capex + opex) / ingreso_mensual
    """
    for col in [capex_col, opex_col, revenue_col]:
        if col not in df.columns:
            raise KeyError(f"Columna {col} no encontrada en el DataFrame")
    total_cost = df[capex_col] + df[opex_col]
    df[output_col] = total_cost / df[revenue_col]
    return df
