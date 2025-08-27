# src/db.py
"""
Capa de persistencia usando SQLite para alojar oportunidades y tarifas.
"""
import os
import sqlite3
import pandas as pd

def init_db(db_path: str = 'data/analisis.sqlite') -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    return con

def persist_tables(con: sqlite3.Connection,
                   df_opp: pd.DataFrame,
                   df_tariffs: pd.DataFrame | None = None) -> None:
    # Oportunidades
    df_opp.to_sql('opportunities', con, if_exists='replace', index=False)
    # Tarifas (opcional)
    if df_tariffs is not None:
        df_tariffs.to_sql('tariffs', con, if_exists='replace', index=False)

def query_to_df(con: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, con)
