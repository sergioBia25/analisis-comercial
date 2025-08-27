# run_opps_sql.py
"""
PARTE 1 (anidado, listo para cruce con tarifas) — SOLO valores únicos y sumas a nivel oportunidad.
- Totales y campos únicos en la cabecera de cada oportunidad
- Detalle por frontera en la clave "fronteras"
- Exporta: "nivel_de_tension" como nivel_{1|2|3}_{user|operator|shared}
Salida: outputs/opportunities_curated_nested.json
"""
import sys
import os
import json
import unicodedata
import pandas as pd
from src.srcload import load_opportunities

def _normalize_path(path: str) -> str:
    return path.strip().strip('"').strip("'")

def _clean_str(x):
    s = str(x) if x is not None else ''
    return ' '.join(s.split())

def _norm_text(s):
    if s is None:
        return ''
    s = str(s).strip().upper()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = ' '.join(s.split())
    return s

def _nivel_simple(nivel_raw):
    n = _norm_text(nivel_raw)
    if '1' in n: return 'NIVEL 1'
    if '2' in n: return 'NIVEL 2'
    if '3' in n: return 'NIVEL 3'
    for ch in n:
        if ch.isdigit() and ch in '123':
            return f'NIVEL {ch}'
    return None

def _nivel_compuesto(propiedad_raw, nivel_raw):
    """
    Construye nivel_{1|2|3}_{operator|user|shared}
    """
    p = _norm_text(propiedad_raw)
    n = _nivel_simple(nivel_raw)  # -> 'NIVEL X' o None
    if not n:
        return None
    dig = n[-1]  # '1'/'2'/'3'

    # tipo por propiedad
    if 'USUARIO' in p:
        kind = 'user'
    elif 'OPERADOR' in p:
        kind = 'operator'
    elif 'COMPARTID' in p or 'SHARED' in p:
        kind = 'shared'
    else:
        return None

    return f'nivel_{dig}_{kind}'

def _first_nonnull(series: pd.Series):
    for x in series:
        if pd.notna(x) and str(x).strip() != '':
            return x
    return None

def main(raw_csv_path: str):
    csv_path = _normalize_path(raw_csv_path)

    # 1) Cargar oportunidades
    df = load_opportunities(csv_path).copy()

    # 1.1) Rellenar hacia adelante columnas de identidad
    id_cols = [
        'Oportunidad', 'Cliente', 'Calculadora Payback/Número de Cuenta',
        'Calculadora Payback/Ciudad', 'Calculadora Payback/Ciiu',
        'Calculadora Payback/Region', 'Calculadora Payback/Nivel Tension',
        'Calculadora Payback/Propiedad de Equipos',
        'Calculadora Payback/Operador de Red',
        'Calculadora Payback/Comercializador Actual',
        'Tarifa B', 'Costo Total Opex Oportunity', 'Costo Total Capex Oportunity',
    ]
    for c in id_cols:
        if c in df.columns:
            df[c] = df[c].ffill()

    # 2) Construir salida anidada por Oportunidad
    out = []
    for opp, g in df.groupby('Oportunidad', dropna=False):
        consumo_total = float(g['Calculadora Payback/kWh Promedio / Mes'].sum())
        total_renting = float(g['Calculadora Payback/Modem & Medidor'].sum())

        frontier_name = _first_nonnull(g['Calculadora Payback/Número de Cuenta'])
        cliente   = _clean_str(_first_nonnull(g['Cliente']))
        tarifa_b  = _first_nonnull(g['Tarifa B'])
        opex      = _first_nonnull(g['Costo Total Opex Oportunity'])
        capex     = _first_nonnull(g['Costo Total Capex Oportunity'])
        ciudad_u  = _clean_str(_first_nonnull(g['Calculadora Payback/Ciudad']))
        inversion_cliente = float(capex or 0) + float(opex or 0)

        fronteras = []
        for _, r in g.iterrows():
            prop_act  = _clean_str(r.get('Calculadora Payback/Propiedad de Equipos', '') or '')
            nivel_in  = _clean_str(r.get('Calculadora Payback/Nivel Tension', '') or '')
            nivel_cmp = _nivel_compuesto(prop_act, nivel_in)      # <- compuesto
            if nivel_cmp is None:
                # respaldo a simple si no se pudo componer
                ns = _nivel_simple(nivel_in)
                nivel_cmp = ns.lower().replace(' ', '_') if ns else None  # ej. 'nivel_1'

            fronteras.append({
                "frontier_name":  _clean_str(r.get('Calculadora Payback/Número de Cuenta', '') or ''),
                "consumo": float(r.get('Calculadora Payback/kWh Promedio / Mes', 0) or 0),
                "renting": float(r.get('Calculadora Payback/Modem & Medidor', 0) or 0),
                "city":    _clean_str(r.get('Calculadora Payback/Region', '') or ''),   # ↔ tarifas.city
                "ciudad":  _clean_str(r.get('Calculadora Payback/Ciudad', '') or ''),
                "nivel_de_tension": nivel_cmp,   # <- SOLO este campo
                "operador_de_red": _clean_str(r.get('Calculadora Payback/Operador de Red', '') or ''),
                "provider_actual": _clean_str(r.get('Calculadora Payback/Comercializador Actual', '') or ''),
                "provider":        _clean_str(r.get('Calculadora Payback/Comercializador Actual', '') or '')
            })

        registro = {
            "oportunidad": opp,
            "cliente": cliente,
            "inversion_cliente": float(inversion_cliente),
            "tarifa_b": float(tarifa_b or 0),
            "opex": float(opex or 0),
            "capex": float(capex or 0),
            "consumo_total": consumo_total,
            "total_renting": total_renting,
            "ciudad": ciudad_u,
            "fronteras": fronteras,
            "provider_objetivo": "BIA ENERGY"
        }
        out.append(registro)

    os.makedirs('outputs', exist_ok=True)
    with open(os.path.join('outputs', 'opportunities_curated_nested.json'), 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("✅ Parte 1 lista: outputs/opportunities_curated_nested.json — 'nivel_de_tension' compuesto.")

if __name__ == '__main__':
    raw = sys.argv[1] if len(sys.argv) > 1 else input('Ruta al CSV de oportunidades: ')
    main(raw)
