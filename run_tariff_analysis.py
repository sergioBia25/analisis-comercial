# run_tariff_analysis.py — Nivel compuesto + fallback a simple + filtro de meses
"""
Uso:
  python run_tariff_analysis.py [path_nested_json] [cities_mapping.json] [providers_mapping.json]
                                --from YYYY-MM --to YYYY-MM
"""
import os
import sys
import json
import argparse
import unicodedata
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

# --------- Carga de tarifas ----------
try:
    from src.srcload import fetch_tariffs, LAMBDA_URL, TARIFF_RESOURCE_ID
except Exception:
    from srcload import fetch_tariffs, LAMBDA_URL, TARIFF_RESOURCE_ID

PROVIDER_BIA = "BIA ENERGY"

# --------- Helpers ---------
def norm_text(s: Any) -> str:
    if s is None:
        return ''
    s = str(s).strip().upper()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = ' '.join(s.split())
    return s

def clean_space(s: Any) -> str:
    return ' '.join(str(s or '').split())

def canonical_simple(n: Any) -> Optional[str]:
    """Devuelve 'NIVEL 1/2/3' si encuentra dígito, si no None."""
    nrm = norm_text(n)
    if '1' in nrm: return 'NIVEL 1'
    if '2' in nrm: return 'NIVEL 2'
    if '3' in nrm: return 'NIVEL 3'
    for ch in nrm:
        if ch.isdigit() and ch in '123':
            return f'NIVEL {ch}'
    return None

def canonical_comp_from_tokens(txt: str) -> Optional[str]:
    """
    Intenta construir nivel_{d}_{kind} a partir de texto (tarifas o calculadora).
    Acepta variantes: NIVEL 1 OPERADOR, NIVEL_1_USER, BT1 USUARIO, etc.
    """
    t = norm_text(txt).replace('-', ' ').replace('/', ' ').replace('.', ' ').replace('_', ' ')
    toks = t.split()
    # detectar dígito
    dig = None
    for d in ('1','2','3'):
        if d in toks or f'NIVEL{d}' in toks or f'BT{d}' in toks or d in ''.join(toks):
            dig = d
            break
    if not dig:
        # también si viene como 'NIVEL 1 USER' -> buscar 'NIVEL' y el token siguiente numérico
        for i, w in enumerate(toks[:-1]):
            if 'NIVEL' in w and toks[i+1] in ('1','2','3'):
                dig = toks[i+1]; break
    if not dig:
        return None

    # detectar tipo
    kind = None
    joined = ' '.join(toks)
    if 'OPERADOR' in joined or 'OPERATOR' in joined:
        kind = 'operator'
    elif 'USUARIO' in joined or 'USER' in joined:
        kind = 'user'
    elif 'COMPARTID' in joined or 'SHARED' in joined:
        kind = 'shared'

    return f"nivel_{dig}_{kind}" if kind else None

def coalesce(*vals):
    for v in vals:
        if v not in (None, ''):
            return v
    return None

def _load_json(path: str) -> Optional[dict]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def load_mapping_json(path_or_none: Optional[str], fname_default: str) -> Dict[str, str]:
    candidates: List[str] = []
    if path_or_none:
        candidates.append(path_or_none)
    candidates.extend([
        os.path.join('mapping',  fname_default),
        os.path.join('mappings', fname_default),
        fname_default,
        os.path.join('outputs', fname_default),
        os.path.join('/mnt/data', fname_default),
    ])
    raw = None
    for p in candidates:
        if os.path.exists(p):
            raw = _load_json(p)
            if raw is not None:
                break
    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            k_n = norm_text(k); v_n = norm_text(v)
            if k_n:
                out[k_n] = v_n
    elif isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict) and 'key' in row and 'value' in row:
                k_n = norm_text(row['key']); v_n = norm_text(row['value'])
                if k_n:
                    out[k_n] = v_n
    return out

# --------- Tokens / fallback provider ---------
def provider_tokens(name: str) -> List[str]:
    s = norm_text(name)
    s = s.replace('.', ' ').replace('/', ' ').replace('-', ' ')
    stop = {'S', 'A', 'SA', 'SAS', 'ESP', 'E', 'P', 'ES', 'EP'}
    toks = [t for t in s.split() if t and t not in stop]
    return toks

def best_token_match(target: str, candidates: List[str]) -> Tuple[Optional[str], float]:
    tgt = set(provider_tokens(target))
    if not tgt:
        return None, 0.0
    best, best_score = None, 0.0
    for c in candidates:
        cand = set(provider_tokens(c))
        if not cand:
            continue
        inter = len(tgt & cand)
        union = len(tgt | cand)
        score = inter / union if union else 0.0
        if score > best_score:
            best, best_score = c, score
    return best, best_score

def r2(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)

# --------- Tarifas ---------
def prep_tariffs(df: pd.DataFrame, canonical_provider_targets: List[str]) -> pd.DataFrame:
    expected = {'mes', 'provider', 'city', 'nivel_de_tension', 'tarifa'}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f'Faltan columnas en tarifas: {missing}. Esperadas: {expected}')
    out = df.copy()
    out['mes_key']        = out['mes'].astype(str).str[:7]
    out['city_n']         = out['city'].map(norm_text)
    out['provider_raw_n'] = out['provider'].map(norm_text)
    out['tarifa']         = pd.to_numeric(out['tarifa'], errors='coerce')

    # Normalizar nivel de tarifas (intenta compuesto; si no, deja simple)
    out['nivel_raw']      = out['nivel_de_tension'].map(clean_space)
    out['nivel_comp']     = out['nivel_raw'].map(canonical_comp_from_tokens)
    out['nivel_simp']     = out['nivel_raw'].map(canonical_simple)

    # Canonizar provider (lado tarifas) a targets del mapping
    targets = sorted(set(norm_text(t) for t in canonical_provider_targets if t), key=len, reverse=True)
    def canon_provider_tar_side(p: str) -> str:
        if p in targets: return p
        best, score = best_token_match(p, targets)
        return best if best and score >= 0.45 else p
    out['provider_n'] = out['provider_raw_n'].map(canon_provider_tar_side)

    out = out.dropna(subset=['tarifa'])
    return out[['mes_key','city_n','provider_n','tarifa','nivel_comp','nivel_simp']]

def build_index_comp(df: pd.DataFrame) -> Dict[Tuple[str,str,str,str], float]:
    """índice por llave compuesta: (mes, city, nivel_comp, provider)"""
    idx: Dict[Tuple[str,str,str,str], float] = {}
    sub = df.dropna(subset=['nivel_comp'])
    for _, r in sub.iterrows():
        idx[(r['mes_key'], r['city_n'], r['nivel_comp'], r['provider_n'])] = float(r['tarifa'])
    return idx

def build_index_simple(df: pd.DataFrame) -> Dict[Tuple[str,str,str,str], float]:
    """índice por llave simple: (mes, city, NIVEL X, provider)"""
    idx: Dict[Tuple[str,str,str,str], float] = {}
    sub = df.dropna(subset=['nivel_simp'])
    for _, r in sub.iterrows():
        idx[(r['mes_key'], r['city_n'], r['nivel_simp'], r['provider_n'])] = float(r['tarifa'])
    return idx

def build_bucket(df: pd.DataFrame, level_col: str) -> Dict[Tuple[str,str,str], List[str]]:
    """
    (mes, city, level_col) -> lista de providers disponibles.
    level_col: 'nivel_comp' o 'nivel_simp'
    """
    bucket: Dict[Tuple[str,str,str], List[str]] = {}
    sub = df.dropna(subset=[level_col])
    for _, r in sub.iterrows():
        k = (r['mes_key'], r['city_n'], r[level_col])
        bucket.setdefault(k, set()).add(r['provider_n'])
    return {k: sorted(list(v)) for k, v in bucket.items()}

# --------- Oportunidades ---------
def load_opps_nested(path_json: str) -> List[Dict[str, Any]]:
    with open(path_json, 'r', encoding='utf-8') as f:
        return json.load(f)

# --------- CLI / rango ---------
def parse_args():
    p = argparse.ArgumentParser(description="Análisis por frontera (nivel compuesto + rango de meses).")
    p.add_argument("nested_json", nargs="?", default=os.path.join("outputs","opportunities_curated_nested.json"))
    p.add_argument("cities_map",  nargs="?", default=None)
    p.add_argument("providers_map", nargs="?", default=None)
    p.add_argument("--from", dest="from_month", required=True, help="Mes inicial (YYYY-MM)")
    p.add_argument("--to",   dest="to_month",   required=True, help="Mes final (YYYY-MM)")
    return p.parse_args()

# --------- MAIN ---------
def main():
    args = parse_args()

    city_map = load_mapping_json(args.cities_map, 'cities_mapping.json')
    prov_map = load_mapping_json(args.providers_map, 'providers_mapping.json')

    df_tariffs = fetch_tariffs(LAMBDA_URL, TARIFF_RESOURCE_ID)
    df_tar = prep_tariffs(df_tariffs, canonical_provider_targets=list(prov_map.values()) + [PROVIDER_BIA])

    # FILTRO DE RANGO AQUÍ
    start = args.from_month.strip()[:7]
    end   = args.to_month.strip()[:7]
    df_tar = df_tar[(df_tar['mes_key'] >= start) & (df_tar['mes_key'] <= end)].copy()

    # Índices y buckets (compuesto + simple)
    index_comp   = build_index_comp(df_tar)
    index_simple = build_index_simple(df_tar)
    bucket_comp  = build_bucket(df_tar, 'nivel_comp')
    bucket_simp  = build_bucket(df_tar, 'nivel_simp')

    meses_disponibles = sorted(df_tar['mes_key'].unique())
    opps = load_opps_nested(args.nested_json)

    salida: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []

    for reg in opps:
        oportunidad = reg.get('oportunidad')
        cliente = reg.get('cliente')
        fronteras = reg.get('fronteras', [])

        filas_fronteras: List[Dict[str, Any]] = []

        for f in fronteras:
            city_raw   = coalesce(f.get('city'), f.get('region'), f.get('ciudad'), f.get('market'))
            nivel_raw  = f.get('nivel_de_tension')  # <- ya viene compuesto (nivel_1_user), pero soportamos simple
            consumo_rw = f.get('consumo_kwh') or f.get('consumo') or f.get('kwh_mes')
            prov_raw   = coalesce(
                f.get('provider_actual'), f.get('provider'),
                f.get('comercializador actual'), f.get('comercializador_actual'),
                f.get('comercializador')
            )

            # Ciudad y provider canon
            city_calc_n   = norm_text(city_raw)
            city_tarifa_n = city_map.get(city_calc_n, city_calc_n)
            prov_calc_n   = norm_text(prov_raw)
            prov_tarifa_n = prov_map.get(prov_calc_n, prov_calc_n)
            bia_tarifa_n  = prov_map.get(norm_text(PROVIDER_BIA), norm_text(PROVIDER_BIA))

            # Nivel de la frontera: intentar extraer compuesto y simple
            nivel_comp_f  = canonical_comp_from_tokens(nivel_raw) or canonical_comp_from_tokens(norm_text(nivel_raw))
            nivel_simp_f  = canonical_simple(nivel_raw)

            try:
                consumo = float(consumo_rw) if consumo_rw not in (None, '') else None
            except Exception:
                consumo = None

            mensual: List[Dict[str, Any]] = []
            for mes in meses_disponibles:
                # ------- BIA -------
                # 1) compuesto
                t_bia = index_comp.get((mes, city_tarifa_n, nivel_comp_f, bia_tarifa_n)) if nivel_comp_f else None
                # 2) simple (fallback)
                if t_bia is None and nivel_simp_f is not None:
                    t_bia = index_simple.get((mes, city_tarifa_n, nivel_simp_f, bia_tarifa_n))

                # ------- Actual -------
                t_act = index_comp.get((mes, city_tarifa_n, nivel_comp_f, prov_tarifa_n)) if nivel_comp_f else None
                used_provider = prov_tarifa_n
                used_level_variant = 'compuesto' if t_act is not None else None
                used_fallback = False
                fallback_score = None

                # Fallback a simple si no hay compuesto
                if t_act is None and nivel_simp_f is not None:
                    t_act = index_simple.get((mes, city_tarifa_n, nivel_simp_f, prov_tarifa_n))
                    used_level_variant = 'simple' if t_act is not None else None

                # Fallback de provider por tokens dentro de la llave adecuada
                if t_act is None:
                    # Elegir bucket según nivel disponible
                    if used_level_variant == 'compuesto' or (used_level_variant is None and nivel_comp_f):
                        providers_here = bucket_comp.get((mes, city_tarifa_n, nivel_comp_f), [])
                    else:
                        providers_here = bucket_simp.get((mes, city_tarifa_n, nivel_simp_f), [])

                    if providers_here:
                        cand, score = best_token_match(used_provider, providers_here)
                        if cand is not None and score >= 0.45:
                            if used_level_variant == 'compuesto' or (used_level_variant is None and nivel_comp_f):
                                t_act = index_comp.get((mes, city_tarifa_n, nivel_comp_f, cand))
                            else:
                                t_act = index_simple.get((mes, city_tarifa_n, nivel_simp_f, cand))
                            used_provider = cand
                            used_fallback = True
                            fallback_score = score

                # Costos y métricas
                costo_bia = (consumo * t_bia) if (consumo is not None and t_bia is not None) else None
                costo_act = (consumo * t_act) if (consumo is not None and t_act is not None) else None
                delta_unit = (t_act - t_bia) if (t_act is not None and t_bia is not None) else None
                ahorro_mensual = (costo_act - costo_bia) if (costo_act is not None and costo_bia is not None) else None

                mensual.append({
                    'mes': mes,
                    'tarifa_bia': r2(t_bia),
                    'tarifa_actual': r2(t_act),
                    'consumo_kwh': consumo,
                    'costo_bia': r2(costo_bia),
                    'costo_actual': r2(costo_act),
                    'delta_unit': r2(delta_unit),
                    'ahorro_mensual_estimado': r2(ahorro_mensual)
                })

                debug_rows.append({
                    "oportunidad": oportunidad,
                    "frontier_name": f.get('frontier_name'),
                    "mes": mes,
                    "city_tarifas_usada": city_tarifa_n,
                    "nivel_comp_fr": nivel_comp_f,
                    "nivel_simple_fr": nivel_simp_f,
                    "nivel_variant_usado": used_level_variant,
                    "provider_calc": prov_calc_n,
                    "provider_tarifas_mapeado": prov_tarifa_n,
                    "provider_usado_para_actual": used_provider if t_act is not None else None,
                    "fallback_usado": used_fallback,
                    "fallback_score": fallback_score,
                    "encontro_bia": t_bia is not None,
                    "encontro_actual": t_act is not None
                })

            filas_fronteras.append({
                'frontier_name': f.get('frontier_name'),
                'city_calculadora': city_calc_n,
                'city_tarifas_usada': city_tarifa_n,
                'nivel_de_tension': nivel_comp_f or nivel_simp_f,  # para visibilidad en salida
                'provider_actual_calc_norm': prov_calc_n,
                'provider_actual_tarifas_norm': prov_tarifa_n,
                'analisis_mensual': mensual
            })

        salida.append({
            'oportunidad': oportunidad,
            'cliente': cliente,
            'fronteras': filas_fronteras
        })

    os.makedirs('outputs', exist_ok=True)
    out_file = os.path.join('outputs','analisis_tarifas_por_frontera.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)

    debug_file = os.path.join('outputs','debug_tariff_lookup.json')
    with open(debug_file, 'w', encoding='utf-8') as f:
        json.dump(debug_rows, f, ensure_ascii=False, indent=2)

    print(f'✅ Análisis: {out_file}')
    print(f'🪪 Debug:    {debug_file}')

if __name__ == '__main__':
    main()
