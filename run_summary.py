# run_summary.py
"""
Genera un resumen por oportunidad combinando:
- outputs/opportunities_curated_nested.json (cabeceras y totales)
- outputs/analisis_tarifas_por_frontera.json (costos por frontera y mes)

Salida:
- outputs/resumen_oportunidades.json

Estructura por oportunidad:
{
  "oportunidad": ...,
  "cliente": ...,
  "inversion_cliente": ...,
  "tarifa_b": ...,
  "opex": ...,
  "capex": ...,
  "consumo_total": ...,
  "total_renting": ...,
  "ciudad": ...,
  "Costo actual total por mes": { "YYYY-MM": float|null, ... },
  "Costo Bia total por mes":   { "YYYY-MM": float|null, ... },
  "Costo total actual": float,
  "Costo total Bia":   float,
  "Ahorro Bia":        float
}
"""

import os
import json
from typing import Any, Dict, List, Optional
from collections import defaultdict

def r2(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)

def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main(
    opps_path: str = os.path.join("outputs", "opportunities_curated_nested.json"),
    analisis_path: str = os.path.join("outputs", "analisis_tarifas_por_frontera.json"),
    out_path: str = os.path.join("outputs", "resumen_oportunidades.json")
):
    # 1) Cargar cabeceras/valores únicos por oportunidad
    opps = load_json(opps_path)  # lista
    head_by_opp: Dict[str, Dict[str, Any]] = {}
    for reg in opps:
        opp = reg.get("oportunidad")
        if not opp:
            continue
        head_by_opp[opp] = {
            "oportunidad": opp,
            "cliente": reg.get("cliente"),
            "inversion_cliente": reg.get("inversion_cliente"),
            "tarifa_b": reg.get("tarifa_b"),
            "opex": reg.get("opex"),
            "capex": reg.get("capex"),
            "consumo_total": reg.get("consumo_total"),
            "total_renting": reg.get("total_renting"),
            "ciudad": reg.get("ciudad"),
        }

    # 2) Cargar análisis por frontera y agregar por oportunidad/mes
    analisis = load_json(analisis_path)  # lista
    # Estructuras acumuladoras
    monthly_actual_by_opp: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    monthly_bia_by_opp: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    months_seen_by_opp: Dict[str, set] = defaultdict(set)

    for reg in analisis:
        opp = reg.get("oportunidad")
        for fr in reg.get("fronteras", []):
            for row in fr.get("analisis_mensual", []):
                mes = row.get("mes")
                months_seen_by_opp[opp].add(mes)
                ca = row.get("costo_actual")
                cb = row.get("costo_bia")
                # Sumar solo si hay número
                if isinstance(ca, (int, float)):
                    monthly_actual_by_opp[opp][mes] += float(ca)
                if isinstance(cb, (int, float)):
                    monthly_bia_by_opp[opp][mes] += float(cb)

    # 3) Construir salida con redondeo a 2 decimales
    out: List[Dict[str, Any]] = []
    for opp in sorted(months_seen_by_opp.keys() | head_by_opp.keys()):
        head = head_by_opp.get(opp, {"oportunidad": opp})
        meses = sorted(months_seen_by_opp.get(opp, set()))

        # Mapas por mes (si un mes no tiene dato numérico, lo dejamos en None)
        costo_act_total_mes: Dict[str, Optional[float]] = {}
        costo_bia_total_mes: Dict[str, Optional[float]] = {}
        for m in meses:
            ca_val = monthly_actual_by_opp[opp].get(m, None)
            cb_val = monthly_bia_by_opp[opp].get(m, None)
            # Si quedaron en 0.0 porque no había datos, distinguimos:
            # - Si nunca se sumó nada, dejamos None (no confundir con costo cero real).
            if m not in monthly_actual_by_opp[opp]:
                ca_out = None
            else:
                ca_out = ca_val
            if m not in monthly_bia_by_opp[opp]:
                cb_out = None
            else:
                cb_out = cb_val
            costo_act_total_mes[m] = r2(ca_out) if ca_out is not None else None
            costo_bia_total_mes[m] = r2(cb_out) if cb_out is not None else None

        # Totales anuales (sumando solo valores numéricos)
        total_actual = sum(v for v in monthly_actual_by_opp[opp].values()) if monthly_actual_by_opp.get(opp) else 0.0
        total_bia    = sum(v for v in monthly_bia_by_opp[opp].values()) if monthly_bia_by_opp.get(opp) else 0.0
        ahorro_bia   = total_actual - total_bia

        out.append({
            **head,
            "Costo actual total por mes": costo_act_total_mes,
            "Costo Bia total por mes": costo_bia_total_mes,
            "Costo total actual": r2(total_actual),
            "Costo total Bia": r2(total_bia),
            "Ahorro Bia": r2(ahorro_bia)
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"✅ Resumen listo: {out_path}")

if __name__ == "__main__":
    main()
