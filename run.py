# run.py
import os
import sys
import re
import subprocess

# ---------- Utilidades ----------
def info(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def err(msg):  print(f"🔴 {msg}")

def has_tk():
    try:
        import tkinter  # noqa: F401
        return True
    except Exception:
        return False

def choose_csv_file():
    info("Selecciona el CSV de oportunidades…")
    path = None
    if has_tk():
        try:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            path = filedialog.askopenfilename(
                title="Selecciona el CSV de oportunidades",
                filetypes=[("CSV", "*.csv"), ("Todos", "*.*")]
            )
            root.destroy()
        except Exception as e:
            warn(f"No se pudo abrir el explorador gráfico ({e}).")
            path = None
    if not path:
        path = input("Ruta al CSV (arrástralo aquí o pega la ruta): ").strip().strip('"').strip("'")
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"No existe el archivo: {path}")
    return path

def ensure_mappings():
    # Acepta mapping/ o mappings/
    candidates_cities = [
        os.path.join("mapping", "cities_mapping.json"),
        os.path.join("mappings", "cities_mapping.json"),
    ]
    candidates_provs = [
        os.path.join("mapping", "providers_mapping.json"),
        os.path.join("mappings", "providers_mapping.json"),
    ]
    cities = next((p for p in candidates_cities if os.path.exists(p)), None)
    provs  = next((p for p in candidates_provs if os.path.exists(p)), None)
    if not cities or not provs:
        raise FileNotFoundError(
            "Faltan mapeos. Crea alguno de estos:\n"
            "  - mapping/cities_mapping.json  o  mappings/cities_mapping.json\n"
            "  - mapping/providers_mapping.json  o  mappings/providers_mapping.json"
        )
    return cities, provs

def run_subpy(script, args=None):
    args = args or []
    cmd = [sys.executable, script] + args
    info(f"Ejecutando: {' '.join(cmd)}")
    res = subprocess.run(cmd, check=True)
    return res.returncode == 0

# ---------- Filtro YYYY-MM..YYYY-MM ----------
RE_YYYY_MM = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

def ask_range():
    def ask(label):
        while True:
            m = input(label).strip()
            if RE_YYYY_MM.match(m):
                return m
            print("  Formato inválido. Usa YYYY-MM (ej. 2024-07).")
    m_from = ask("Mes inicial (YYYY-MM): ")
    m_to   = ask("Mes final   (YYYY-MM): ")
    if m_from > m_to:
        warn("Mes inicial > Mes final. Intercambiando…")
        m_from, m_to = m_to, m_from
    return m_from, m_to

def open_folder(path_to_file):
    try:
        folder = os.path.abspath(os.path.dirname(path_to_file))
        if sys.platform.startswith("win"):
            os.startfile(folder)  # type: ignore
        elif sys.platform == "darwin":
            subprocess.Popen(["open", folder])
        else:
            subprocess.Popen(["xdg-open", folder])
    except Exception as e:
        warn(f"No pude abrir la carpeta: {e}")

# ---------- Main ----------
def main():
    print("\n=== Analisis comercial — Iniciador ===\n")

    # 1) CSV
    csv_path = choose_csv_file()
    info(f"CSV seleccionado: {csv_path}")

    # 2) Mapeos
    cities_map, providers_map = ensure_mappings()
    info(f"Usando mapeos:\n  - {cities_map}\n  - {providers_map}")

    # 3) Rango de meses (siempre inicio-fin)
    from_m, to_m = ask_range()
    rango_args = ["--from", from_m, "--to", to_m]
    info(f"Rango aplicado: {from_m} .. {to_m}")

    # 4) Oportunidades -> JSON anidado
    out_nested = os.path.join("outputs", "opportunities_curated_nested.json")
    run_subpy("run_opps_sql.py", [csv_path])
    if not os.path.exists(out_nested):
        raise RuntimeError("No se generó outputs/opportunities_curated_nested.json")

    # 5) Análisis de tarifas por frontera (FILTRA aquí)
    out_analysis = os.path.join("outputs", "analisis_tarifas_por_frontera.json")
    run_subpy("run_tariff_analysis.py", [out_nested, cities_map, providers_map] + rango_args)
    if not os.path.exists(out_analysis):
        raise RuntimeError("No se generó outputs/analisis_tarifas_por_frontera.json")

    # 6) Resumen usando el MISMO rango
    out_summary = os.path.join("outputs", "resumen_oportunidades.json")
    run_subpy("run_summary.py", rango_args)
    if not os.path.exists(out_summary):
        raise RuntimeError("No se generó outputs/resumen_oportunidades.json")

    print("\n✅ Listo.")
    print(f"   Resumen: {os.path.abspath(out_summary)}")
    print("   (También se generó outputs/debug_tariff_lookup.json para auditoría)\n")
    open_folder(out_summary)

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        err(f"Falló una etapa: {e}")
        sys.exit(1)
    except Exception as e:
        err(str(e))
        sys.exit(1)
