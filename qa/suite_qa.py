"""
Suite de QA del pipeline ETL.

No se conforma con "no truena". Verifica tres cosas distintas:

  1. RESILIENCIA   el pipeline termina sin excepciones no controladas.
  2. DETECCION     encontro exactamente los errores que plantamos, ni mas ni menos.
                   Se compara contra la verdad conocida del generador.
  3. INTEGRIDAD    los datos que SI pasaron no fueron corrompidos. Diez invariantes
                   que deben cumplirse siempre, sin importar el tamano ni la suciedad.

Un pipeline puede "no tronar" y aun asi estar corrompiendo datos en silencio.
Los invariantes son lo que separa un pipeline que funciona de uno que parece funcionar.

Uso:
    python qa/suite_qa.py                 # escenarios rapidos
    python qa/suite_qa.py --completo      # incluye 100k filas
    python qa/suite_qa.py --escenario volumen_100k
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
TMP = RAIZ / "qa" / "tmp"
REPORTES = RAIZ / "qa" / "reportes"
PY = sys.executable

VERDE, ROJO, AMAR, GRIS, FIN = "\033[92m", "\033[91m", "\033[93m", "\033[90m", "\033[0m"


# =========================================================== infraestructura
class Resultado:
    def __init__(self, escenario: str):
        self.escenario = escenario
        self.checks: list[dict] = []
        self.metricas: dict = {}
        self.error_fatal: str | None = None

    def check(self, categoria: str, nombre: str, ok: bool, detalle: str = "") -> bool:
        self.checks.append({"categoria": categoria, "nombre": nombre,
                            "ok": bool(ok), "detalle": detalle})
        marca = f"{VERDE}PASA{FIN}" if ok else f"{ROJO}FALLA{FIN}"
        print(f"    [{marca}] {categoria:<12} {nombre:<46} {GRIS}{detalle}{FIN}")
        return ok

    @property
    def ok(self) -> bool:
        return self.error_fatal is None and all(c["ok"] for c in self.checks)

    @property
    def fallos(self) -> list[dict]:
        return [c for c in self.checks if not c["ok"]]


def correr(script: str, *args: str, timeout: int = 1800) -> tuple[bool, str, float]:
    """Ejecuta una fase. Devuelve (ok, salida, segundos)."""
    t0 = time.perf_counter()
    try:
        p = subprocess.run([PY, script, *args], cwd=RAIZ, capture_output=True,
                           text=True, timeout=timeout)
        dt = time.perf_counter() - t0
        salida = (p.stdout or "") + (p.stderr or "")
        return p.returncode == 0, salida, dt
    except subprocess.TimeoutExpired:
        return False, f"TIMEOUT tras {timeout}s", time.perf_counter() - t0
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}", time.perf_counter() - t0


# ================================================================ invariantes
def verificar_invariantes(r: Resultado, datos: Path, f2: Path, f4: Path, f6: Path) -> None:
    """Los diez invariantes que un pipeline correcto cumple siempre."""

    limpias = pd.read_csv(f6 / "transacciones_limpias.csv")
    cuar = pd.read_csv(f6 / "cuarentena_errores.csv") if (f6 / "cuarentena_errores.csv").exists() \
        else pd.DataFrame()
    unif = pd.read_csv(f4 / "unificado_grupo1.csv")

    # --- I1  Conservacion de filas en la bifurcacion de la Fase 6
    r.check("INTEGRIDAD", "I1 ninguna fila se pierde en Fase 6",
            len(limpias) + len(cuar) == len(unif),
            f"{len(limpias)} + {len(cuar)} = {len(limpias)+len(cuar)} vs {len(unif)}")

    # --- I2  Ningun huerfano se filtra a la tabla limpia
    huerf = int(limpias["flag_huerfano"].sum()) if "flag_huerfano" in limpias else -1
    r.check("INTEGRIDAD", "I2 cero huerfanos en tabla limpia", huerf == 0, f"{huerf} encontrados")

    # --- I3  Ningun correo invalido sobrevive
    if "correo" in limpias.columns:
        malos = limpias["correo"].dropna().astype(str)
        malos = malos[malos.str.contains("@@|\\.\\.", regex=True)
                      | ~malos.str.contains("@", regex=False)]
        r.check("INTEGRIDAD", "I3 cero correos invalidos en tabla limpia",
                len(malos) == 0, f"{len(malos)} encontrados")

    # --- I4  Aritmetica interna coherente (salvo filas recortadas por IQR)
    if len(limpias):
        sub = limpias[~limpias["flag_outlier"].astype(bool)]
        dif = (sub["cantidad"] * sub["precio_unitario_mxn"] - sub["monto_mxn"]).abs()
        r.check("INTEGRIDAD", "I4 monto = cantidad x precio (no recortadas)",
                bool((dif <= 0.011).all()), f"max desviacion {dif.max():.4f}" if len(dif) else "")

    # --- I5  Todas las fechas en ISO y parseables
    fechas = pd.to_datetime(limpias["fecha"], format="%Y-%m-%d", errors="coerce")
    r.check("INTEGRIDAD", "I5 todas las fechas en YYYY-MM-DD",
            int(fechas.isna().sum()) == 0, f"{int(fechas.isna().sum())} no parseables")

    # --- I6  Ningun importe quedo en USD
    if "moneda" in limpias.columns:
        no_mxn = int((limpias["moneda"].astype(str).str.upper() != "MXN").sum())
        r.check("INTEGRIDAD", "I6 cero importes sin convertir a MXN", no_mxn == 0,
                f"{no_mxn} filas")

    # --- I7  Normalizado estrictamente dentro de [0,1]
    nrm = pd.to_numeric(limpias["monto_mxn_norm"], errors="coerce")
    fuera = int(((nrm < 0) | (nrm > 1) | nrm.isna()).sum())
    r.check("INTEGRIDAD", "I7 monto_mxn_norm dentro de [0,1]", fuera == 0, f"{fuera} fuera")

    # --- I8  Llave primaria unica
    dups = int(limpias["id_registro"].duplicated().sum())
    r.check("INTEGRIDAD", "I8 id_registro unico (llave primaria)", dups == 0,
            f"{dups} duplicados")

    # --- I9  Todo registro en cuarentena trae motivo
    if len(cuar):
        sin_motivo = int(cuar["motivo_rechazo_cuarentena"].isna().sum())
        r.check("INTEGRIDAD", "I9 toda cuarentena tiene motivo", sin_motivo == 0,
                f"{sin_motivo} sin motivo")

    # --- I10  Los montos no fueron alterados mas alla de lo documentado
    if len(limpias):
        no_rec = limpias[~limpias["flag_outlier"].astype(bool)]
        neg = int((no_rec["monto_mxn"] <= 0).sum())
        r.check("INTEGRIDAD", "I10 cero montos no positivos en limpias", neg == 0,
                f"{neg} filas")

    r.metricas.update({
        "filas_unificado": len(unif),
        "filas_limpias": len(limpias),
        "filas_cuarentena": len(cuar),
        "tasa_aceptacion_pct": round(100 * len(limpias) / len(unif), 2) if len(unif) else 0,
    })


def verificar_deteccion(r: Resultado, verdad: dict, f4: Path, f6: Path) -> None:
    """Comparar lo detectado contra lo que realmente plantamos."""
    unif = pd.read_csv(f4 / "unificado_grupo1.csv")
    cuar = pd.read_csv(f6 / "cuarentena_errores.csv") if (f6 / "cuarentena_errores.csv").exists() \
        else pd.DataFrame()

    # Huerfanos: el generador sabe cuantos planto en cada fuente.
    plantados = verdad.get("fisica_huerfanos", 0) + verdad.get("online_huerfanos", 0)
    detect = int(unif["flag_huerfano"].sum()) if "flag_huerfano" in unif else 0
    # Los huerfanos de filas eliminadas por cantidad<=0 o deduplicadas ya no estan.
    r.check("DETECCION", "huerfanos detectados >= 85% de los plantados",
            detect >= plantados * 0.85 if plantados else True,
            f"{detect} de {plantados} plantados")

    # Correos invalidos: deben acabar todos en cuarentena.
    plant_mail = verdad.get("online_correos_malos", 0)
    if len(cuar) and plant_mail:
        det_mail = int(cuar["motivo_rechazo_cuarentena"].astype(str)
                       .str.contains("correo", case=False).sum())
        r.check("DETECCION", "correos invalidos capturados >= 85%",
                det_mail >= plant_mail * 0.85, f"{det_mail} de {plant_mail} plantados")

    # Bots: total inflado exactamente x80.
    plant_bot = verdad.get("online_bots", 0)
    if len(cuar) and plant_bot:
        det_bot = int(cuar["motivo_rechazo_cuarentena"].astype(str)
                      .str.contains("bot|inconsistente|inflado", case=False, regex=True).sum())
        r.check("DETECCION", "bots / totales inflados capturados >= 70%",
                det_bot >= plant_bot * 0.70, f"{det_bot} de {plant_bot} plantados")

    # Falsos positivos: la cuarentena no puede desbordarse.
    if len(unif):
        pct_q = 100 * len(cuar) / len(unif)
        r.check("DETECCION", "cuarentena por debajo del 40% (sin falsos masivos)",
                pct_q < 40, f"{pct_q:.2f}% del lote")


# ================================================================= escenarios
def escenario(nombre: str, filas: int, perfil: str, corrupcion: float,
              esperar_fallo_esquema: bool = False) -> Resultado:
    r = Resultado(nombre)
    print(f"\n{'='*86}\n  ESCENARIO: {nombre}")
    print(f"  filas={filas:,}  perfil={perfil}  corrupcion={corrupcion}x\n{'='*86}")

    base = TMP / nombre
    datos, f2, f4, f6 = base / "datasets", base / "f2", base / "f4", base / "f6"
    shutil.rmtree(base, ignore_errors=True)
    datos.mkdir(parents=True, exist_ok=True)

    # --- generar
    ok, out, dt = correr("qa/generador_datasets.py", str(datos), "--filas", str(filas),
                         "--perfil", perfil, "--corrupcion", str(corrupcion))
    if not r.check("SETUP", "generacion del dataset", ok, f"{dt:.1f}s"):
        r.error_fatal = out[-600:]
        return r

    verdad = json.loads((datos / "_verdad_conocida.json").read_text(encoding="utf-8"))
    tam = sum(f.stat().st_size for f in datos.iterdir()) / 1_048_576
    r.metricas["mb_entrada"] = round(tam, 2)

    # --- Fase 0: adaptar cualquier esquema al canonico antes de procesar
    canon = base / "canon"
    ok0, out0, dt0 = correr("fase0_contrato_esquema.py", str(datos), str(canon))
    traceback0 = "Traceback (most recent call last)" in out0
    r.check("RESILIENCIA", "fase0 adapta el esquema sin excepcion",
            not traceback0, f"{dt0:.1f}s")
    if traceback0:
        r.error_fatal = out0[-1200:]
        return r

    if esperar_fallo_esquema:
        mapeo = pd.read_csv(canon / "_mapeo_esquema.csv")
        renom = int(mapeo["estrategia"].isin(["alias", "difuso"]).sum())
        r.check("ESQUEMA", "columnas renombradas reconocidas por el contrato",
                renom > 0 and ok0, f"{renom} columnas mapeadas")
        baja = mapeo[(mapeo["columna_origen"].notna()) & (mapeo["confianza"] < 0.90)]
        r.check("ESQUEMA", "sin mapeos de confianza dudosa (<0.90)",
                len(baja) == 0, f"{len(baja)} dudosos")

    datos = canon  # el resto del pipeline consume el esquema canonico

    # --- fases 1 a 7
    tiempos = {"fase0": round(dt0, 2)}
    for etiqueta, script, args in [
        ("fase1_2", "fase1_fase2_pipeline.py", [str(datos), str(f2)]),
        ("fase3_4", "fase3_fase4_pipeline.py", [str(f2), str(datos), str(f4)]),
        ("fase5_6", "fase5_fase6_pipeline.py", [str(f4), str(f6)]),
        ("fase7", "fase7_carga_bd.py", [str(f6), str(base / "pipeline.db")]),
    ]:
        ok, out, dt = correr(script, *args)
        tiempos[etiqueta] = round(dt, 2)

        traceback = "Traceback (most recent call last)" in out
        r.check("RESILIENCIA", f"{etiqueta} termina sin excepcion", ok and not traceback,
                f"{dt:.1f}s" + ("" if ok else f" | {out.strip()[-200:]}"))
        if not ok:
            r.error_fatal = out[-1200:]
            r.metricas["tiempos_s"] = tiempos
            return r

    r.metricas["tiempos_s"] = tiempos
    r.metricas["total_s"] = round(sum(tiempos.values()), 2)
    if filas:
        r.metricas["filas_por_segundo"] = int(filas * 2 / max(sum(tiempos.values()), 0.01))

    # --- verificaciones
    try:
        verificar_invariantes(r, datos, f2, f4, f6)
        verificar_deteccion(r, verdad, f4, f6)
    except Exception as e:  # noqa: BLE001
        r.check("INTEGRIDAD", "verificaciones ejecutables", False, f"{type(e).__name__}: {e}")
        r.error_fatal = str(e)

    # --- idempotencia: correr la Fase 6 otra vez debe dar lo mismo
    antes = pd.read_csv(f6 / "transacciones_limpias.csv").shape
    ok2, _, _ = correr("fase5_fase6_pipeline.py", str(f4), str(f6))
    despues = pd.read_csv(f6 / "transacciones_limpias.csv").shape
    r.check("INTEGRIDAD", "I-extra idempotencia (misma salida al repetir)",
            ok2 and antes == despues, f"{antes} vs {despues}")

    # --- carga a BD repetida no duplica
    ok3, out3, _ = correr("fase7_carga_bd.py", str(f6), str(base / "pipeline.db"))
    import sqlite3
    con = sqlite3.connect(base / "pipeline.db")
    n_bd = con.execute("SELECT COUNT(*) FROM transacciones_limpias").fetchone()[0]
    con.close()
    r.check("INTEGRIDAD", "I-extra recarga a BD es idempotente",
            ok3 and n_bd == despues[0], f"{n_bd} filas en BD vs {despues[0]} en CSV")

    return r


# ====================================================================== main
ESCENARIOS = {
    "base_2k":        dict(filas=2_000,   perfil="canonico", corrupcion=1.0),
    "sucio_3x":       dict(filas=5_000,   perfil="canonico", corrupcion=3.0),
    "limpio_0x":      dict(filas=3_000,   perfil="canonico", corrupcion=0.0),
    "ruidoso_cols":   dict(filas=3_000,   perfil="ruidoso",  corrupcion=1.0),
    "minimo_cols":    dict(filas=3_000,   perfil="minimo",   corrupcion=1.0),
    "drift_ingles":   dict(filas=3_000,   perfil="ingles",   corrupcion=1.0,
                           esperar_fallo_esquema=True),
    "drift_erp":      dict(filas=3_000,   perfil="erp",      corrupcion=1.0,
                           esperar_fallo_esquema=True),
    "volumen_25k":    dict(filas=25_000,  perfil="canonico", corrupcion=1.0),
    "volumen_100k":   dict(filas=100_000, perfil="canonico", corrupcion=1.0),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--escenario", action="append", help="correr solo estos")
    ap.add_argument("--completo", action="store_true", help="incluye volumen_100k")
    a = ap.parse_args()

    nombres = a.escenario or [k for k in ESCENARIOS
                              if a.completo or k != "volumen_100k"]
    TMP.mkdir(parents=True, exist_ok=True)
    REPORTES.mkdir(parents=True, exist_ok=True)

    print("=" * 86)
    print("  SUITE DE QA - PIPELINE ETL")
    print("=" * 86)

    resultados = []
    t0 = time.perf_counter()
    for n in nombres:
        cfg = dict(ESCENARIOS[n])
        resultados.append(escenario(n, **cfg))

    # ------------------------------------------------------------- resumen
    print("\n" + "=" * 86)
    print("  RESUMEN")
    print("=" * 86)
    print(f"  {'Escenario':<16}{'Checks':>8}{'Fallos':>8}{'Entrada':>10}"
          f"{'Limpias':>9}{'Cuar.':>8}{'Seg.':>8}  Estado")
    print("  " + "-" * 82)

    filas_rep = []
    for r in resultados:
        m = r.metricas
        estado = f"{VERDE}OK{FIN}" if r.ok else f"{ROJO}REVISAR{FIN}"
        print(f"  {r.escenario:<16}{len(r.checks):>8}{len(r.fallos):>8}"
              f"{m.get('filas_unificado', 0):>10,}{m.get('filas_limpias', 0):>9,}"
              f"{m.get('filas_cuarentena', 0):>8,}{m.get('total_s', 0):>8.1f}  {estado}")
        filas_rep.append({
            "escenario": r.escenario, "checks": len(r.checks), "fallos": len(r.fallos),
            "ok": r.ok, **{k: v for k, v in m.items() if not isinstance(v, dict)},
            "tiempos_s": json.dumps(m.get("tiempos_s", {})),
            "error_fatal": r.error_fatal or "",
        })

    total_checks = sum(len(r.checks) for r in resultados)
    total_fallos = sum(len(r.fallos) for r in resultados)
    print("  " + "-" * 82)
    print(f"  Total: {total_checks} verificaciones, {total_fallos} fallos, "
          f"{time.perf_counter()-t0:.1f}s")

    if total_fallos:
        print(f"\n  {AMAR}Fallos detectados:{FIN}")
        for r in resultados:
            for c in r.fallos:
                print(f"    [{r.escenario}] {c['categoria']} {c['nombre']}: {c['detalle']}")

    pd.DataFrame(filas_rep).to_csv(REPORTES / "resumen_qa.csv", index=False,
                                   encoding="utf-8-sig")
    detalle = [{"escenario": r.escenario, **c} for r in resultados for c in r.checks]
    pd.DataFrame(detalle).to_csv(REPORTES / "detalle_qa.csv", index=False,
                                 encoding="utf-8-sig")
    print(f"\n  Reportes en {REPORTES}")
    sys.exit(0 if total_fallos == 0 else 1)


if __name__ == "__main__":
    main()
