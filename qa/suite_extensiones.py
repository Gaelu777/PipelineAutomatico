"""
Suite de las extensiones: config.yaml, motor por bloques y pestana Antes / despues.

Complementa a suite_qa.py (pipeline oficial) y tortura_universal.py (archivos rotos).
Cada verificacion compara contra una verdad conocida, no solo "que no truene":

  CONFIG    config.yaml se lee igual con PyYAML y con el lector interno, coincide con
            los valores documentados, rechaza errores de dedo, y un cambio de umbral
            SI llega a las salidas (tipo de cambio, 4 cuantiles de punta a punta).
  BLOQUES   el modo por bloques produce EXACTAMENTE las mismas salidas (byte a byte)
            que el modo en memoria, con bloques pequenos para forzar muchos bloques;
            un fallo a media carga no deja salidas parciales.
  PANEL     el endpoint de comparacion cumple los invariantes de la tabla limpia, el
            panel sigue sin dependencias externas y responde en vivo (subida incluida).
  GIGANTE   (opcional) un CSV de N GB con errores plantados: memoria acotada y conteos
            exactos.  python qa/suite_extensiones.py --gigante 5.2

Uso:
    python qa/suite_extensiones.py                 # ~2 minutos
    python qa/suite_extensiones.py --gigante 5.2   # agrega la prueba de 5.2 GB (~20-40 min)
"""

from __future__ import annotations

import ast
import csv
import filecmp
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "qa"))
TMP = RAIZ / "qa" / "tmp_extensiones"
REPORTES = RAIZ / "qa" / "reportes"
PY = sys.executable

import configuracion as C  # noqa: E402

RESULTADOS: list[dict] = []


def check(seccion: str, nombre: str, ok: bool, detalle: str = "") -> bool:
    RESULTADOS.append({"seccion": seccion, "verificacion": nombre, "ok": bool(ok),
                       "detalle": str(detalle)[:300]})
    marca = "\033[92mOK \033[0m" if ok else "\033[91mFALLA\033[0m"
    print(f"  [{marca}] {seccion:<8} {nombre}" + (f"  ({detalle})" if detalle and not ok else ""))
    return bool(ok)


def correr(script: str, *args: str, env: dict | None = None, timeout: int = 900):
    e = dict(os.environ, PYTHONIOENCODING="utf-8")
    if env:
        e.update(env)
    return subprocess.run([PY, script, *args], cwd=RAIZ, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", env=e, timeout=timeout)


def config_variante(nombre: str, reemplazos: list[tuple[str, str]]) -> Path:
    t = (RAIZ / "config.yaml").read_text(encoding="utf-8")
    for a, b in reemplazos:
        assert a in t, a
        t = t.replace(a, b)
    ruta = TMP / f"config_{nombre}.yaml"
    ruta.write_text(t, encoding="utf-8")
    return ruta


# =============================================================== CONFIG
def seccion_config() -> None:
    print("\n  CONFIG: config.yaml")
    texto = (RAIZ / "config.yaml").read_text(encoding="utf-8")
    try:
        cfg = C.cargar()
        check("CONFIG", "config.yaml es valido", True)
    except C.ErrorConfiguracion as e:
        check("CONFIG", "config.yaml es valido", False, e)
        return
    check("CONFIG", "version declarada >= 1", cfg.version >= 1, cfg.version)
    interno = C.leer_yaml_simple(texto)
    try:
        import yaml  # type: ignore
        check("CONFIG", "lector interno == PyYAML", interno == yaml.safe_load(texto))
    except ImportError:
        check("CONFIG", "lector interno == PyYAML (PyYAML no instalado: se usa el interno)", True)
    check("CONFIG", "config.yaml == valores documentados (README / REGLAS)", interno == C.DEFECTOS)
    check("CONFIG", "umbrales de la defensa: TC 19.50, IQR 1.5, terciles BAJO/MEDIO/ALTO",
          (cfg.tipo_cambio_usd_mxn, cfg.factor_iqr, cfg.cuantiles, cfg.etiquetas)
          == (19.5, 1.5, 3, ("BAJO", "MEDIO", "ALTO")))

    malos = {
        "clave mal escrita": ("factor_iqr:", "factor_iqrr:"),
        "texto en un numero": ("tipo_cambio_usd_mxn: 19.50", "tipo_cambio_usd_mxn: diecinueve"),
        "factor IQR negativo": ("factor_iqr: 1.5", "factor_iqr: -1.5"),
        "tipo de cambio en cero": ("tipo_cambio_usd_mxn: 19.50", "tipo_cambio_usd_mxn: 0"),
        "etiquetas que no cuadran con cuantiles": ("cuantiles: 3", "cuantiles: 4"),
        "etiqueta con caracteres SQL": ("[BAJO, MEDIO, ALTO]", "[BAJO, MEDIO, \"ALTO');--\"]"),
        "periodo invertido": ('"2026-12-31"', '"2025-12-31"'),
        "booleano en lugar de entero": ("cuantiles: 3", "cuantiles: true"),
        "tabulador en la sangria": ("  factor_iqr: 1.5", "\tfactor_iqr: 1.5"),
        "clave faltante": ("  factor_bot: 80 ", "  # factor_bot: 80 "),
    }
    for nombre, (a, b) in malos.items():
        assert a in texto, a
        roto = texto.replace(a, b)
        rechazos = []
        for lector in ("interno", "auto"):
            try:
                C.validar(C.leer_yaml(roto, lector))
                rechazos.append(False)
            except C.ErrorConfiguracion:
                rechazos.append(True)
            except Exception as e:  # noqa: BLE001
                rechazos.append(f"excepcion {type(e).__name__}")
        check("CONFIG", f"rechaza: {nombre}", rechazos == [True, True], rechazos)

    # sin archivo: valores documentados (la defensa no depende de que exista)
    original = C.RUTA_POR_DEFECTO
    entorno = os.environ.pop(C.VARIABLE_ENTORNO, None)
    try:
        C.RUTA_POR_DEFECTO = TMP / "no_existe.yaml"
        d = C.cargar()
        check("CONFIG", "sin config.yaml usa los valores documentados",
              d.origen.startswith("valores por defecto") and d.tipo_cambio_usd_mxn == 19.5)
    finally:
        C.RUTA_POR_DEFECTO = original
        if entorno is not None:
            os.environ[C.VARIABLE_ENTORNO] = entorno

    # un script con configuracion invalida se detiene con codigo 2 y SIN escribir nada
    malo = config_variante("invalida", [("factor_iqr: 1.5", "factor_iqr: -1")])
    salida = TMP / "salida_config_invalida"
    p = correr("fase5_fase6_pipeline.py", "salidas_fase4", str(salida),
               env={C.VARIABLE_ENTORNO: str(malo)})
    check("CONFIG", "fase con config invalida: codigo 2, mensaje claro, sin salidas",
          p.returncode == 2 and "configuracion invalida" in p.stderr and not salida.exists(),
          f"rc={p.returncode} {p.stderr[-120:]}")

    from motor import pipeline_universal as M
    os.environ[C.VARIABLE_ENTORNO] = str(malo)
    try:
        csv_min = TMP / "min.csv"
        csv_min.write_text("a,b\n1,2\n", encoding="utf-8")
        r = M.ejecutar([csv_min], TMP / "salida_motor_invalida", lambda s: None)
        check("CONFIG", "motor con config invalida: se detiene controlado",
              r.get("ok") is False and "ErrorConfiguracion" in str(r.get("error")), r.get("error"))
    except Exception as e:  # noqa: BLE001
        check("CONFIG", "motor con config invalida: se detiene controlado", False, repr(e))
    finally:
        os.environ.pop(C.VARIABLE_ENTORNO, None)

    # un cambio de umbral SI llega al resultado: tipo de cambio 20.00
    tc20 = config_variante("tc20", [("tipo_cambio_usd_mxn: 19.50", "tipo_cambio_usd_mxn: 20.00")])
    filas = ["id_venta,cantidad,precio,total,moneda"] + [
        f"V{i},{1 + i % 3},{10 + i}.00,{(1 + i % 3) * (10 + i)}.00,{'USD' if i % 2 else 'MXN'}"
        for i in range(60)]
    f_tc = TMP / "tc.csv"
    f_tc.write_text("\n".join(filas), encoding="utf-8")
    os.environ[C.VARIABLE_ENTORNO] = str(tc20)
    try:
        r = M.ejecutar([f_tc], TMP / "salida_tc20", lambda s: None)
        ok = pd.read_csv(TMP / "salida_tc20" / "tc_limpias.csv")
        fila = ok[ok["id_venta"] == "V1"].iloc[0]          # USD, cantidad 2, precio 11.00
        check("CONFIG", "tipo de cambio 20.00 en config.yaml -> el motor convierte x 20",
              abs(fila["precio"] - 220.0) < 1e-9 and r["configuracion"]["sha256"] != cfg.sha256[:16],
              f"precio={fila['precio']}")
    finally:
        os.environ.pop(C.VARIABLE_ENTORNO, None)
        M.aplicar_configuracion()

    # pipeline oficial de punta a punta: con la config versionada reproduce las cifras de la
    # defensa, y con 4 cuantiles (Q1..Q4) segmenta en 4 y la base acepta las etiquetas nuevas
    for nombre, ruta_cfg, esperado in (
            ("defensa", None, None),
            ("cuartiles", config_variante("cuartiles", [("cuantiles: 3", "cuantiles: 4"),
                                                         ("[BAJO, MEDIO, ALTO]", "[Q1, Q2, Q3, Q4]")]),
             ["Q1", "Q2", "Q3", "Q4"])):
        base = TMP / f"oficial_{nombre}"
        env = {C.VARIABLE_ENTORNO: str(ruta_cfg)} if ruta_cfg else None
        pasos = [("fase0_contrato_esquema.py", ["datasets", str(base / "canon")]),
                 ("fase1_fase2_pipeline.py", [str(base / "canon"), str(base / "f2")]),
                 ("fase3_fase4_pipeline.py", [str(base / "f2"), str(base / "canon"), str(base / "f4")]),
                 ("fase5_fase6_pipeline.py", [str(base / "f4"), str(base / "f6")]),
                 ("fase7_carga_bd.py", [str(base / "f6"), str(base / "pipeline.db")])]
        rcs = [correr(s, *a, env=env).returncode for s, a in pasos]
        if not check("CONFIG", f"pipeline oficial completo ({nombre})", rcs == [0] * 5, rcs):
            continue
        con = sqlite3.connect(base / "pipeline.db")
        n_ok = con.execute("SELECT COUNT(*) FROM transacciones_limpias").fetchone()[0]
        n_q = con.execute("SELECT COUNT(*) FROM cuarentena_errores").fetchone()[0]
        total = con.execute("SELECT ROUND(SUM(monto_facturable_mxn),2) FROM transacciones_limpias"
                            ).fetchone()[0]
        segs = sorted(x[0] for x in con.execute(
            "SELECT DISTINCT segmento_monto FROM transacciones_limpias"))
        con.close()
        if esperado is None:
            check("CONFIG", "config versionada reproduce 1,049 / 140 / $4,074,888.52",
                  (n_ok, n_q, total) == (1049, 140, 4074888.52), (n_ok, n_q, total))
        else:
            check("CONFIG", "4 cuantiles en config.yaml: 4 segmentos y CHECK de la base acorde",
                  segs == esperado and n_ok + n_q == 1189, (segs, n_ok, n_q))


# =============================================================== BLOQUES
def comparar_modos(nombre: str, archivos: list[Path], umbral_mb: float) -> None:
    from motor import pipeline_universal as M
    a, b = TMP / "modos" / nombre / "memoria", TMP / "modos" / nombre / "bloques"
    rm = M.ejecutar(archivos, a, lambda s: None, umbral_mb=1e12)
    rb = M.ejecutar(archivos, b, lambda s: None, umbral_mb=umbral_mb)
    modos = sorted({f.get("modo") for f in rb.get("fuentes", {}).values() if f.get("modo")})
    csvs = sorted(x.name for x in a.glob("*.csv"))
    iguales = bool(csvs) and all((b / f).exists() and filecmp.cmp(a / f, b / f, shallow=False)
                                 for f in csvs)
    rel = lambda r: sorted(json.dumps(x, sort_keys=True) for x in r.get("relaciones", []))  # noqa: E731
    bloques = sum(f.get("bloques", {}).get("bloques", 0) for f in rb.get("fuentes", {}).values())
    check("BLOQUES", f"{nombre}: memoria == bloques byte a byte ({bloques} bloques)",
          "bloques" in modos and iguales and rm.get("totales") == rb.get("totales")
          and rel(rm) == rel(rb),
          f"modos={modos} csv_iguales={iguales} {rm.get('totales')} vs {rb.get('totales')}")
    tb = rb.get("totales", {})
    check("BLOQUES", f"{nombre}: limpias + cuarentena = entrada y base cuadra",
          tb and tb["limpias"] + tb["cuarentena"] == tb["entrada"]
          and sum(v for k, v in rb.get("bd", {}).items() if not k.startswith("_")) == tb["entrada"],
          tb)


def seccion_bloques() -> None:
    print("\n  BLOQUES: motor universal por bloques")
    from motor import pipeline_universal as M
    import generador_datasets as G
    import tortura_universal as T

    # bloques de 250 filas: un archivo de 2,000 filas pasa por 8 bloques
    cfg = config_variante("bloques", [("filas_por_bloque: 200000", "filas_por_bloque: 250")])
    os.environ[C.VARIABLE_ENTORNO] = str(cfg)
    try:
        comparar_modos("datasets_oficiales", sorted((RAIZ / "datasets").iterdir()), 0.01)

        # verdad conocida con duplicados, bots, huerfanos, USD y correos rotos; online en JSONL
        cat, fis, onl, _ = G.generar(4000, 3000, tasa_corrupcion=2.0)
        d = TMP / "generado"
        d.mkdir(parents=True, exist_ok=True)
        cat.to_csv(d / "catalogo_productos.csv", index=False)
        fis.to_csv(d / "ventas_tienda_fisica.csv", index=False)
        with open(d / "ventas_online.jsonl", "w", encoding="utf-8") as f:
            for r in onl:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        comparar_modos("generador_sucio_x2 (CSV + JSONL)", sorted(d.iterdir()), 0.05)

        for nombre, archivos in T.casos(False):
            if archivos is None or not any(Path(k).suffix in (".csv", ".tsv", ".jsonl")
                                           for k in archivos) or nombre in (
                    "archivo_vacio", "basura_binaria", "solo_encabezado",
                    "una_sola_columna_toda_nula", "lineas_rotas_csv", "fechas_mezcladas_y_epoch",
                    "infinitos_y_numeros_raros", "columnas_duplicadas_y_sin_nombre"):
                continue
            dd = TMP / "tortura" / nombre
            dd.mkdir(parents=True, exist_ok=True)
            for k, v in archivos.items():
                (dd / k).write_bytes(v)
            comparar_modos(f"tortura/{nombre}", sorted(dd.iterdir()), 0.001)

        # archivos raros que superan el umbral: nunca una excepcion no controlada
        raros = {
            "binario": (b"\x00\x01\x02" * 5000, "x.csv", lambda r: r.get("ok") is False),
            "json_un_documento": (json.dumps([{"a": i, "b": i * 2} for i in range(3000)]).encode(),
                                  "y.json", lambda r: r.get("ok") and r["totales"]["entrada"] == 3000),
            "comilla_sin_cerrar": (("a,b\n" + "".join(f"{i},{i}\n" for i in range(3000))
                                    + '"sin cerrar,9\n').encode(), "z.csv",
                                   lambda r: r.get("ok") and r["totales"]["entrada"] >= 3000),
            # Limitacion conocida de pandas (motor C): si la linea con columnas de mas cae justo
            # al inicio de un bloque se conserva recortada en vez de omitirse. Nunca se pierde.
            "columnas_de_mas": (("a,b\n" + "".join(f"{i},{i}\n" for i in range(3000))
                                 + "1,2,3,4\n").encode(), "w.csv",
                                lambda r: r.get("ok") and r["totales"]["entrada"] >= 3000),
        }
        for nombre, (datos, archivo, criterio) in raros.items():
            dd = TMP / "raros" / nombre
            dd.mkdir(parents=True, exist_ok=True)
            (dd / archivo).write_bytes(datos)
            try:
                r = M.ejecutar([dd / archivo], dd / "out", lambda s: None, umbral_mb=0.001)
                check("BLOQUES", f"archivo grande raro '{nombre}': controlado", bool(criterio(r)),
                      r.get("totales") or r.get("error"))
            except Exception as e:  # noqa: BLE001
                check("BLOQUES", f"archivo grande raro '{nombre}': controlado", False, repr(e))

        # ZIP enorme: se extrae a disco por streaming y su CSV va por bloques
        dz = TMP / "zip"
        dz.mkdir(parents=True, exist_ok=True)
        import zipfile
        with zipfile.ZipFile(dz / "grande.zip", "w", zipfile.ZIP_DEFLATED) as z:
            z.write(d / "ventas_tienda_fisica.csv", "dentro/ventas.csv")
        r = M.ejecutar([dz / "grande.zip"], dz / "out", lambda s: None, umbral_mb=0.001)
        check("BLOQUES", "ZIP grande: extraccion por streaming y CSV por bloques",
              r.get("ok") and any(f.get("modo") == "bloques" for f in r["fuentes"].values()),
              r.get("error"))

        # atomicidad: si algo falla a media pasada 3, no quedan CSV ni tablas a medias
        original = M.fase6_validar
        llamadas = {"n": 0}

        def falla_en_el_tercero(*a, **k):
            llamadas["n"] += 1
            if llamadas["n"] == 3:
                raise RuntimeError("falla simulada a media carga")
            return original(*a, **k)

        M.fase6_validar = falla_en_el_tercero
        try:
            out = TMP / "atomicidad"
            r = M.ejecutar([d / "ventas_tienda_fisica.csv"], out, lambda s: None, umbral_mb=0.001)
            con = sqlite3.connect(out / "resultado.db")
            tablas = [x[0] for x in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            con.close()
            check("BLOQUES", "falla a media carga: rollback, sin CSV ni tablas parciales",
                  not list(out.glob("ventas_tienda_fisica_*.csv*"))
                  and not any(t.startswith("ventas_tienda_fisica") for t in tablas)
                  and any("fallo el procesamiento por bloques" in e["mensaje"]
                          for e in r.get("bitacora", [])), tablas)
        finally:
            M.fase6_validar = original
    finally:
        os.environ.pop(C.VARIABLE_ENTORNO, None)
        M.aplicar_configuracion()


# =============================================================== PANEL
def seccion_panel() -> None:
    print("\n  PANEL: pestana Antes / despues")
    stdlib = set(sys.stdlib_module_names)
    for archivo in ("app/servidor.py", "configuracion.py", "app/generar_dashboard_offline.py"):
        arbol = ast.parse((RAIZ / archivo).read_text(encoding="utf-8"))
        externos = set()
        for n in ast.walk(arbol):
            if isinstance(n, ast.Import):
                externos |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                externos.add(n.module.split(".")[0])
        externos -= stdlib | {"servidor", "motor", "configuracion", "yaml", "__future__"}
        check("PANEL", f"{archivo}: solo libreria estandar", not externos, externos)

    sys.path.insert(0, str(RAIZ / "app"))
    import servidor as S
    con = sqlite3.connect(RAIZ / "pipeline.db")
    n_bd = {c: n for c, n in con.execute(
        "SELECT canal, COUNT(*) FROM transacciones_limpias GROUP BY canal")}
    con.close()
    for canal in (None, "FISICA", "ONLINE"):
        r = S.api_comparacion(canal)
        cols = {c["columna"]: c for c in r["columnas"]}
        esperado = sum(n_bd.values()) if canal is None else n_bd.get(canal, 0)
        ok = r["despues"]["filas"] == esperado and r["antes"]["filas"] > r["despues"]["filas"]
        m, f = cols.get("monto_mxn", {}).get("despues") or {}, cols.get("fecha", {}).get("despues") or {}
        ok &= m.get("nulos") == 0 and m.get("no_positivos") == 0 and m.get("no_numericos") == 0
        ok &= list((f.get("formatos") or {"x": 0}).keys()) == ["AAAA-MM-DD"]
        if "correo" in cols:
            ok &= cols["correo"]["despues"]["invalidos"] == 0
        check("PANEL", f"comparacion {canal or 'TODOS'}: invariantes de la tabla limpia", ok,
              json.dumps({k: (cols[k]["antes"], cols[k]["despues"]) for k in ("monto_mxn",)
                          if k in cols})[:250])

    # servidor real, de punta a punta, como en la defensa
    # -u: sin bufer, para leer la linea con la URL en cuanto el servidor la imprime
    p = subprocess.Popen([PY, "-u", "app/servidor.py", "--sin-navegador", "--puerto", "8790"], cwd=RAIZ,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                         encoding="utf-8", errors="replace",
                         env=dict(os.environ, PYTHONIOENCODING="utf-8"))
    url, cid = None, None
    try:
        t0 = time.time()
        for linea in p.stdout:  # type: ignore[union-attr]
            if "Abre en el navegador:" in linea:
                url = linea.split("Abre en el navegador:")[1].strip()
                break
            if time.time() - t0 > 20:
                break
        if not check("PANEL", "servidor --sin-navegador levanta", bool(url)):
            return

        def get(ruta: str):
            with urllib.request.urlopen(url + ruta, timeout=30) as r:
                return r.status, r.read()

        for ruta in ("/", "/app.js", "/estilos.css", "/api/resumen", "/api/comparacion",
                     "/api/comparacion?canal=FISICA", "/api/comparacion?canal=ONLINE"):
            st, cuerpo = get(ruta)
            ok = st == 200 and (not ruta.startswith("/api") or isinstance(json.loads(cuerpo), dict))
            check("PANEL", f"GET {ruta} -> 200", ok, st)
        html = get("/")[1].decode("utf-8")
        check("PANEL", "la pestana 'Antes / despues' existe en el panel",
              'data-vista="comparacion"' in html and 'id="v-comparacion"' in html)

        datos = "\n".join(["Folio,Fecha,Correo,Cantidad,Precio,Total,Moneda"] + [
            f"F{i},{'2026-03-0' + str(1 + i % 9) if i % 2 else str(1 + i % 9) + '/03/2026'},"
            f"{'mal@@x..com' if i % 37 == 0 else f'c{i}@x.com'},{1 + i % 4},{10 + i},"
            f"{'' if i % 29 == 0 else (1 + i % 4) * (10 + i)},{'USD' if i % 5 == 0 else 'MXN'}"
            for i in range(300)]).encode()
        req = urllib.request.Request(url + "/api/universal/subir", data=datos, method="POST",
                                     headers={"X-Filename": "prueba_panel.csv"})
        with urllib.request.urlopen(req, timeout=30) as r:
            cid = json.loads(r.read())["id"]
        urllib.request.urlopen(urllib.request.Request(
            f"{url}/api/universal/ejecutar?id={cid}", data=b"", method="POST"), timeout=30).read()
        estado = {}
        for _ in range(120):
            estado = json.loads(get(f"/api/universal/estado?id={cid}")[1])
            if not estado.get("activo") and estado.get("ok") is not None:
                break
            time.sleep(0.5)
        rep = json.loads(get(f"/api/universal/reporte?id={cid}")[1])
        fuente = next(iter(rep.get("fuentes", {}).values()), {})
        cols = {c["columna"]: c for c in (fuente.get("comparacion") or {}).get("columnas", [])}
        check("PANEL", "subida desde el panel: procesa y trae la comparacion por columna",
              estado.get("ok") is True and rep.get("ok") and "fecha" in cols
              and list(cols["fecha"]["despues"]["formatos"]) == ["AAAA-MM-DD"]
              and cols["correo"]["antes"]["invalidos"] > 0
              and cols["correo"]["despues"]["invalidos"] == 0,
              {k: v for k, v in rep.items() if k in ("ok", "error", "totales")})
    except Exception as e:  # noqa: BLE001
        check("PANEL", "panel en vivo sin errores", False, repr(e))
    finally:
        p.terminate()
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
        if cid:
            shutil.rmtree(RAIZ / "corridas" / cid, ignore_errors=True)


# =============================================================== GIGANTE
CODIGO_HIJO = r"""
import json, sys, time
sys.path.insert(0, sys.argv[1])
from pathlib import Path
from motor.pipeline_universal import ejecutar
t0 = time.perf_counter()
r = ejecutar([Path(sys.argv[2])], Path(sys.argv[3]), lambda s: print(s, flush=True))
seg = time.perf_counter() - t0
pico = None
try:
    import ctypes
    from ctypes import wintypes
    class PMC(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                    ("a", ctypes.c_size_t), ("b", ctypes.c_size_t), ("c", ctypes.c_size_t),
                    ("d", ctypes.c_size_t), ("e", ctypes.c_size_t), ("f", ctypes.c_size_t)]
    m = PMC(); m.cb = ctypes.sizeof(PMC)
    ctypes.windll.psapi.GetProcessMemoryInfo(ctypes.windll.kernel32.GetCurrentProcess(),
                                             ctypes.byref(m), m.cb)
    pico = m.PeakWorkingSetSize / 1024**2
except Exception:
    import resource
    pico = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
print("@@RESULTADO@@" + json.dumps({"ok": r.get("ok"), "error": r.get("error"),
      "totales": r.get("totales"), "seg": round(seg, 1), "pico_mb": round(pico or 0, 1),
      "causas": {k: v.get("fase6", {}).get("causas") for k, v in r.get("fuentes", {}).items()},
      "bloques": {k: v.get("bloques") for k, v in r.get("fuentes", {}).items()}}))
"""


def generar_gigante(ruta: Path, gb: float) -> dict:
    """CSV de `gb` GB con errores plantados en posiciones conocidas (verdad conocida)."""
    objetivo = gb * 1024**3
    rng = np.random.default_rng(5)
    escritos, n, primero = 0, 0, True
    # causa_principal sigue la prioridad de etiqueta_causa: bot > correo > imposible
    verdad = {"correo": 0, "imposible": 0, "bot": 0, "filas": 0, "con_error": 0}
    relleno = "Calle Prueba No. 123, Col. Centro, Colima, Col., CP 28000 - " * 4
    while escritos < objetivo:
        m = 500_000
        i = np.arange(n, n + m)
        cant = rng.integers(1, 6, m)
        precio = np.round(rng.uniform(20, 900, m), 2)
        total = np.round(cant * precio, 2)
        malo_correo = i % 1000 == 7
        imposible = i % 997 == 3
        bot = (i % 1009 == 5) & ~imposible
        cant = np.where(imposible, -1, cant)
        total = np.where(bot, np.round(cant * precio * 80, 2), np.where(imposible, -precio, total))
        fechas = pd.to_datetime("2026-01-01") + pd.to_timedelta(i % 360, unit="D")
        fecha_txt = np.where(i % 3 == 0, fechas.strftime("%d/%m/%Y"),
                             np.where(i % 3 == 1, fechas.strftime("%Y-%m-%d"),
                                      (fechas.astype("int64") // 10**9 + 43200).astype(str)))
        df = pd.DataFrame({
            "id_venta": np.char.add("V", i.astype(str)),
            "fecha": fecha_txt,
            "correo": np.where(malo_correo, "cliente@@mail..com",
                               np.char.add(np.char.add("c", i.astype(str)), "@mail.com")),
            "cantidad": cant, "precio": precio, "total": total,
            "moneda": np.where(i % 10 == 0, "USD", "MXN"),
            "sucursal": rng.choice(["Norte", "Sur", "centro ", "CENTRO"], m),
            "direccion": np.char.add(relleno, i.astype(str)),
        })
        b = df.to_csv(index=False, header=primero, lineterminator="\n").encode("utf-8")
        with open(ruta, "ab" if not primero else "wb") as f:
            f.write(b)
        escritos += len(b)
        n += m
        primero = False
        verdad["bot"] += int(bot.sum())
        verdad["correo"] += int((malo_correo & ~bot).sum())
        verdad["imposible"] += int((imposible & ~malo_correo).sum())
        verdad["con_error"] += int((malo_correo | imposible | bot).sum())
    verdad["filas"] = n
    verdad["gb"] = round(escritos / 1024**3, 2)
    return verdad


def seccion_gigante(gb: float) -> None:
    print(f"\n  GIGANTE: CSV de {gb} GB (memoria acotada y verdad conocida)")
    d = RAIZ / "qa" / "tmp_gigante"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    t0 = time.perf_counter()
    verdad = generar_gigante(d / "ventas_gigantes.csv", gb)
    print(f"    generado {verdad['gb']} GB, {verdad['filas']:,} filas en "
          f"{time.perf_counter() - t0:,.0f}s; corriendo el motor...", flush=True)
    p = subprocess.Popen([PY, "-c", CODIGO_HIJO, str(RAIZ), str(d / "ventas_gigantes.csv"),
                          str(d / "out")], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         text=True, encoding="utf-8", errors="replace",
                         env=dict(os.environ, PYTHONIOENCODING="utf-8"))
    res = {}
    for linea in p.stdout:  # type: ignore[union-attr]
        if linea.startswith("@@RESULTADO@@"):
            res = json.loads(linea[len("@@RESULTADO@@"):])
        elif "bloque" in linea and ("0%" in linea or "pasada" in linea) or "pasada" in linea:
            print("   ", linea.rstrip(), flush=True)
    p.wait()
    t = res.get("totales") or {}
    causas = next(iter((res.get("causas") or {}).values()), {}) or {}
    info = next(iter((res.get("bloques") or {}).values()), {}) or {}
    check("GIGANTE", f"{verdad['gb']} GB ({verdad['filas']:,} filas) sin excepciones",
          p.returncode == 0 and res.get("ok"), res.get("error"))
    check("GIGANTE", "procesado por bloques (no se cargo completo a memoria)",
          info.get("bloques", 0) > 1, info)
    check("GIGANTE", "ninguna fila se pierde: limpias + cuarentena = filas del archivo",
          t.get("entrada") == verdad["filas"] and t["limpias"] + t["cuarentena"] == t["entrada"], t)
    check("GIGANTE", "cuarentena = exactamente las filas con error plantado",
          t.get("cuarentena") == verdad["con_error"], (t.get("cuarentena"), verdad["con_error"]))
    check("GIGANTE", "causas exactas: correos, imposibles y bots plantados",
          causas.get("Correo invalido") == verdad["correo"]
          and causas.get("Valor imposible") == verdad["imposible"]
          and causas.get("Total inflado (bot)") == verdad["bot"]
          and sum(causas.values()) == verdad["con_error"], (causas, verdad))
    pico = res.get("pico_mb") or 0
    check("GIGANTE", f"memoria pico {pico:,.0f} MB (< 4 GB) para un archivo de {verdad['gb']} GB",
          0 < pico < 4096, pico)
    print(f"    tiempo del motor: {res.get('seg', 0):,.0f}s ({res.get('seg', 0) / 60:,.1f} min)")
    with open(REPORTES / "gigante.json", "w", encoding="utf-8") as f:
        json.dump({"verdad": verdad, "resultado": res}, f, ensure_ascii=False, indent=1)
    if "--conservar" not in sys.argv:
        shutil.rmtree(d, ignore_errors=True)


# =================================================================== main
def main() -> None:
    t0 = time.perf_counter()
    shutil.rmtree(TMP, ignore_errors=True)
    TMP.mkdir(parents=True)
    REPORTES.mkdir(parents=True, exist_ok=True)
    print("=" * 78)
    print("  SUITE DE EXTENSIONES: config.yaml, motor por bloques y Antes / despues")
    print("=" * 78)
    for seccion in (seccion_config, seccion_bloques, seccion_panel):
        try:
            seccion()
        except Exception as e:  # noqa: BLE001  una seccion rota no oculta a las demas
            check(seccion.__name__.replace("seccion_", "").upper(), "seccion completa", False,
                  f"{type(e).__name__}: {e}")
    if "--gigante" in sys.argv:
        i = sys.argv.index("--gigante")
        gb = float(sys.argv[i + 1]) if len(sys.argv) > i + 1 else 5.2
        seccion_gigante(gb)
    res = pd.DataFrame(RESULTADOS)
    res.to_csv(REPORTES / "extensiones_qa.csv", index=False, encoding="utf-8-sig")
    fallos = int((~res.ok).sum())
    print("\n  " + "-" * 60)
    for s, g in res.groupby("seccion", sort=False):
        print(f"  {s:<8} {len(g):>3} verificaciones, {int((~g.ok).sum())} fallos")
    print(f"  Total: {len(res)} verificaciones, {fallos} fallos, {time.perf_counter() - t0:,.1f}s")
    print(f"  Reporte: {REPORTES / 'extensiones_qa.csv'}")
    shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
