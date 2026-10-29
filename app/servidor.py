"""
Servidor del panel de control del pipeline ETL.

Decision de diseno: SOLO libreria estandar de Python.
Sin FastAPI, sin Flask, sin CDN, sin npm. El panel tiene que levantar en
cualquier maquina con Python 3.10+ y sin internet, porque va a usarse en una
defensa en vivo y cada dependencia externa es un punto de falla mas.

Uso:
    python app/servidor.py                 # http://127.0.0.1:8000
    python app/servidor.py --puerto 8080
    python app/servidor.py --sin-navegador
"""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

RAIZ = Path(__file__).resolve().parent.parent
ESTATICO = Path(__file__).resolve().parent / "estatico"
PY = sys.executable

F4 = RAIZ / "salidas_fase4"
F6 = RAIZ / "salidas_fase6"
BD = RAIZ / "pipeline.db"
QA = RAIZ / "qa" / "reportes"
CORRIDAS = RAIZ / "corridas"
MAX_SUBIDA = 2 * 1024**3            # 2 GB por archivo
EXT_OK = {".csv", ".tsv", ".txt", ".xlsx", ".xlsm", ".xls", ".json", ".jsonl", ".ndjson",
          ".parquet", ".zip"}
sys.path.insert(0, str(RAIZ))
UNIV: dict[str, dict] = {}          # estado por corrida

# Estado compartido de la ejecucion del pipeline.
EJECUCION: dict = {"activo": False, "lineas": [], "fase": "", "inicio": None,
                   "fin": None, "ok": None, "paso": 0, "total_pasos": 6}
CANDADO = threading.Lock()


# ============================================================== utilidades
def leer_csv(ruta: Path, limite: int | None = None) -> list[dict]:
    """Lee un CSV a lista de dicts. Devuelve [] si no existe: el panel nunca revienta."""
    if not ruta.exists():
        return []
    try:
        with open(ruta, encoding="utf-8-sig", newline="") as f:
            filas = []
            for i, fila in enumerate(csv.DictReader(f)):
                if limite is not None and i >= limite:
                    break
                filas.append(fila)
            return filas
    except Exception:  # noqa: BLE001
        return []


def leer_json(ruta: Path, por_defecto=None):
    if not ruta.exists():
        return por_defecto if por_defecto is not None else {}
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return por_defecto if por_defecto is not None else {}


def num(v, defecto=0.0) -> float:
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return defecto


def consulta_bd(sql: str, params: tuple = ()) -> list[dict]:
    if not BD.exists():
        return []
    try:
        con = sqlite3.connect(f"file:{BD}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        filas = [dict(r) for r in con.execute(sql, params).fetchall()]
        con.close()
        return filas
    except Exception:  # noqa: BLE001
        return []


# ================================================================ endpoints
def api_resumen() -> dict:
    res = leer_json(F6 / "resumen_fase5_fase6.json")
    limpias = consulta_bd("SELECT COUNT(*) n FROM transacciones_limpias")
    cuar = consulta_bd("SELECT COUNT(*) n FROM cuarentena_errores")
    dinero = consulta_bd(
        "SELECT ROUND(SUM(monto_facturable_mxn),2) total, "
        "ROUND(AVG(monto_facturable_mxn),2) ticket FROM transacciones_limpias")

    n_lim = limpias[0]["n"] if limpias else res.get("validos", 0)
    n_cua = cuar[0]["n"] if cuar else res.get("cuarentena", 0)
    entrada = res.get("entrada", n_lim + n_cua)

    estres = leer_csv(F6 / "pruebas_estres.csv")
    crashes = sum(1 for r in estres if r.get("resultado") == "CRASH")

    return {
        "entrada": entrada,
        "limpias": n_lim,
        "cuarentena": n_cua,
        "tasa_aceptacion": round(100 * n_lim / entrada, 2) if entrada else 0,
        "monto_facturable": dinero[0]["total"] if dinero and dinero[0]["total"] else 0,
        "ticket_promedio": dinero[0]["ticket"] if dinero and dinero[0]["ticket"] else 0,
        "pruebas_estres": len(estres),
        "crashes": crashes,
        "bd_existe": BD.exists(),
        "bd_kb": round(BD.stat().st_size / 1024, 1) if BD.exists() else 0,
        "actualizado": time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.localtime(BD.stat().st_mtime)) if BD.exists() else "-",
    }


def api_fases() -> list[dict]:
    """Embudo: cuantos registros sobreviven a cada fase."""
    perfil = leer_csv(F4 / "../salidas_fase2/perfil_fase1.csv") if F4.exists() else []
    r12 = leer_json(RAIZ / "salidas_fase2" / "resumen_fase1_fase2.json")
    r34 = leer_json(F4 / "resumen_fase3_fase4.json")
    r56 = leer_json(F6 / "resumen_fase5_fase6.json")

    crudo = 0
    if isinstance(r12, dict):
        for v in r12.values():
            if isinstance(v, dict) and "filas" in v:
                crudo += int(v.get("filas") or 0)
    entrada56 = r56.get("entrada", 0)

    return [
        {"fase": "0", "nombre": "Contrato de esquema",
         "detalle": "Traduce cualquier esquema al canonico", "registros": crudo or entrada56},
        {"fase": "1", "nombre": "Diagnostico de calidad",
         "detalle": "Completitud, unicidad y tipos", "registros": crudo or entrada56},
        {"fase": "2", "nombre": "Faltantes e imputacion",
         "detalle": "MCAR/MAR, mediana y etiquetas", "registros": crudo or entrada56},
        {"fase": "3", "nombre": "Outliers con IQR",
         "detalle": "Capping, sin borrar filas validas", "registros": entrada56},
        {"fase": "4", "nombre": "Integracion y huerfanos",
         "detalle": "Fechas ISO, USD a MXN, cruce maestro", "registros": entrada56},
        {"fase": "5", "nombre": "Escalamiento y discretizacion",
         "detalle": "Min-Max [0,1] y 3 segmentos", "registros": entrada56},
        {"fase": "6", "nombre": "Validacion Pydantic",
         "detalle": "Esquema estricto y cuarentena", "registros": r56.get("validos", 0)},
        {"fase": "7", "nombre": "Carga transaccional",
         "detalle": "SQLite con commit/rollback", "registros": r56.get("validos", 0)},
    ]


def api_cuarentena(causa: str | None = None, limite: int = 100) -> dict:
    resumen = leer_csv(F6 / "resumen_cuarentena.csv")
    resumen = [{"causa": r.get("causa", ""), "registros": int(num(r.get("registros", 0)))}
               for r in resumen]

    if causa:
        filas = consulta_bd(
            "SELECT id_registro, canal, id_producto, cliente, monto_mxn, fecha, "
            "causa_principal, motivo_rechazo_cuarentena FROM cuarentena_errores "
            "WHERE causa_principal = ? LIMIT ?", (causa, limite))
    else:
        filas = consulta_bd(
            "SELECT id_registro, canal, id_producto, cliente, monto_mxn, fecha, "
            "causa_principal, motivo_rechazo_cuarentena FROM cuarentena_errores LIMIT ?",
            (limite,))
    return {"resumen": resumen, "registros": filas}


def api_enganosos() -> list[dict]:
    filas = leer_csv(F4 / "datos_enganosos.csv")
    return [{
        "codigo": r.get("codigo", ""), "titulo": r.get("titulo", ""),
        "afectados": int(num(r.get("afectados", 0))), "total": int(num(r.get("total", 0))),
        "pct": num(r.get("pct", 0)), "por_que": r.get("por_que_enganan", ""),
        "evidencia": r.get("evidencia", ""), "accion": r.get("accion_fase6", ""),
    } for r in filas]


def api_segmentos() -> dict:
    rangos = leer_csv(F6 / "segmentos_fase5.csv")
    cruce = consulta_bd(
        "SELECT canal, segmento_monto segmento, COUNT(*) pedidos, "
        "ROUND(SUM(monto_facturable_mxn),2) facturado "
        "FROM transacciones_limpias GROUP BY canal, segmento_monto")
    params = leer_json(F6 / "parametros_escalamiento.json")
    return {"rangos": rangos, "cruce": cruce, "parametros": params}


def api_estres() -> list[dict]:
    return leer_csv(F6 / "pruebas_estres.csv")


def api_qa() -> dict:
    return {"resumen": leer_csv(QA / "resumen_qa.csv"),
            "detalle": leer_csv(QA / "detalle_qa.csv")}


def api_serie_temporal() -> list[dict]:
    return consulta_bd(
        "SELECT fecha, COUNT(*) pedidos, ROUND(SUM(monto_facturable_mxn),2) facturado "
        "FROM transacciones_limpias GROUP BY fecha ORDER BY fecha")


def api_top_productos() -> list[dict]:
    return consulta_bd(
        "SELECT id_producto, COALESCE(nombre_producto,'-') nombre, COALESCE(categoria,'-') categoria, "
        "COUNT(*) pedidos, ROUND(SUM(monto_facturable_mxn),2) facturado "
        "FROM transacciones_limpias GROUP BY id_producto "
        "ORDER BY facturado DESC LIMIT 12")


# ======================================================= ejecutar pipeline
PASOS = [
    ("Fase 0  Contrato de esquema", "fase0_contrato_esquema.py", ["datasets", "datasets_canon"]),
    ("Fase 1-2  Diagnostico e imputacion", "fase1_fase2_pipeline.py",
     ["datasets_canon", "salidas_fase2"]),
    ("Fase 3-4  Outliers e integracion", "fase3_fase4_pipeline.py",
     ["salidas_fase2", "datasets_canon", "salidas_fase4"]),
    ("Analisis forense de datos enganosos", "utilidades/forense_datos_enganosos.py",
     ["datasets_canon", "salidas_fase4"]),
    ("Fase 5-6  Escalamiento y validacion", "fase5_fase6_pipeline.py",
     ["salidas_fase4", "salidas_fase6"]),
    ("Fase 7  Carga transaccional a SQLite", "fase7_carga_bd.py",
     ["salidas_fase6", "pipeline.db"]),
]


def _emitir(texto: str) -> None:
    with CANDADO:
        EJECUCION["lineas"].append(texto)
        if len(EJECUCION["lineas"]) > 4000:
            del EJECUCION["lineas"][:1500]


def ejecutar_pipeline() -> None:
    with CANDADO:
        EJECUCION.update({"activo": True, "lineas": [], "fase": "", "ok": None,
                          "inicio": time.time(), "fin": None, "paso": 0,
                          "total_pasos": len(PASOS)})
    todo_ok = True
    try:
        for i, (etiqueta, script, args) in enumerate(PASOS, 1):
            with CANDADO:
                EJECUCION["fase"] = etiqueta
                EJECUCION["paso"] = i
            _emitir(f"\n{'='*70}\n  [{i}/{len(PASOS)}] {etiqueta}\n{'='*70}")
            t0 = time.perf_counter()
            try:
                p = subprocess.Popen([PY, script, *args], cwd=RAIZ, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1)
                for linea in p.stdout:  # type: ignore[union-attr]
                    _emitir(linea.rstrip("\n"))
                p.wait(timeout=1800)
                dt = time.perf_counter() - t0
                if p.returncode == 0:
                    _emitir(f"  -> OK en {dt:.1f}s")
                else:
                    todo_ok = False
                    _emitir(f"  -> FALLO (codigo {p.returncode}) en {dt:.1f}s")
                    break
            except Exception as e:  # noqa: BLE001
                todo_ok = False
                _emitir(f"  -> ERROR no controlado: {type(e).__name__}: {e}")
                break
    finally:
        with CANDADO:
            EJECUCION.update({"activo": False, "ok": todo_ok, "fin": time.time()})
        _emitir(f"\n{'='*70}")
        _emitir("  PIPELINE COMPLETO" if todo_ok else "  PIPELINE DETENIDO POR UN ERROR")
        _emitir(f"{'='*70}")




# ======================================================= motor universal
import re as _re
import uuid as _uuid
from urllib.parse import unquote as _unquote


def _id_valido(i: str | None) -> bool:
    return bool(i) and bool(_re.fullmatch(r"[a-f0-9]{12}", i))


def univ_ejecutar(cid: str) -> None:
    from motor.pipeline_universal import ejecutar
    est = UNIV[cid]
    entrada, salida = CORRIDAS / cid / "in", CORRIDAS / cid / "out"

    def avisar(s: str) -> None:
        with CANDADO:
            est["lineas"].append(s)
            if len(est["lineas"]) > 3000:
                del est["lineas"][:1000]

    est.update(activo=True, inicio=time.time(), ok=None)
    try:
        archivos = sorted(p for p in entrada.iterdir() if p.is_file())
        r = ejecutar(archivos, salida, avisar)
        est["ok"] = bool(r.get("ok"))
    except Exception as e:  # noqa: BLE001
        avisar(f"  [x] error no controlado del servidor: {type(e).__name__}: {e}")
        est["ok"] = False
    finally:
        est.update(activo=False, fin=time.time())

# ================================================================ servidor
RUTAS_API = {
    "/api/resumen": lambda q: api_resumen(),
    "/api/fases": lambda q: api_fases(),
    "/api/cuarentena": lambda q: api_cuarentena(
        (q.get("causa") or [None])[0], int((q.get("limite") or [100])[0])),
    "/api/enganosos": lambda q: api_enganosos(),
    "/api/segmentos": lambda q: api_segmentos(),
    "/api/estres": lambda q: api_estres(),
    "/api/qa": lambda q: api_qa(),
    "/api/serie": lambda q: api_serie_temporal(),
    "/api/productos": lambda q: api_top_productos(),
}


class Manejador(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # silencio: la consola es para el pipeline
        pass

    def _responder(self, codigo: int, cuerpo: bytes, tipo: str) -> None:
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(cuerpo)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(cuerpo)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, datos, codigo: int = 200) -> None:
        from motor.pipeline_universal import sanear_json
        self._responder(codigo, json.dumps(sanear_json(datos), ensure_ascii=False, default=str,
                                           allow_nan=False).encode(),
                        "application/json; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        u = urlparse(self.path)
        ruta, q = u.path, parse_qs(u.query)

        if ruta in RUTAS_API:
            try:
                self._json(RUTAS_API[ruta](q))
            except Exception as e:  # noqa: BLE001
                self._json({"error": f"{type(e).__name__}: {e}"}, 500)
            return

        if ruta == "/api/universal/estado":
            cid = (q.get("id") or [""])[0]
            if not _id_valido(cid) or cid not in UNIV:
                self._json({"error": "corrida inexistente"}, 404)
                return
            with CANDADO:
                e = dict(UNIV[cid])
                e["lineas"] = e["lineas"][-300:]
            e["segundos"] = round((e.get("fin") or time.time()) - (e.get("inicio") or time.time()), 1)
            self._json(e)
            return

        if ruta == "/api/universal/reporte":
            cid = (q.get("id") or [""])[0]
            f = CORRIDAS / cid / "out" / "reporte.json"
            if not _id_valido(cid) or not f.exists():
                self._json({"error": "sin reporte"}, 404)
                return
            r = json.loads(f.read_text(encoding="utf-8"))
            r.pop("bitacora", None)
            r["archivos"] = sorted(x.name for x in f.parent.iterdir()
                                   if x.suffix in (".csv", ".db", ".json"))
            self._json(r)
            return

        if ruta == "/api/universal/descargar":
            cid, nombre = (q.get("id") or [""])[0], (q.get("f") or [""])[0]
            f = (CORRIDAS / cid / "out" / Path(nombre).name)
            if not _id_valido(cid) or not f.is_file():
                self._responder(404, b"No encontrado", "text/plain; charset=utf-8")
                return
            datos = f.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{f.name}"')
            self.send_header("Content-Length", str(len(datos)))
            self.end_headers()
            self.wfile.write(datos)
            return

        if ruta == "/api/estado":
            with CANDADO:
                e = dict(EJECUCION)
                e["lineas"] = e["lineas"][-400:]
                e["segundos"] = round((e["fin"] or time.time()) - e["inicio"], 1) \
                    if e["inicio"] else 0
            self._json(e)
            return

        # --- archivos estaticos
        nombre = "index.html" if ruta == "/" else ruta.lstrip("/")
        archivo = (ESTATICO / nombre).resolve()
        if not str(archivo).startswith(str(ESTATICO.resolve())) or not archivo.is_file():
            self._responder(404, b"No encontrado", "text/plain; charset=utf-8")
            return
        tipo = mimetypes.guess_type(archivo.name)[0] or "application/octet-stream"
        if tipo.startswith("text/") or tipo == "application/javascript":
            tipo += "; charset=utf-8"
        self._responder(200, archivo.read_bytes(), tipo)

    def do_POST(self) -> None:  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/api/universal/subir":
            self._subir(q)
            return
        if u.path == "/api/universal/ejecutar":
            cid = (q.get("id") or [""])[0]
            if not _id_valido(cid) or cid not in UNIV:
                self._json({"ok": False, "mensaje": "corrida inexistente"}, 404)
                return
            if UNIV[cid]["activo"]:
                self._json({"ok": False, "mensaje": "ya se esta ejecutando"}, 409)
                return
            threading.Thread(target=univ_ejecutar, args=(cid,), daemon=True).start()
            self._json({"ok": True})
            return
        if u.path != "/api/ejecutar":
            self._responder(404, b"No encontrado", "text/plain; charset=utf-8")
            return
        with CANDADO:
            if EJECUCION["activo"]:
                self._json({"ok": False, "mensaje": "Ya hay una ejecucion en curso"}, 409)
                return
        threading.Thread(target=ejecutar_pipeline, daemon=True).start()
        self._json({"ok": True, "mensaje": "Pipeline iniciado"})


def _subir_impl(self, q) -> None:
    """Recibe el archivo como cuerpo crudo (sin multipart) y lo escribe a disco por bloques."""
    try:
        largo = int(self.headers.get("Content-Length") or 0)
    except ValueError:
        largo = 0
    nombre = Path(_unquote(self.headers.get("X-Filename") or "archivo.csv")).name
    nombre = _re.sub(r"[^\w.\- ]", "_", nombre)[:120] or "archivo.csv"
    ext = Path(nombre).suffix.lower()
    if largo <= 0:
        self._json({"ok": False, "mensaje": "archivo vacio"}, 400)
        return
    if largo > MAX_SUBIDA:
        self._json({"ok": False, "mensaje": "archivo mayor a 2 GB"}, 413)
        return
    if ext not in EXT_OK:
        nombre += ".csv"            # se intentara leer como texto; el motor decide
    cid = (q.get("id") or [""])[0]
    if not _id_valido(cid) or cid not in UNIV:
        cid = _uuid.uuid4().hex[:12]
        UNIV[cid] = {"activo": False, "lineas": [], "ok": None, "inicio": None,
                     "fin": None, "archivos": []}
    destino = CORRIDAS / cid / "in" / nombre
    destino.parent.mkdir(parents=True, exist_ok=True)
    restante = largo
    with open(destino, "wb") as f:
        while restante > 0:
            bloque = self.rfile.read(min(restante, 1 << 20))
            if not bloque:
                break
            f.write(bloque)
            restante -= len(bloque)
    UNIV[cid]["archivos"].append(nombre)
    self._json({"ok": True, "id": cid, "archivo": nombre, "bytes": largo - restante})


Manejador._subir = _subir_impl


def main() -> None:
    ap = argparse.ArgumentParser(description="Panel de control del pipeline ETL")
    ap.add_argument("--puerto", type=int, default=8000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--sin-navegador", action="store_true")
    a = ap.parse_args()

    puerto = a.puerto
    for intento in range(12):      # si el puerto esta ocupado, prueba el siguiente
        try:
            srv = ThreadingHTTPServer((a.host, puerto), Manejador)
            break
        except OSError:
            puerto += 1
    else:
        print(f"[error] no se encontro puerto libre entre {a.puerto} y {puerto}")
        sys.exit(1)

    url = f"http://{a.host}:{puerto}"
    print("=" * 70)
    print("  PANEL DE CONTROL - PIPELINE ETL")
    print("  Ingenieria de Datos (ATD-2602) | TecNM Campus Colima")
    print("=" * 70)
    print(f"  Abre en el navegador:  {url}")
    print(f"  Base de datos:         {'encontrada' if BD.exists() else 'aun no generada'}")
    print("  Detener:               Ctrl+C")
    print("=" * 70)

    if not a.sin_navegador and not os.environ.get("SIN_NAVEGADOR"):
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  Servidor detenido.")
        srv.shutdown()


if __name__ == "__main__":
    main()
