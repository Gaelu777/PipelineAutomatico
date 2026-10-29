"""
Motor universal del pipeline ETL.

Aplica las 7 fases del proyecto a CUALQUIER dataset, sin conocer de antemano sus
columnas. Es lo que usa el panel cuando el profesor sube su propio archivo.

Formatos: CSV, TSV, TXT (detecta separador y codificacion), Excel (.xlsx, .xlsm,
.xls, todas las hojas), JSON (lista, objeto envoltorio o JSON Lines), Parquet y
ZIP con cualquiera de los anteriores.

Principio: NINGUNA entrada puede tumbar el motor. Cada fase va protegida; si una
falla, se registra, se continua con lo que se tenga y el reporte lo dice. Los
datos originales nunca se modifican: todo se escribe en una carpeta de corrida.

Uso por consola:
    python motor/pipeline_universal.py <archivo_o_carpeta> [carpeta_salida]
"""

from __future__ import annotations

import csv
import io
import json
import math
import re
import sqlite3
import sys
import time
import traceback
import unicodedata
import warnings
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ------------------------------------------------------------ parametros
TC_USD_MXN = 19.50
FACTOR_IQR = 1.5
MAX_COLUMNAS = 500
MAX_CATEGORIAS_CHI2 = 15
UMBRAL_COLUMNA_VACIA = 0.60          # mas de 60% nulos: no se imputa, se reporta
LOTE_VALIDACION = 50_000
VACIOS = {"", "na", "n/a", "nan", "null", "none", "nil", "-", "--", "?", "sin dato",
          "sin_dato", "s/d", "nd", "n.d.", "#n/a", "#value!", "#ref!", "undefined", "vacio"}
RE_EMAIL = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")
PISTAS_IMPORTE = ("monto", "total", "importe", "amount", "precio", "price", "venta",
                  "costo", "cost", "pago", "saldo", "subtotal", "valor", "value", "ingreso")
PISTAS_NO_NEGATIVO = PISTAS_IMPORTE + ("cantidad", "qty", "quantity", "unidades", "units",
                                        "piezas", "edad", "age", "stock")
PISTAS_MONEDA = ("moneda", "currency", "divisa")
PISTAS_FECHA = ("fecha", "date", "time", "timestamp", "dia", "created", "updated", "fec")
FORMATOS_FECHA = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y",
                  "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S",
                  "%Y-%m-%dT%H:%M:%S", "%d.%m.%Y", "%Y%m%d"]


def sanear_json(o):
    """NaN e infinito no son JSON valido: el navegador rechaza toda la respuesta."""
    if isinstance(o, float):
        return None if (o != o or o in (float("inf"), float("-inf"))) else o
    if isinstance(o, dict):
        return {str(k): sanear_json(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [sanear_json(v) for v in o]
    if hasattr(o, "item"):                    # numpy escalar
        try:
            return sanear_json(o.item())
        except Exception:  # noqa: BLE001
            return str(o)
    return o


# ================================================================ bitacora
class Bitacora:
    def __init__(self, avisar: Callable[[str], None] | None = None):
        self.eventos: list[dict] = []
        self.avisar = avisar or (lambda s: print(s, flush=True))
        self.t0 = time.perf_counter()

    def __call__(self, fase: str, mensaje: str, nivel: str = "info", **datos):
        ev = {"t": round(time.perf_counter() - self.t0, 2), "fase": fase,
              "nivel": nivel, "mensaje": mensaje, **datos}
        self.eventos.append(ev)
        marca = {"info": " ", "ok": "+", "aviso": "!", "error": "x"}.get(nivel, " ")
        self.avisar(f"  [{marca}] F{fase:<3} {mensaje}")


def protegido(fase: str, log: Bitacora, por_defecto=None):
    """Decorador: si la funcion falla, se registra y se devuelve el valor por defecto."""
    def deco(fn):
        def envoltura(*a, **k):
            try:
                return fn(*a, **k)
            except Exception as e:  # noqa: BLE001
                log(fase, f"{fn.__name__} fallo y se omitio: {type(e).__name__}: {e}", "error",
                    traza=traceback.format_exc(limit=3))
                return por_defecto(*a, **k) if callable(por_defecto) else por_defecto
        return envoltura
    return deco


# =============================================================== utilidades
def normalizar_nombre(s: Any, i: int) -> str:
    s = "" if s is None or (isinstance(s, float) and math.isnan(s)) else str(s)
    s = unicodedata.normalize("NFKD", s.strip())
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    if not s or s.startswith("unnamed"):
        s = f"columna_{i+1}"
    if s[0].isdigit():
        s = f"c_{s}"
    return s[:60]


def nombres_unicos(cols) -> list[str]:
    vistos: dict[str, int] = {}
    out = []
    for i, c in enumerate(cols):
        n = normalizar_nombre(c, i)
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        out.append(n)
    return out


def tiene(nombre: str, pistas) -> bool:
    return any(p in nombre for p in pistas)


def a_numero(s: pd.Series) -> pd.Series:
    """Convierte texto a numero tolerando $, %, espacios, miles con coma y coma decimal."""
    t = s.astype("string").str.strip()
    t = t.str.replace(r"[\$€£\s]|mxn|usd|eur", "", regex=True, case=False)
    t = t.str.replace("%", "", regex=False)
    # Coma decimal se decide POR COLUMNA, no por valor: "123,45" es ambiguo solo,
    # pero si la mayoria de la columna tiene forma 1.234,56 o 123,45, la coma es decimal.
    nn = t.dropna()
    coma_dec = nn.str.match(r"^-?\d{1,3}(\.\d{3})*,\d{1,2}$|^-?\d+,\d{1,2}$", na=False).mean() \
        if len(nn) else 0
    punto_dec = nn.str.match(r"^-?\d+(,\d{3})*\.\d+$", na=False).mean() if len(nn) else 0
    if coma_dec > 0.5 and coma_dec > punto_dec:
        t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    else:
        t = t.str.replace(",", "", regex=False)
    t = t.str.replace(r"^\((.*)\)$", r"-\1", regex=True)    # (500) contable -> -500
    # float64 de numpy y no Float64 nullable: las comparaciones con NaN dan False
    # en vez de pd.NA, que hace fallar cualquier `if` ("boolean value of NA is ambiguous").
    return pd.to_numeric(t, errors="coerce").astype("float64")


def a_fecha(s: pd.Series) -> tuple[pd.Series, str]:
    """Prueba formatos explicitos y epoch; nunca deja que pandas adivine el orden dia/mes."""
    original = s
    t = s.astype("string").str.strip()
    resultado = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    usados = []

    # epoch numerico (segundos o milisegundos)
    num = pd.to_numeric(t, errors="coerce").astype("float64")
    es_epoch = num.between(5e8, 4.2e9) | num.between(5e11, 4.2e12)
    if es_epoch.mean() > 0.05:
        seg = num.where(num < 1e11, num / 1000)
        conv = pd.to_datetime(seg, unit="s", errors="coerce", utc=True)
        conv = conv.dt.tz_convert("America/Mexico_City").dt.tz_localize(None)
        resultado = resultado.where(~es_epoch, conv)
        usados.append("epoch")

    pendiente = resultado.isna() & t.notna()
    for fmt in FORMATOS_FECHA:
        if not pendiente.any():
            break
        conv = pd.to_datetime(t[pendiente], format=fmt, errors="coerce")
        ok = conv.notna()
        if ok.any():
            resultado.loc[conv[ok].index] = conv[ok]
            usados.append(fmt)
            pendiente = resultado.isna() & t.notna()

    if pendiente.any():   # ISO con zona horaria u otros formatos estandar
        conv = pd.to_datetime(t[pendiente], errors="coerce", utc=True, format="ISO8601")
        ok = conv.notna()
        if ok.any():
            conv = conv[ok].dt.tz_convert("America/Mexico_City").dt.tz_localize(None)
            resultado.loc[conv.index] = conv
            usados.append("ISO8601")
    _ = original
    return resultado, ", ".join(usados) or "ninguno"


# ================================================================== lectura
def leer_csv(datos: bytes, nombre: str, log: Bitacora) -> pd.DataFrame:
    texto, cod = None, None
    for c in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            texto, cod = datos.decode(c), c
            break
        except UnicodeDecodeError:
            continue
    if texto is None:
        raise ValueError("no se pudo decodificar el texto")
    if "\x00" in texto[:4096]:
        raise ValueError("el archivo parece binario, no es un CSV de texto")

    muestra = texto[:65536]
    sep = ","
    try:
        sep = csv.Sniffer().sniff(muestra, delimiters=",;\t|").delimiter
    except csv.Error:
        conteos = {d: muestra.count(d) for d in (",", ";", "\t", "|")}
        sep = max(conteos, key=conteos.get)

    malas: list[int] = []

    def mala(linea):
        malas.append(len(linea))
        return None

    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str, keep_default_na=False,
                     engine="python", on_bad_lines=mala, skip_blank_lines=True)
    log("0", f"{nombre}: codificacion {cod}, separador {repr(sep)}", "info")
    if malas:
        log("0", f"{nombre}: {len(malas)} lineas con columnas de mas se omitieron", "aviso")
    return df


def leer_json(datos: bytes, nombre: str, log: Bitacora) -> pd.DataFrame:
    texto = datos.decode("utf-8-sig", errors="replace").strip()
    try:
        obj = json.loads(texto)
    except json.JSONDecodeError:
        filas = []
        for i, linea in enumerate(texto.splitlines()):
            linea = linea.strip().rstrip(",")
            if not linea:
                continue
            try:
                filas.append(json.loads(linea))
            except json.JSONDecodeError:
                pass
        if not filas:
            raise ValueError("JSON invalido y tampoco es JSON Lines")
        log("0", f"{nombre}: leido como JSON Lines ({len(filas)} lineas)", "info")
        obj = filas
    if isinstance(obj, dict):
        lista = next((v for v in obj.values() if isinstance(v, list) and v
                      and isinstance(v[0], dict)), None)
        obj = lista if lista is not None else [obj]
    if not isinstance(obj, list):
        raise ValueError("el JSON no contiene una lista de registros")
    df = pd.json_normalize([x if isinstance(x, dict) else {"valor": x} for x in obj],
                           max_level=2, sep="_")
    # listas anidadas -> texto para que no rompan las fases siguientes
    for c in df.columns:
        if df[c].map(lambda v: isinstance(v, (list, dict))).any():
            df[c] = df[c].map(lambda v: json.dumps(v, ensure_ascii=False)
                              if isinstance(v, (list, dict)) else v)
    return df.astype(object).where(df.notna(), None).astype("string").fillna("")


def leer_excel(datos: bytes, nombre: str, log: Bitacora) -> dict[str, pd.DataFrame]:
    motor = "xlrd" if nombre.lower().endswith(".xls") else "openpyxl"
    try:
        hojas = pd.read_excel(io.BytesIO(datos), sheet_name=None, dtype=str, engine=motor)
    except ImportError:
        raise ValueError(f"falta la libreria '{motor}' para leer Excel. "
                         f"Instala con: pip install {motor}  (o guarda el archivo como CSV)")
    out = {}
    for h, df in hojas.items():
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if len(df):
            out[f"{Path(nombre).stem}__{normalizar_nombre(h, 0)}"] = df.fillna("")
    log("0", f"{nombre}: {len(hojas)} hojas, {len(out)} con datos", "info")
    return out


def leer_archivo(ruta: Path, log: Bitacora) -> dict[str, pd.DataFrame]:
    """Devuelve {nombre_fuente: DataFrame de texto}. Nunca lanza: devuelve {} si no se puede."""
    ext = ruta.suffix.lower()
    try:
        datos = ruta.read_bytes()
    except Exception as e:  # noqa: BLE001
        log("0", f"{ruta.name}: no se pudo leer ({e})", "error")
        return {}
    if not datos.strip():
        log("0", f"{ruta.name}: archivo vacio", "aviso")
        return {}
    nombre = ruta.stem
    try:
        if ext == ".zip" or datos[:2] == b"PK" and ext not in (".xlsx", ".xlsm"):
            if ext != ".zip":
                pass
            out = {}
            with zipfile.ZipFile(io.BytesIO(datos)) as z:
                for m in z.infolist():
                    if m.is_dir() or m.file_size > 2_000_000_000 or "/." in m.filename \
                            or m.filename.startswith(("__MACOSX", ".")):
                        continue
                    destino = ruta.parent / "_zip" / Path(m.filename).name
                    destino.parent.mkdir(parents=True, exist_ok=True)
                    destino.write_bytes(z.read(m))
                    out.update(leer_archivo(destino, log))
            return out
        if ext in (".xlsx", ".xlsm", ".xls"):
            return leer_excel(datos, ruta.name, log)
        if ext in (".json", ".jsonl", ".ndjson") or datos.lstrip()[:1] in (b"[", b"{"):
            return {nombre: leer_json(datos, ruta.name, log)}
        if ext == ".parquet":
            return {nombre: pd.read_parquet(io.BytesIO(datos)).astype("string").fillna("")}
        return {nombre: leer_csv(datos, ruta.name, log)}
    except Exception as e:  # noqa: BLE001
        log("0", f"{ruta.name}: no se pudo interpretar ({type(e).__name__}: {e})", "error")
        return {}


# ======================================================= fases por fuente
def fase0_estructura(df: pd.DataFrame, nombre: str, log: Bitacora) -> tuple[pd.DataFrame, dict]:
    """Nombres limpios y unicos, filas/columnas vacias fuera, vacios ocultos a nulo."""
    info = {"filas_crudas": len(df), "columnas_crudas": df.shape[1]}
    if df.shape[1] > MAX_COLUMNAS:
        log("0", f"{nombre}: {df.shape[1]} columnas, se conservan las primeras {MAX_COLUMNAS}",
            "aviso")
        df = df.iloc[:, :MAX_COLUMNAS]
    originales = list(df.columns)
    df.columns = nombres_unicos(originales)
    info["mapeo_columnas"] = dict(zip(originales, df.columns))

    df = df.astype("string")
    for c in df.columns:
        s = df[c].str.strip()
        ocultos = s.str.lower().isin(VACIOS)
        df[c] = s.mask(ocultos)
    antes = len(df)
    df = df.dropna(how="all")
    vacias = [c for c in df.columns if df[c].isna().all()]
    df = df.drop(columns=vacias)
    info.update(filas_vacias_quitadas=antes - len(df), columnas_vacias_quitadas=vacias)
    log("0", f"{nombre}: {len(df):,} filas x {df.shape[1]} columnas tras normalizar estructura",
        "ok")
    return df.reset_index(drop=True), info


def inferir_tipos(df: pd.DataFrame) -> dict[str, str]:
    """numerico | fecha | correo | booleano | id | categorico | texto"""
    tipos = {}
    n = max(len(df), 1)
    for c in df.columns:
        s = df[c].dropna()
        if s.empty:
            tipos[c] = "texto"
            continue
        muestra = s.sample(min(len(s), 5000), random_state=1) if len(s) > 5000 else s
        num = a_numero(muestra)
        r_num = num.notna().mean()
        r_mail = muestra.str.contains("@", regex=False).mean()
        bools = muestra.str.lower().isin({"true", "false", "si", "no", "sí", "1", "0",
                                         "verdadero", "falso", "yes"}).mean()
        unicos = s.nunique()
        if r_mail > 0.5:
            tipos[c] = "correo"
        elif bools > 0.95 and unicos <= 4:
            tipos[c] = "booleano"
        elif r_num > 0.9:
            entero = (num.dropna() % 1 == 0).all()
            es_id = entero and unicos > 0.95 * len(s) and (tiene(c, ("id", "folio", "clave",
                                                                     "codigo", "num")) or
                                                          muestra.str.match(r"^0\d+").mean() > .3)
            es_fecha_epoch = tiene(c, PISTAS_FECHA) and num.between(5e8, 4.2e12).mean() > .8
            tipos[c] = "fecha" if es_fecha_epoch else ("id" if es_id else "numerico")
        else:
            fechas, _ = a_fecha(muestra)
            if fechas.notna().mean() > 0.8 or (tiene(c, PISTAS_FECHA) and fechas.notna().mean() > .5):
                tipos[c] = "fecha"
            elif unicos > 0.9 * len(s) and len(s) > 20:
                tipos[c] = "id" if muestra.str.len().median() < 40 else "texto"
            elif unicos <= max(50, 0.05 * n):
                tipos[c] = "categorico"
            else:
                tipos[c] = "texto"
    return tipos


def fase1_perfil(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora) -> dict:
    filas = []
    for c in df.columns:
        nulos = int(df[c].isna().sum())
        filas.append({"columna": c, "tipo": tipos.get(c), "nulos": nulos,
                      "pct_nulos": round(100 * nulos / max(len(df), 1), 2),
                      "unicos": int(df[c].nunique()),
                      "ejemplo": str(df[c].dropna().iloc[0])[:40] if df[c].notna().any() else ""})
    dup = int(df.duplicated().sum())
    completitud = round(100 - 100 * df.isna().sum().sum() / max(df.size, 1), 2)
    log("1", f"{nombre}: completitud {completitud}%, {dup:,} duplicados exactos", "ok")
    return {"columnas": filas, "duplicados_exactos": dup, "completitud_pct": completitud,
            "tipos": tipos}


def clasificar_faltante(df: pd.DataFrame, col: str) -> tuple[str, float | None, str | None]:
    """MCAR vs MAR con chi-cuadrada contra columnas categoricas de baja cardinalidad."""
    from scipy.stats import chi2_contingency
    falta = df[col].isna()
    if falta.sum() == 0 or falta.all():
        return "N/A", None, None
    mejor_p, mejor_c = 1.0, None
    for c in df.columns:
        if c == col:
            continue
        s = df[c]
        k = s.nunique()
        if k < 2 or k > MAX_CATEGORIAS_CHI2:
            continue
        tabla = pd.crosstab(s.fillna("_nulo_"), falta)
        if tabla.shape[1] < 2:
            continue
        try:
            p = chi2_contingency(tabla)[1]
        except ValueError:
            continue
        if p < mejor_p:
            mejor_p, mejor_c = p, c
    return ("MAR" if mejor_p < 0.05 else "MCAR"), round(mejor_p, 5), mejor_c


def columnas_aritmeticas(tipos: dict, cols) -> tuple[str | None, str | None, str | None]:
    """Detecta (cantidad, precio, total) por nombre entre las columnas numericas."""
    num_cols = [c for c, t in tipos.items() if t == "numerico" and c in cols]
    c_qty = next((c for c in num_cols if tiene(c, ("cantidad", "qty", "quantity", "units",
                                                   "unidades", "piezas"))), None)
    c_pre = next((c for c in num_cols if tiene(c, ("precio", "price", "unit_cost", "costo_u"))
                  and not tiene(c, ("total",))), None)
    c_tot = next((c for c in num_cols if tiene(c, ("total", "monto", "importe", "amount"))
                  and c != c_pre), None)
    return c_qty, c_pre, c_tot


def fase2_faltantes(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora
                    ) -> tuple[pd.DataFrame, list[dict]]:
    acciones = []
    df["campos_imputados"] = ""
    df["campos_recalculados"] = ""

    # Recalcular antes que imputar: si la informacion existe en otras columnas,
    # se reconstruye exacta en vez de estimarla con la mediana.
    c_qty, c_pre, c_tot = columnas_aritmeticas(tipos, df.columns)
    if c_qty and c_pre and c_tot:
        q, pr, tot = a_numero(df[c_qty]), a_numero(df[c_pre]), a_numero(df[c_tot])
        for col, valor in ((c_tot, q * pr), (c_pre, tot / q.where(q != 0)),
                           (c_qty, tot / pr.where(pr != 0))):
            m = df[col].isna() & valor.notna() & np.isfinite(valor)
            if m.any():
                df.loc[m, col] = valor[m].round(2).astype(str)
                df.loc[m, "campos_recalculados"] += col + ";"
                acciones.append({"columna": col, "tipo": "numerico", "faltantes": int(m.sum()),
                                 "pct": round(100 * m.mean(), 2), "clasificacion": "recalculable",
                                 "p_valor": None, "depende_de": f"{c_qty}, {c_pre}, {c_tot}",
                                 "estrategia": "recalculo exacto desde otras columnas"})
    for c, t in tipos.items():
        if c not in df.columns:
            continue
        m = df[c].isna()
        if not m.any():
            continue
        pct = m.mean()
        tipo_f, p, pred = clasificar_faltante(df, c) if len(df) <= 300_000 else ("sin_prueba", None, None)
        accion = {"columna": c, "tipo": t, "faltantes": int(m.sum()), "pct": round(100 * pct, 2),
                  "clasificacion": tipo_f, "p_valor": p, "depende_de": pred}
        if pct > UMBRAL_COLUMNA_VACIA:
            accion["estrategia"] = "no se imputa (mas de 60% vacia)"
        elif t == "numerico":
            num = a_numero(df[c])
            med = float(num.median()) if num.notna().any() else 0.0
            df[c] = df[c].mask(m, str(med))
            accion.update(estrategia="mediana", valor=med)
        elif t in ("categorico", "booleano", "texto"):
            df[c] = df[c].mask(m, "DESCONOCIDO")
            accion.update(estrategia="etiqueta controlada", valor="DESCONOCIDO")
        elif t == "correo":
            df[c] = df[c].mask(m, "ANONIMO")
            accion.update(estrategia="etiqueta controlada", valor="ANONIMO")
        else:
            accion["estrategia"] = "se conserva nulo (id o fecha: imputar inventaria el dato)"
        if "valor" in accion:
            df.loc[m, "campos_imputados"] = df.loc[m, "campos_imputados"] + c + ";"
        acciones.append(accion)
    log("2", f"{nombre}: {len(acciones)} columnas con faltantes tratadas", "ok")
    return df, acciones


def fase34_tipos_outliers_fechas(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora
                                 ) -> tuple[pd.DataFrame, dict]:
    info: dict[str, Any] = {"iqr": [], "fechas": [], "moneda": None, "imposibles": {}}
    df["flag_outlier"] = False
    df["motivo_previo"] = ""

    # --- moneda: USD a MXN antes del IQR para no mezclar escalas
    col_mon = next((c for c in df.columns if tiene(c, PISTAS_MONEDA)), None)
    usd = pd.Series(False, index=df.index)
    if col_mon:
        usd = df[col_mon].astype("string").str.upper().str.strip().isin(["USD", "US$", "DOLAR",
                                                                         "DÓLAR", "DOLARES"])
        info["moneda"] = {"columna": col_mon, "filas_usd": int(usd.sum())}

    # --- coherencia aritmetica: total = cantidad x precio (si las tres existen)
    c_qty, c_pre, c_tot = columnas_aritmeticas(tipos, df.columns)
    info["aritmetica"] = None
    if c_qty and c_pre and c_tot:
        q, pr, tot = a_numero(df[c_qty]), a_numero(df[c_pre]), a_numero(df[c_tot])
        esperado = (q * pr).round(2)
        imputado = (df["campos_imputados"] + df["campos_recalculados"]).str.contains(
            f"{c_tot};|{c_pre};|{c_qty};", regex=True)
        roto = (esperado - tot).abs().gt(0.011) & q.gt(0) & ~imputado & tot.notna()
        ratio = (tot / esperado).round(2)
        bot = roto & ratio.between(10, 1000) & (ratio % 1 == 0)
        df.loc[roto & ~bot, "motivo_previo"] += f"{c_tot} != {c_qty} x {c_pre}; "
        df.loc[bot, "motivo_previo"] += f"{c_tot} inflado x entero (patron de bot); "
        info["aritmetica"] = {"total": c_tot, "cantidad": c_qty, "precio": c_pre,
                              "inconsistentes": int(roto.sum()), "patron_bot": int(bot.sum())}

    for c, t in tipos.items():
        if c not in df.columns:
            continue
        if t == "numerico":
            num = a_numero(df[c])
            if usd.any() and tiene(c, PISTAS_IMPORTE):
                num = num.where(~usd, num * TC_USD_MXN)
            # imposibles: negativos o cero en columnas que no los admiten
            if tiene(c, PISTAS_NO_NEGATIVO):
                imp = num <= 0 if tiene(c, ("cantidad", "qty", "units", "unidades", "piezas")) \
                    else num < 0
                if imp.any():
                    df.loc[imp, "motivo_previo"] += f"{c} imposible ({'<=0' if 'cant' in c or 'qty' in c else '<0'}); "
                    info["imposibles"][c] = int(imp.sum())
            # IQR + capping (no se borra ninguna fila)
            serie = num[np.isfinite(num)] if num.notna().any() else num
            if serie.nunique() > 10:
                q1, q3 = serie.quantile([.25, .75])
                iqr = q3 - q1
                if iqr > 0:
                    lo, hi = q1 - FACTOR_IQR * iqr, q3 + FACTOR_IQR * iqr
                    fuera = (num < lo) | (num > hi)
                    df[f"{c}_original"] = num.where(fuera)
                    num = num.clip(lo, hi)
                    df.loc[fuera, "flag_outlier"] = True
                    info["iqr"].append({"columna": c, "q1": round(q1, 4), "q3": round(q3, 4),
                                        "limite_inf": round(lo, 4), "limite_sup": round(hi, 4),
                                        "recortados": int(fuera.sum())})
            df[c] = num
        elif t == "fecha":
            conv, usados = a_fecha(df[c])
            no_conv = df[c].notna() & conv.isna()
            df[f"{c}_original"] = df[c].where(no_conv)
            df[c] = conv.dt.strftime("%Y-%m-%d").where(conv.notna())
            if no_conv.any():
                df.loc[no_conv, "motivo_previo"] += f"{c} no convertible a fecha; "
            info["fechas"].append({"columna": c, "convertidas": int(conv.notna().sum()),
                                   "no_convertibles": int(no_conv.sum()), "formatos": usados})
        elif t in ("categorico", "booleano"):
            df[c] = df[c].astype("string").str.strip().str.upper()
    if usd.any():
        df[col_mon] = df[col_mon].where(~usd, "MXN")

    dups = df.drop(columns=["campos_imputados", "campos_recalculados", "flag_outlier",
                            "motivo_previo"],
                   errors="ignore").duplicated(keep="first")
    df.loc[dups, "motivo_previo"] += "duplicado exacto; "
    info["duplicados"] = int(dups.sum())
    log("3", f"{nombre}: {sum(x['recortados'] for x in info['iqr']):,} valores recortados por IQR "
        f"en {len(info['iqr'])} columnas", "ok")
    log("4", f"{nombre}: {len(info['fechas'])} columnas de fecha a ISO, "
        f"{int(usd.sum()):,} importes USD->MXN, {info['duplicados']:,} duplicados marcados", "ok")
    return df, info


def detectar_huerfanos(fuentes: dict[str, pd.DataFrame], tipos: dict[str, dict],
                       log: Bitacora) -> list[dict]:
    """Si una fuente tiene una llave unica que otra fuente usa, es su catalogo maestro."""
    relaciones = []
    for maestro, dm in fuentes.items():
        for c in dm.columns:
            if tipos[maestro].get(c) not in ("id", "categorico", "texto"):
                continue
            col = dm[c].dropna()
            if len(col) < 2 or not col.is_unique:
                continue
            llaves = set(col.astype(str).str.strip().str.upper())
            for hijo, dh in fuentes.items():
                if hijo == maestro:
                    continue
                # candidatas: mismo nombre, o cualquier columna de texto/id cuyos valores
                # coincidan con las llaves del maestro (product_id vs id_producto)
                cand = [x for x in dh.columns if tipos[hijo].get(x) in ("id", "categorico", "texto")
                        and (x == c or dh[x].dropna().astype(str).str.strip().str.upper()
                             .head(2000).isin(llaves).mean() >= 0.5)]
                for h in cand:
                    v = dh[h].dropna().astype(str).str.strip().str.upper()
                    if v.empty or v.isin(llaves).mean() < 0.5:   # no es la misma entidad
                        continue
                    huerf = dh[h].notna() & ~dh[h].astype(str).str.strip().str.upper().isin(llaves)
                    dh.loc[huerf, "motivo_previo"] += f"llave huerfana: {h} no existe en {maestro}; "
                    relaciones.append({"maestro": maestro, "llave": c, "hija": hijo,
                                       "columna": h, "huerfanos": int(huerf.sum())})
                    log("4", f"{hijo}.{h} -> {maestro}.{c}: {int(huerf.sum()):,} huerfanos", "ok")
    return relaciones


def fase5_escalar(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora
                  ) -> tuple[pd.DataFrame, dict]:
    info: dict[str, Any] = {"minmax": [], "discretizacion": None}
    numericas = [c for c, t in tipos.items() if t == "numerico" and c in df.columns]
    for c in numericas:
        s = pd.to_numeric(df[c], errors="coerce")
        lo, hi = s.min(), s.max()
        if pd.isna(lo) or hi == lo:
            continue
        df[f"{c}_norm"] = ((s - lo) / (hi - lo)).clip(0, 1).round(6)
        info["minmax"].append({"columna": c, "min": float(lo), "max": float(hi)})

    prioridad = ("total", "monto", "importe", "amount", "venta", "saldo", "valor", "precio", "price")
    objetivo = next((c for p in prioridad for c in numericas if p in c), None)
    if objetivo is None and numericas:
        objetivo = max(numericas, key=lambda c: pd.to_numeric(df[c], errors="coerce").std() or 0)
    if objetivo:
        s = pd.to_numeric(df[objetivo], errors="coerce")
        try:
            seg, cortes = pd.qcut(s, 3, labels=["BAJO", "MEDIO", "ALTO"], retbins=True,
                                  duplicates="drop")
            if len(cortes) < 4:
                raise ValueError
        except (ValueError, IndexError):
            seg, cortes = pd.cut(s, 3, labels=["BAJO", "MEDIO", "ALTO"], retbins=True)
        df["segmento"] = seg.astype("string")
        info["discretizacion"] = {"columna": objetivo,
                                  "cortes": [round(float(x), 4) for x in cortes],
                                  "reparto": df["segmento"].value_counts().to_dict()}
        log("5", f"{nombre}: Min-Max en {len(info['minmax'])} columnas; '{objetivo}' en "
            f"BAJO/MEDIO/ALTO por terciles", "ok")
    else:
        log("5", f"{nombre}: sin columnas numericas que escalar", "aviso")
    return df, info


def fase6_validar(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora
                  ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Esquema Pydantic construido en tiempo de ejecucion a partir de los tipos inferidos.
    Se valida por lotes con TypeAdapter (nucleo en Rust de Pydantic v2), asi que
    escala a millones de filas; cada fila mala se identifica por su indice.
    """
    from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, \
        create_model, field_validator

    campos: dict[str, Any] = {}
    validadores: dict[str, Any] = {}
    hoy = datetime.now().year + 1

    for c, t in tipos.items():
        if c not in df.columns:
            continue
        if t == "numerico":
            campos[c] = (float | None, Field(default=None, allow_inf_nan=False))
        elif t == "fecha":
            campos[c] = (str | None, None)
        else:
            campos[c] = (str | None, None)
        if f"{c}_norm" in df.columns:
            campos[f"{c}_norm"] = (float | None, Field(default=None, ge=0, le=1))

    correos = [c for c, t in tipos.items() if t == "correo" and c in df.columns]
    fechas = [c for c, t in tipos.items() if t == "fecha" and c in df.columns]
    campos["motivo_previo"] = (str | None, None)

    if correos:
        @field_validator(*correos)
        @classmethod
        def correo_valido(cls, v):  # noqa: N805
            if v is None or v == "ANONIMO":
                return v
            v = v.strip().lower()
            if "@@" in v or ".." in v or not RE_EMAIL.match(v):
                raise ValueError(f"correo invalido: '{v}'")
            return v
        validadores["correo_valido"] = correo_valido

    if fechas:
        @field_validator(*fechas)
        @classmethod
        def fecha_en_rango(cls, v):  # noqa: N805
            if v is None:
                return v
            anio = int(v[:4])
            if not (1900 <= anio <= hoy):
                raise ValueError(f"fecha fuera de rango plausible: {v}")
            return v
        validadores["fecha_en_rango"] = fecha_en_rango

    @field_validator("motivo_previo")
    @classmethod
    def sin_motivo_previo(cls, v):  # noqa: N805
        if v and v.strip():
            raise ValueError(v.strip().rstrip(";"))
        return v
    validadores["sin_motivo_previo"] = sin_motivo_previo

    Modelo = create_model("RegistroValido", __config__=ConfigDict(extra="ignore",
                          str_strip_whitespace=True), __validators__=validadores, **campos)
    adaptador = TypeAdapter(list[Modelo])

    registros = df[list(campos)].astype(object).where(df[list(campos)].notna(), None)
    malos: dict[int, list[str]] = {}
    for ini in range(0, len(registros), LOTE_VALIDACION):
        lote = registros.iloc[ini:ini + LOTE_VALIDACION].to_dict(orient="records")
        try:
            adaptador.validate_python(lote)
        except ValidationError as e:
            for err in e.errors():
                if not err["loc"]:
                    continue
                i = ini + int(err["loc"][0])
                campo = err["loc"][1] if len(err["loc"]) > 1 else "fila"
                msg = err["msg"].replace("Value error, ", "")
                malos.setdefault(i, []).append(msg if campo == "motivo_previo"
                                               else f"{campo}: {msg}")

    mascara = df.index.isin(list(malos))
    limpias = df[~mascara].drop(columns=["motivo_previo"], errors="ignore").copy()
    cuar = df[mascara].copy()
    cuar["motivo_rechazo_cuarentena"] = [" | ".join(malos[i]) for i in cuar.index]
    cuar["causa_principal"] = cuar["motivo_rechazo_cuarentena"].map(etiqueta_causa)
    cuar = cuar.drop(columns=["motivo_previo"], errors="ignore")

    resumen = cuar["causa_principal"].value_counts().to_dict() if len(cuar) else {}
    log("6", f"{nombre}: {len(limpias):,} validas, {len(cuar):,} a cuarentena "
        f"({100*len(cuar)/max(len(df),1):.2f}%)", "ok")
    return limpias, cuar, {"validas": len(limpias), "cuarentena": len(cuar), "causas": resumen,
                           "campos_esquema": list(campos)}


def etiqueta_causa(m: str) -> str:
    m = m.lower()
    for clave, etq in [("huerfana", "Llave huerfana"), ("bot", "Total inflado (bot)"),
                       (" != ", "Total distinto de cantidad x precio"), ("correo", "Correo invalido"),
                       ("imposible", "Valor imposible"), ("duplicado", "Duplicado exacto"),
                       ("no convertible", "Fecha no convertible"),
                       ("fuera de rango", "Fecha fuera de rango"),
                       ("finite", "Numero no finito"), ("less than or equal", "Normalizado fuera de [0,1]")]:
        if clave in m:
            return etq
    return "Otro error de esquema"


def fase7_cargar(fuentes: dict[str, tuple[pd.DataFrame, pd.DataFrame]], bd: Path,
                 log: Bitacora) -> dict:
    """Una transaccion por tabla; rollback ante cualquier error; prueba de rollback incluida."""
    con = sqlite3.connect(bd)
    resultado = {}
    try:
        for nombre, (ok, q) in fuentes.items():
            base = normalizar_nombre(nombre, 0)
            for sufijo, tabla in (("limpias", ok), ("cuarentena", q)):
                nt = f"{base}_{sufijo}"
                try:
                    con.execute("BEGIN")
                    con.execute(f'DROP TABLE IF EXISTS "{nt}"')
                    tabla.astype(object).where(tabla.notna(), None).to_sql(
                        nt, con, index=False, if_exists="replace", chunksize=20_000)
                    con.commit()
                    resultado[nt] = len(tabla)
                except Exception as e:  # noqa: BLE001
                    con.rollback()
                    log("7", f"{nt}: rollback ({type(e).__name__}: {e})", "error")
        # prueba de atomicidad real
        con.execute("CREATE TABLE IF NOT EXISTS _prueba_rollback (x INTEGER CHECK (x > 0))")
        antes = con.execute("SELECT COUNT(*) FROM _prueba_rollback").fetchone()[0]
        try:
            con.execute("BEGIN")
            con.execute("INSERT INTO _prueba_rollback VALUES (1)")
            con.execute("INSERT INTO _prueba_rollback VALUES (-1)")
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
        despues = con.execute("SELECT COUNT(*) FROM _prueba_rollback").fetchone()[0]
        resultado["_rollback_verificado"] = antes == despues
        log("7", f"{sum(v for k, v in resultado.items() if not k.startswith('_')):,} filas en "
            f"{len([k for k in resultado if not k.startswith('_')])} tablas; rollback verificado: "
            f"{antes == despues}", "ok")
    finally:
        con.close()
    return resultado


# ================================================================ orquestador
def procesar_dataframe(df: pd.DataFrame, nombre: str, log: Bitacora) -> dict:
    df, est = fase0_estructura(df, nombre, log)
    if df.empty:
        log("0", f"{nombre}: sin filas utiles tras limpiar la estructura", "aviso")
        return {"nombre": nombre, "vacio": True, "estructura": est}
    tipos = inferir_tipos(df)
    perfil = fase1_perfil(df, tipos, nombre, log)
    df, faltantes = fase2_faltantes(df, tipos, nombre, log)
    df, f34 = fase34_tipos_outliers_fechas(df, tipos, nombre, log)
    return {"nombre": nombre, "df": df, "tipos": tipos, "estructura": est, "perfil": perfil,
            "faltantes": faltantes, "fase34": f34}


def ejecutar(entradas: list[Path], salida: Path, avisar: Callable[[str], None] | None = None
             ) -> dict:
    salida.mkdir(parents=True, exist_ok=True)
    log = Bitacora(avisar)
    reporte: dict[str, Any] = {"inicio": datetime.now().isoformat(timespec="seconds"),
                               "fuentes": {}, "ok": False}
    try:
        crudas: dict[str, pd.DataFrame] = {}
        for p in entradas:
            for k, v in leer_archivo(p, log).items():
                clave = normalizar_nombre(k, 0)
                while clave in crudas:
                    clave += "_b"
                crudas[clave] = v
                log("0", f"fuente '{clave}': {len(v):,} filas leidas", "ok")
        if not crudas:
            raise ValueError("ningun archivo pudo leerse como tabla de datos")

        etapa: dict[str, dict] = {}
        for k, df in crudas.items():
            try:
                etapa[k] = procesar_dataframe(df, k, log)
            except Exception as e:  # noqa: BLE001
                log("*", f"{k}: fallo en fases 0-4 ({type(e).__name__}: {e}); fuente omitida",
                    "error", traza=traceback.format_exc(limit=4))

        vivas = {k: v for k, v in etapa.items() if not v.get("vacio") and "df" in v}
        try:
            reporte["relaciones"] = detectar_huerfanos(
                {k: v["df"] for k, v in vivas.items()}, {k: v["tipos"] for k, v in vivas.items()},
                log) if len(vivas) > 1 else []
        except Exception as e:  # noqa: BLE001
            log("4", f"deteccion de huerfanos omitida ({e})", "error")
            reporte["relaciones"] = []

        cargas = {}
        for k, v in vivas.items():
            try:
                df, f5 = fase5_escalar(v["df"], v["tipos"], k, log)
                ok, q, f6 = fase6_validar(df, v["tipos"], k, log)
            except Exception as e:  # noqa: BLE001
                log("6", f"{k}: validacion fallo ({type(e).__name__}: {e}); todo a cuarentena",
                    "error")
                ok, q = v["df"].iloc[0:0], v["df"].assign(motivo_rechazo_cuarentena=str(e),
                                                         causa_principal="Error del motor")
                f5, f6 = {}, {"validas": 0, "cuarentena": len(q)}
            ok.to_csv(salida / f"{k}_limpias.csv", index=False, encoding="utf-8-sig")
            q.to_csv(salida / f"{k}_cuarentena.csv", index=False, encoding="utf-8-sig")
            cargas[k] = (ok, q)
            reporte["fuentes"][k] = {
                "estructura": v["estructura"], "perfil": v["perfil"],
                "faltantes": v["faltantes"], "fase34": v["fase34"], "fase5": f5, "fase6": f6,
                "muestra_limpias": ok.head(25).astype(str).to_dict(orient="records"),
                "muestra_cuarentena": q.head(50)[[c for c in ("motivo_rechazo_cuarentena",
                    "causa_principal") if c in q] + [c for c in q.columns[:6]]].astype(str)
                    .to_dict(orient="records") if len(q) else [],
            }
        for k, v in etapa.items():
            if v.get("vacio"):
                reporte["fuentes"][k] = {"vacio": True, "estructura": v["estructura"]}

        reporte["bd"] = fase7_cargar(cargas, salida / "resultado.db", log)
        tot_ok = sum(len(a) for a, _ in cargas.values())
        tot_q = sum(len(b) for _, b in cargas.values())
        reporte["totales"] = {"entrada": tot_ok + tot_q, "limpias": tot_ok, "cuarentena": tot_q,
                              "tasa_aceptacion": round(100 * tot_ok / max(tot_ok + tot_q, 1), 2)}
        reporte["ok"] = True
        log("*", f"LISTO: {tot_ok:,} limpias + {tot_q:,} en cuarentena = {tot_ok+tot_q:,}", "ok")
    except Exception as e:  # noqa: BLE001
        reporte["error"] = f"{type(e).__name__}: {e}"
        log("*", f"el motor se detuvo de forma controlada: {reporte['error']}", "error")
    finally:
        reporte["fin"] = datetime.now().isoformat(timespec="seconds")
        reporte["segundos"] = round(time.perf_counter() - log.t0, 2)
        reporte["bitacora"] = log.eventos
        (salida / "reporte.json").write_text(
            json.dumps(sanear_json(reporte), ensure_ascii=False, indent=1, default=str,
                       allow_nan=False), encoding="utf-8")
    return reporte


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    origen = Path(sys.argv[1])
    destino = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("salida_universal")
    archivos = sorted(p for p in origen.iterdir() if p.is_file() and not p.name.startswith("_")) \
        if origen.is_dir() else [origen]
    r = ejecutar(archivos, destino)
    sys.exit(0 if r.get("ok") else 1)
