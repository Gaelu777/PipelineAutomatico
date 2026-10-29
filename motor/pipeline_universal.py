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

Archivos enormes (mas de motor.bloques.umbral_mb en config.yaml, 256 MB por defecto):
CSV, TSV, TXT y JSON Lines se procesan POR BLOQUES (chunksize) en tres pasadas, con
memoria acotada sin importar el tamano del archivo (probado con mas de 5 GB):
  1. estructura: nulos, duplicados (hash de 64 bits por fila), muestra para tipos;
  2. distribuciones: medianas, cuartiles, minimos y maximos globales, tablas chi2;
  3. transformacion: aplica las fases 2 a 7 bloque por bloque con los parametros
     GLOBALES de las pasadas 1 y 2, y escribe CSV y SQLite de forma incremental.
Los cuantiles son exactos hasta motor.bloques.valores_muestra_cuantiles valores por
columna; por encima se estiman con una muestra aleatoria uniforme de ese tamano.

Los umbrales (tipo de cambio, IQR, cuantiles de segmentacion...) se leen de config.yaml.

Uso por consola:
    python motor/pipeline_universal.py <archivo_o_carpeta> [carpeta_salida]
    python motor/pipeline_universal.py <archivo> <salida> --umbral-mb 0.5   # forzar bloques
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import shutil
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

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
from configuracion import Configuracion, ErrorConfiguracion  # noqa: E402
from configuracion import cargar as _cargar_config  # noqa: E402

warnings.filterwarnings("ignore")

# El modulo csv (motor "python" de pandas) descarta EN SILENCIO cualquier fila con un campo
# de mas de 128 KB. Eso viola la regla de no perder filas: se sube el limite al maximo.
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:                  # en Windows el limite es un long de 32 bits
    csv.field_size_limit(2**31 - 1)

# ------------------------------------------------------------ parametros
# Los umbrales viven en config.yaml (versionado). Estos valores son los documentados
# y solo se usan como respaldo: aplicar_configuracion() los reemplaza al importar el
# modulo y al inicio de cada ejecucion (asi el panel toma un config.yaml editado
# sin reiniciarse).
TC_USD_MXN = 19.50
FACTOR_IQR = 1.5
MAX_COLUMNAS = 500
MAX_CATEGORIAS_CHI2 = 15
UMBRAL_COLUMNA_VACIA = 0.60          # mas de 60% nulos: no se imputa, se reporta
LOTE_VALIDACION = 50_000
ALFA = 0.05                          # significancia de la prueba MCAR/MAR
N_SEGMENTOS = 3                      # terciles
ETIQUETAS_SEGMENTO = ["BAJO", "MEDIO", "ALTO"]
BLOQUES = {"umbral_mb": 256.0, "filas_por_bloque": 200_000, "filas_muestra_tipos": 100_000,
           "valores_muestra_cuantiles": 1_000_000}
CONFIG: Configuracion | None = None
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


def aplicar_configuracion(ruta: str | Path | None = None) -> Configuracion:
    """Lee config.yaml y actualiza los parametros del motor. Lanza ErrorConfiguracion."""
    global TC_USD_MXN, FACTOR_IQR, MAX_COLUMNAS, MAX_CATEGORIAS_CHI2, UMBRAL_COLUMNA_VACIA
    global LOTE_VALIDACION, ALFA, N_SEGMENTOS, ETIQUETAS_SEGMENTO, BLOQUES, CONFIG
    cfg = _cargar_config(ruta)
    TC_USD_MXN = cfg.tipo_cambio_usd_mxn
    FACTOR_IQR = cfg.factor_iqr
    MAX_COLUMNAS = cfg.max_columnas
    MAX_CATEGORIAS_CHI2 = cfg.max_categorias_chi2
    UMBRAL_COLUMNA_VACIA = cfg.umbral_columna_vacia
    LOTE_VALIDACION = cfg.lote_validacion
    ALFA = cfg.alfa
    N_SEGMENTOS = cfg.cuantiles
    ETIQUETAS_SEGMENTO = list(cfg.etiquetas)
    BLOQUES = {"umbral_mb": cfg.bloques_umbral_mb, "filas_por_bloque": cfg.filas_por_bloque,
               "filas_muestra_tipos": cfg.filas_muestra_tipos,
               "valores_muestra_cuantiles": cfg.valores_muestra_cuantiles}
    CONFIG = cfg
    return cfg


try:
    aplicar_configuracion()
except ErrorConfiguracion:
    pass        # ejecutar() lo vuelve a intentar y se detiene de forma controlada


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


def _texto_numerico(s: pd.Series) -> pd.Series:
    t = s.astype("string").str.strip()
    t = t.str.replace(r"[\$€£\s]|mxn|usd|eur", "", regex=True, case=False)
    return t.str.replace("%", "", regex=False)


def decidir_coma_decimal(s: pd.Series) -> bool:
    """
    Coma decimal se decide POR COLUMNA, no por valor: "123,45" es ambiguo solo,
    pero si la mayoria de la columna tiene forma 1.234,56 o 123,45, la coma es decimal.
    """
    nn = _texto_numerico(s).dropna()
    if not len(nn):
        return False
    coma_dec = nn.str.match(r"^-?\d{1,3}(\.\d{3})*,\d{1,2}$|^-?\d+,\d{1,2}$", na=False).mean()
    punto_dec = nn.str.match(r"^-?\d+(,\d{3})*\.\d+$", na=False).mean()
    return bool(coma_dec > 0.5 and coma_dec > punto_dec)


def a_numero(s: pd.Series, coma_decimal: bool | None = None) -> pd.Series:
    """
    Convierte texto a numero tolerando $, %, espacios, miles con coma y coma decimal.
    coma_decimal=None decide con la propia serie; en modo por bloques llega la decision
    GLOBAL de la columna, para que todos los bloques lean los numeros igual.
    """
    t = _texto_numerico(s)
    if coma_decimal is None:
        coma_decimal = decidir_coma_decimal(s)
    if coma_decimal:
        t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    else:
        t = t.str.replace(",", "", regex=False)
    t = t.str.replace(r"^\((.*)\)$", r"-\1", regex=True)    # (500) contable -> -500
    # float64 de numpy y no Float64 nullable: las comparaciones con NaN dan False
    # en vez de pd.NA, que hace fallar cualquier `if` ("boolean value of NA is ambiguous").
    return pd.to_numeric(t, errors="coerce").astype("float64")


def hay_epoch(s: pd.Series) -> bool:
    """La columna trae epoch si mas del 5% de sus valores tiene magnitud de epoch."""
    num = pd.to_numeric(s.astype("string").str.strip(), errors="coerce").astype("float64")
    return bool((num.between(5e8, 4.2e9) | num.between(5e11, 4.2e12)).mean() > 0.05)


def a_fecha(s: pd.Series, epoch: bool | None = None) -> tuple[pd.Series, str]:
    """
    Prueba formatos explicitos y epoch; nunca deja que pandas adivine el orden dia/mes.
    epoch=None decide con la propia serie; en modo por bloques llega la decision global.
    """
    original = s
    t = s.astype("string").str.strip()
    resultado = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    usados = []

    # epoch numerico (segundos o milisegundos)
    num = pd.to_numeric(t, errors="coerce").astype("float64")
    es_epoch = num.between(5e8, 4.2e9) | num.between(5e11, 4.2e12)
    if (es_epoch.mean() > 0.05) if epoch is None else epoch:
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
    return ("MAR" if mejor_p < ALFA else "MCAR"), round(mejor_p, 5), mejor_c


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


def fase2_faltantes(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora,
                    globales: dict | None = None) -> tuple[pd.DataFrame, list[dict]]:
    """
    globales (modo por bloques): {"coma": {col: bool}, "imputar": {col: valor}} con las
    decisiones calculadas sobre TODO el archivo. Sin globales, se decide con este df.
    """
    coma = (globales or {}).get("coma", {})
    acciones = []
    df["campos_imputados"] = ""
    df["campos_recalculados"] = ""

    # Recalcular antes que imputar: si la informacion existe en otras columnas,
    # se reconstruye exacta en vez de estimarla con la mediana.
    c_qty, c_pre, c_tot = columnas_aritmeticas(tipos, df.columns)
    if c_qty and c_pre and c_tot:
        q, pr, tot = (a_numero(df[c_qty], coma.get(c_qty)), a_numero(df[c_pre], coma.get(c_pre)),
                      a_numero(df[c_tot], coma.get(c_tot)))
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
    if globales is not None:
        # modo por bloques: la estrategia y el valor ya se decidieron con todo el archivo
        for c, valor in globales.get("imputar", {}).items():
            if c not in df.columns:
                continue
            m = df[c].isna()
            if m.any():
                df[c] = df[c].mask(m, valor)
                df.loc[m, "campos_imputados"] = df.loc[m, "campos_imputados"] + c + ";"
        return df, acciones
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
            accion["estrategia"] = f"no se imputa (mas de {UMBRAL_COLUMNA_VACIA:.0%} vacia)"
        elif t == "numerico":
            num = a_numero(df[c], coma.get(c))
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


def fase34_tipos_outliers_fechas(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora,
                                 globales: dict | None = None) -> tuple[pd.DataFrame, dict]:
    """
    globales (modo por bloques): {"coma", "epoch", "limites": {col: (lo, hi)},
    "duplicados": Serie booleana}. Con globales, el IQR NO se calcula con este bloque:
    se usan los limites del archivo completo, igual que en memoria.
    """
    g = globales or {}
    coma, epoch = g.get("coma", {}), g.get("epoch", {})
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
        q, pr, tot = (a_numero(df[c_qty], coma.get(c_qty)), a_numero(df[c_pre], coma.get(c_pre)),
                      a_numero(df[c_tot], coma.get(c_tot)))
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
            num = a_numero(df[c], coma.get(c))
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
            if globales is not None:
                lim = g.get("limites", {}).get(c)
                if lim is not None:
                    lo, hi = lim
                    fuera = (num < lo) | (num > hi)
                    df[f"{c}_original"] = num.where(fuera)
                    num = num.clip(lo, hi)
                    df.loc[fuera, "flag_outlier"] = True
                    info["iqr"].append({"columna": c, "recortados": int(fuera.sum())})
                df[c] = num
                continue
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
            conv, usados = a_fecha(df[c], epoch.get(c))
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

    if "duplicados" in g:
        dups = g["duplicados"].reindex(df.index, fill_value=False)   # decision global
    else:
        dups = df.drop(columns=["campos_imputados", "campos_recalculados", "flag_outlier",
                                "motivo_previo"],
                       errors="ignore").duplicated(keep="first")
    df.loc[dups, "motivo_previo"] += "duplicado exacto; "
    info["duplicados"] = int(dups.sum())
    info["filas_usd"] = int(usd.sum())
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


def fase5_escalar(df: pd.DataFrame, tipos: dict, nombre: str, log: Bitacora,
                  globales: dict | None = None) -> tuple[pd.DataFrame, dict]:
    """
    globales (modo por bloques): {"minmax": {col: (min, max)}, "discretizacion":
    {"columna", "bins", "include_lowest"}} calculados con el archivo completo.
    """
    info: dict[str, Any] = {"minmax": [], "discretizacion": None}
    numericas = [c for c, t in tipos.items() if t == "numerico" and c in df.columns]
    if globales is not None:
        for c in numericas:
            if c in globales.get("minmax", {}):
                lo, hi = globales["minmax"][c]
                s = pd.to_numeric(df[c], errors="coerce")
                df[f"{c}_norm"] = ((s - lo) / (hi - lo)).clip(0, 1).round(6)
        d = globales.get("discretizacion")
        if d:
            s = pd.to_numeric(df[d["columna"]], errors="coerce")
            df["segmento"] = pd.cut(s, d["bins"], labels=ETIQUETAS_SEGMENTO,
                                    include_lowest=d["include_lowest"]).astype("string")
        return df, info
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
            seg, cortes = pd.qcut(s, N_SEGMENTOS, labels=ETIQUETAS_SEGMENTO, retbins=True,
                                  duplicates="drop")
            if len(cortes) < N_SEGMENTOS + 1:
                raise ValueError
        except (ValueError, IndexError):
            seg, cortes = pd.cut(s, N_SEGMENTOS, labels=ETIQUETAS_SEGMENTO, retbins=True)
        df["segmento"] = seg.astype("string")
        info["discretizacion"] = {"columna": objetivo,
                                  "cortes": [round(float(x), 4) for x in cortes],
                                  "reparto": df["segmento"].value_counts().to_dict()}
        log("5", f"{nombre}: Min-Max en {len(info['minmax'])} columnas; '{objetivo}' en "
            f"{'/'.join(ETIQUETAS_SEGMENTO)} por {_nombre_cuantiles()}", "ok")
    else:
        log("5", f"{nombre}: sin columnas numericas que escalar", "aviso")
    return df, info


def _nombre_cuantiles() -> str:
    return {2: "medianas", 3: "terciles", 4: "cuartiles", 5: "quintiles"}.get(
        N_SEGMENTOS, f"{N_SEGMENTOS} cuantiles")


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


# ===================================================== comparacion antes/despues
PATRONES_FECHA = [
    ("AAAA-MM-DD", r"^\d{4}-\d{2}-\d{2}$"),
    ("AAAA-MM-DD con hora", r"^\d{4}-\d{2}-\d{2}[T ]\d{1,2}:\d{2}"),
    ("DD/MM/AAAA", r"^\d{1,2}/\d{1,2}/\d{4}$"),
    ("DD/MM/AAAA con hora", r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}"),
    ("DD-MM-AAAA", r"^\d{1,2}-\d{1,2}-\d{4}$"),
    ("AAAA/MM/DD", r"^\d{4}/\d{1,2}/\d{1,2}"),
    ("epoch", r"^\d{9,13}(\.0+)?$"),
]


def formatos_fecha(s: pd.Series) -> dict[str, int]:
    """Cuantos valores hay de cada formato de fecha (el primero que coincide)."""
    t = s.dropna().astype("string").str.strip()
    out: dict[str, int] = {}
    pendiente = pd.Series(True, index=t.index)
    for etq, pat in PATRONES_FECHA:
        m = pendiente & t.str.match(pat, na=False)
        if m.any():
            out[etq] = int(m.sum())
        pendiente &= ~m
    if pendiente.any():
        out["otro"] = int(pendiente.sum())
    return out


def estadisticas_columna(s: pd.Series, tipo: str, crudo: bool) -> dict:
    """Metricas de una columna para la comparacion antes/despues (mismo formato que el panel)."""
    n = int(len(s))
    nn = s.notna()
    nulos = int(n - nn.sum())
    d: dict[str, Any] = {"n": n, "nulos": nulos, "pct_nulos": round(100 * nulos / max(n, 1), 2),
                         "distintos": int(s.nunique())}
    if tipo == "numerico":
        num = a_numero(s) if crudo else pd.to_numeric(s, errors="coerce").astype("float64")
        d["no_numericos"] = int((nn & num.isna()).sum())
        f = num[np.isfinite(num)]
        d["no_positivos"] = int((f <= 0).sum())
        if len(f):
            q = f.quantile([.25, .5, .75])
            d.update(min=round(float(f.min()), 4), q1=round(float(q.iloc[0]), 4),
                     mediana=round(float(q.iloc[1]), 4), media=round(float(f.mean()), 4),
                     q3=round(float(q.iloc[2]), 4), max=round(float(f.max()), 4),
                     desv=round(float(f.std()), 4) if len(f) > 1 else 0.0)
    elif tipo == "fecha":
        d["formatos"] = formatos_fecha(s)
    elif tipo == "correo":
        v = s.dropna().astype("string").str.strip().str.lower()
        v = v[v != "anonimo"]
        malo = v.str.contains("@@", regex=False) | v.str.contains("..", regex=False) | \
            ~v.str.match(RE_EMAIL.pattern, na=False)
        d["invalidos"] = int(malo.sum())
    if tipo in ("categorico", "booleano") or (tipo != "numerico" and d["distintos"] <= 12):
        d["top"] = [[str(k), int(v)] for k, v in s.dropna().astype(str).value_counts().head(8).items()]
    return d


def comparar_columnas(antes: dict[str, dict], despues: pd.DataFrame, tipos: dict,
                      muestra: str | None = None) -> dict:
    """antes: {columna: estadisticas del dato crudo}; despues: tabla limpia."""
    cols = []
    for c, t in tipos.items():
        if c not in antes:
            continue
        cols.append({"columna": c, "tipo": t, "origen_antes": c, "antes": antes[c],
                     "despues": estadisticas_columna(despues[c], t, False)
                     if c in despues.columns else None})
    return {"columnas": cols, "muestra": muestra}


# ======================================================= modo por bloques
EXT_TEXTO = {".csv", ".tsv", ".txt"}
EXT_JSONL = {".jsonl", ".ndjson"}
TOPE_DISTINTOS = 20_000              # distintos exactos por columna en el perfil
SEMILLA = 20261029                   # muestras reproducibles


def _mudo(*_a, **_k) -> None:
    """Bitacora silenciosa para las fases que se aplican bloque por bloque."""


def es_streamable(ruta: Path) -> bool:
    return ruta.suffix.lower() in EXT_TEXTO | EXT_JSONL


def _detectar_texto(ruta: Path) -> dict:
    """Codificacion y separador con el primer MB, como leer_csv pero sin cargar el archivo."""
    with open(ruta, "rb") as f:
        muestra = f.read(1 << 20)
    if not muestra.strip():
        raise ValueError("archivo vacio")
    if b"\x00" in muestra[:4096]:
        raise ValueError("el archivo parece binario, no es un CSV de texto")
    texto, cod = None, None
    for c in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            texto, cod = muestra.decode(c), c
            break
        except UnicodeDecodeError as e:
            # el corte del primer MB pudo partir un caracter de varios bytes
            if c.startswith("utf-8") and len(muestra) == 1 << 20 and e.start >= len(muestra) - 3:
                texto, cod = muestra[:e.start].decode(c), c
                break
    if texto is None:
        raise ValueError("no se pudo decodificar el texto")
    inicio = texto.lstrip()[:1]
    if inicio in ("[", "{"):
        # mismo criterio que leer_archivo: si empieza como JSON, se lee como JSON.
        # Solo es JSON Lines (procesable por bloques) si la primera linea ya es un objeto.
        primera = texto.lstrip().split("\n", 1)[0].strip().rstrip(",")
        try:
            es_lineas = isinstance(json.loads(primera), dict)
        except json.JSONDecodeError:
            es_lineas = False
        return {"tipo": "jsonl" if es_lineas else "json", "cod": cod}
    m = texto[:65536]
    try:
        sep = csv.Sniffer().sniff(m, delimiters=",;\t|").delimiter
    except csv.Error:
        conteos = {d: m.count(d) for d in (",", ";", "\t", "|")}
        sep = max(conteos, key=conteos.get)
    return {"tipo": "csv", "cod": cod, "sep": sep}


def _jsonl_a_df(lote: list, columnas: list[str] | None) -> pd.DataFrame:
    df = pd.json_normalize(lote, max_level=2, sep="_")
    for c in df.columns:
        if df[c].map(lambda v: isinstance(v, (list, dict))).any():
            df[c] = df[c].map(lambda v: json.dumps(v, ensure_ascii=False)
                              if isinstance(v, (list, dict)) else v)
    if columnas is not None:
        df = df.reindex(columns=columnas)
    return df.astype(object).where(df.notna(), None).astype("string").fillna("")


def _iterar_bloques(ruta: Path, fmt: dict, filas: int, estado: dict):
    """Genera DataFrames de texto de hasta `filas` filas. Una linea rota nunca lo detiene."""
    if fmt["tipo"] == "csv":
        comunes = dict(sep=fmt["sep"], dtype=str, keep_default_na=False, encoding=fmt["cod"],
                       encoding_errors="replace", skip_blank_lines=True, chunksize=filas)
        leidas, tolerante = 0, False
        # lector rapido (motor C); las lineas con columnas de mas se cuentan y se omiten
        lector = pd.read_csv(ruta, engine="c", on_bad_lines="warn", **comunes)
        with lector:
            while True:
                with warnings.catch_warnings(record=True) as avisos:
                    warnings.simplefilter("always")
                    try:
                        bloque = next(lector)
                    except StopIteration:
                        break
                    except (pd.errors.ParserError, ValueError, UnicodeError) as e:
                        estado["cortado"] = f"{type(e).__name__}: {e}"[:200]
                        tolerante = True
                        break
                estado["malas"] += sum(len(re.findall(r"Skipping line", str(w.message)))
                                       for w in avisos)
                leidas += len(bloque)
                yield bloque
        if not tolerante:
            return
        # p. ej. una comilla sin cerrar al final: el motor C aborta el bloque completo. Para
        # no perder esas filas, el resto del archivo se lee con el lector tolerante (Python).
        def mala(_linea):
            estado["malas"] += 1
            return None
        try:
            lector = pd.read_csv(ruta, engine="python", on_bad_lines=mala,
                                 skiprows=range(1, leidas + 1), **comunes)
            with lector:
                yield from lector
        except (pd.errors.ParserError, ValueError, UnicodeError, csv.Error) as e:
            estado["cortado"] += f" | lector tolerante: {type(e).__name__}: {e}"[:200]
        return
    lote: list = []
    with open(ruta, encoding="utf-8-sig", errors="replace") as f:
        for linea in f:
            linea = linea.strip().rstrip(",")
            if not linea or linea in ("[", "]"):
                continue
            try:
                obj = json.loads(linea)
            except json.JSONDecodeError:
                estado["malas"] += 1
                continue
            lote.append(obj if isinstance(obj, dict) else {"valor": obj})
            if len(lote) >= filas:
                yield _jsonl_a_df(lote, fmt.get("columnas"))
                lote = []
    if lote:
        yield _jsonl_a_df(lote, fmt.get("columnas"))


def _fase0_bloque(bloque: pd.DataFrame, nombres: list[str]) -> pd.DataFrame:
    """La misma normalizacion de fase0_estructura, sin decisiones globales."""
    df = bloque.iloc[:, :len(nombres)].copy()
    df.columns = nombres
    df = df.astype("string")
    for c in df.columns:
        s = df[c].str.strip()
        df[c] = s.mask(s.str.lower().isin(VACIOS))
    return df.dropna(how="all")


class _Reservorio:
    """Muestra aleatoria uniforme de tamano fijo (k claves aleatorias mas pequenas)."""

    def __init__(self, k: int, rng: np.random.Generator):
        self.k, self.rng, self.total = k, rng, 0
        self.vals = np.empty(0)
        self.marca = np.empty(0, dtype=bool)
        self.claves = np.empty(0)

    def agregar(self, vals: np.ndarray, marca: np.ndarray) -> None:
        if not len(vals):
            return
        self.total += len(vals)
        claves = self.rng.random(len(vals))
        self.vals = np.concatenate([self.vals, vals])
        self.marca = np.concatenate([self.marca, marca])
        self.claves = np.concatenate([self.claves, claves])
        if len(self.vals) > self.k:
            sel = np.argpartition(self.claves, self.k)[:self.k]
            self.vals, self.marca, self.claves = self.vals[sel], self.marca[sel], self.claves[sel]

    @property
    def completo(self) -> bool:
        """True si conserva TODOS los valores vistos (cuantiles exactos)."""
        return self.total <= self.k


def _hash_filas(df: pd.DataFrame) -> np.ndarray:
    return pd.util.hash_pandas_object(df, index=False).to_numpy(dtype=np.uint64)


def _bins_iguales(mn: float, mx: float, n: int) -> list[float]:
    """Los mismos cortes que pd.cut(s, n): ancho igual con el borde inferior ajustado 0.1%."""
    if mn == mx:
        mn -= 0.001 * abs(mn) if mn != 0 else 0.001
        mx += 0.001 * abs(mx) if mx != 0 else 0.001
        return list(np.linspace(mn, mx, n + 1, endpoint=True))
    b = np.linspace(mn, mx, n + 1, endpoint=True)
    b[0] -= (mx - mn) * 0.001
    return list(b)


def _sumar(acum: dict, nuevo: dict) -> None:
    for k, v in nuevo.items():
        acum[k] = acum.get(k, 0) + v


def procesar_por_bloques(ruta: Path, nombre: str, salida: Path, bd: Path,
                         maestros: dict[str, dict], log: Bitacora) -> dict:
    """
    Las 7 fases sobre un archivo que no cabe en memoria. Devuelve el reporte de la
    fuente con la misma forma que el modo en memoria, mas la clave 'bloques'.
    """
    filas = BLOQUES["filas_por_bloque"]
    k_vals = BLOQUES["valores_muestra_cuantiles"]
    rng = np.random.default_rng(SEMILLA)
    mb = ruta.stat().st_size / 1024**2
    t0 = time.perf_counter()
    tiempos: dict[str, float] = {}
    fmt = _detectar_texto(ruta)
    estado = {"malas": 0, "cortado": None}

    if fmt["tipo"] == "jsonl":            # columnas: union en orden de aparicion
        cols: list[str] = []
        vistas: set[str] = set()
        for b in _iterar_bloques(ruta, fmt, filas, {"malas": 0, "cortado": None}):
            for c in b.columns:
                if c not in vistas:
                    vistas.add(c)
                    cols.append(c)
        fmt["columnas"] = cols
    log("0", f"{nombre}: {mb:,.1f} MB -> modo por bloques de {filas:,} filas "
        f"({fmt['tipo'].upper()}, codificacion {fmt['cod']}"
        f"{', separador ' + repr(fmt['sep']) if fmt['tipo'] == 'csv' else ''})", "info")

    # ------------------------------------------------ pasada 1: estructura
    nombres: list[str] | None = None
    mapeo: dict = {}
    columnas_crudas = 0
    n_crudas = n = 0
    nulos: dict[str, int] = {}
    distintos: dict[str, set | None] = {}
    ejemplo: dict[str, str] = {}
    hashes: list[np.ndarray] = []
    muestra: pd.DataFrame | None = None
    claves = np.empty(0)
    k_filas = BLOQUES["filas_muestra_tipos"]
    for bloque in _iterar_bloques(ruta, fmt, filas, estado):
        if nombres is None:
            columnas_crudas = bloque.shape[1]
            originales = list(bloque.columns)[:MAX_COLUMNAS]
            nombres = nombres_unicos(originales)
            mapeo = dict(zip(originales, nombres))
            nulos = dict.fromkeys(nombres, 0)
            distintos = {c: set() for c in nombres}
            if columnas_crudas > MAX_COLUMNAS:
                log("0", f"{nombre}: {columnas_crudas} columnas, se conservan las primeras "
                    f"{MAX_COLUMNAS}", "aviso")
        bloque.index = pd.RangeIndex(n_crudas, n_crudas + len(bloque))
        n_crudas += len(bloque)
        df = _fase0_bloque(bloque, nombres)
        if df.empty:
            continue
        n += len(df)
        for c in nombres:
            col = df[c]
            nulos[c] += int(col.isna().sum())
            d = distintos[c]
            if d is not None:
                d.update(col.dropna().unique().tolist())
                if len(d) > TOPE_DISTINTOS:
                    distintos[c] = None
            if c not in ejemplo:
                v = col.dropna()
                if len(v):
                    ejemplo[c] = str(v.iloc[0])[:40]
        hashes.append(_hash_filas(df))
        kk = rng.random(len(df))
        if muestra is None:
            muestra, claves = df, kk
        else:
            if len(muestra) >= k_filas:           # solo entran filas con clave menor
                umbral = np.partition(claves, k_filas - 1)[k_filas - 1]
                df, kk = df[kk < umbral], kk[kk < umbral]
            muestra = pd.concat([muestra, df])
            claves = np.concatenate([claves, kk])
        if len(muestra) > k_filas:
            sel = np.argpartition(claves, k_filas)[:k_filas]
            muestra, claves = muestra.iloc[sel], claves[sel]
    tiempos["pasada1"] = round(time.perf_counter() - t0, 2)
    if estado["cortado"]:
        log("0", f"{nombre}: el lector rapido se detuvo ({estado['cortado']}); el resto se leyo "
            f"con el lector tolerante ({n_crudas:,} filas en total)", "aviso")
    if estado["malas"]:
        log("0", f"{nombre}: {estado['malas']:,} lineas con columnas de mas se omitieron", "aviso")
    estructura = {"filas_crudas": n_crudas, "columnas_crudas": columnas_crudas,
                  "mapeo_columnas": mapeo, "filas_vacias_quitadas": n_crudas - n}
    if nombres is None or n == 0:
        estructura["columnas_vacias_quitadas"] = list(nombres or [])
        log("0", f"{nombre}: sin filas utiles tras limpiar la estructura", "aviso")
        return {"vacio": True, "estructura": estructura}

    vacias = [c for c in nombres if nulos[c] == n]
    keep = [c for c in nombres if c not in vacias]
    estructura["columnas_vacias_quitadas"] = vacias
    muestra = muestra.sort_index()[keep].reset_index(drop=True)
    tipos = inferir_tipos(muestra)
    h = np.concatenate(hashes)
    del hashes
    h.sort()
    iguales = h[1:] == h[:-1]
    n_dup = int(iguales.sum())
    valores_dup = np.unique(h[1:][iguales])
    del h, iguales
    completitud = round(100 - 100 * sum(nulos[c] for c in keep) / max(n * len(keep), 1), 2)
    perfil = {"columnas": [{
        "columna": c, "tipo": tipos.get(c), "nulos": nulos[c],
        "pct_nulos": round(100 * nulos[c] / n, 2),
        "unicos": len(distintos[c]) if distintos[c] is not None else TOPE_DISTINTOS,
        "unicos_es_minimo": distintos[c] is None,
        "ejemplo": ejemplo.get(c, "")} for c in keep],
        "duplicados_exactos": n_dup, "completitud_pct": completitud, "tipos": tipos}
    log("0", f"{nombre}: {n:,} filas x {len(keep)} columnas tras normalizar estructura", "ok")
    log("1", f"{nombre}: completitud {completitud}%, {n_dup:,} duplicados exactos "
        f"(pasada 1 de 3, {tiempos['pasada1']}s)", "ok")
    antes = {c: estadisticas_columna(muestra[c], tipos[c], True) for c in keep}

    numericas = [c for c, t in tipos.items() if t == "numerico"]
    fechas = [c for c, t in tipos.items() if t == "fecha"]
    coma = {c: decidir_coma_decimal(muestra[c]) for c in numericas}
    epoch = {c: hay_epoch(muestra[c]) for c in fechas}
    c_qty, c_pre, c_tot = columnas_aritmeticas(tipos, keep)
    col_mon = next((c for c in keep if tiene(c, PISTAS_MONEDA)), None)
    importe = {c: tiene(c, PISTAS_IMPORTE) for c in numericas}
    con_nulos = [c for c in keep if nulos[c] > 0]
    cand_chi2 = [c for c in keep if distintos[c] is not None
                 and 2 <= len(distintos[c]) <= MAX_CATEGORIAS_CHI2]

    # ------------------------------------------------ pasada 2: distribuciones
    t1 = time.perf_counter()
    res = {c: _Reservorio(k_vals, rng) for c in numericas}
    nulo_usd = dict.fromkeys(numericas, 0)
    nulo_mxn = dict.fromkeys(numericas, 0)
    vmin = dict.fromkeys(numericas, np.inf)
    vmax = dict.fromkeys(numericas, -np.inf)
    nonulos = dict.fromkeys(numericas, 0)
    finitos: dict[str, set] = {c: set() for c in numericas}
    nulos2 = dict.fromkeys(keep, 0)
    recalc = {c: 0 for c in (c_tot, c_pre, c_qty) if c}
    faltan_por: dict[str, pd.DataFrame] = {}
    tam_por: dict[str, pd.Series] = {}
    for bloque in _iterar_bloques(ruta, fmt, filas, {"malas": 0, "cortado": None}):
        df = _fase0_bloque(bloque, nombres)
        if df.empty:
            continue
        df = df[keep].reset_index(drop=True)
        df, _ = fase2_faltantes(df, tipos, nombre, _mudo, {"coma": coma, "imputar": {}})
        for c in recalc:
            recalc[c] += int(df["campos_recalculados"].str.contains(f"{c};", regex=False).sum())
        for c in keep:
            nulos2[c] += int(df[c].isna().sum())
        usd = (df[col_mon].astype("string").str.upper().str.strip()
               .isin(["USD", "US$", "DOLAR", "DÓLAR", "DOLARES"]).fillna(False).to_numpy(bool)
               if col_mon else np.zeros(len(df), dtype=bool))
        for c in numericas:
            num = a_numero(df[c], coma[c]).to_numpy(dtype="float64")
            nn = ~np.isnan(num)
            marca = usd & importe[c]
            res[c].agregar(num[nn], marca[nn])
            nulo_usd[c] += int((~nn & marca).sum())
            nulo_mxn[c] += int((~nn & ~marca).sum())
            if nn.any():
                conv = np.where(marca, num * TC_USD_MXN, num)[nn]
                nonulos[c] += int(nn.sum())
                vmin[c] = min(vmin[c], float(conv.min()))
                vmax[c] = max(vmax[c], float(conv.max()))
                if len(finitos[c]) <= 10:
                    finitos[c].update(np.unique(conv[np.isfinite(conv)])[:11].tolist())
        if con_nulos and cand_chi2:
            faltan = df[con_nulos].isna()
            for L in cand_chi2:
                etq = df[L].astype(object).where(df[L].notna(), "_nulo_")
                g = faltan.groupby(etq.to_numpy()).sum()
                faltan_por[L] = g if L not in faltan_por else faltan_por[L].add(g, fill_value=0)
                vc = etq.value_counts()
                tam_por[L] = vc if L not in tam_por else tam_por[L].add(vc, fill_value=0)
    tiempos["pasada2"] = round(time.perf_counter() - t1, 2)

    # --- fase 2 global: estrategia por columna (mismas reglas que en memoria)
    from scipy.stats import chi2_contingency
    faltantes: list[dict] = []
    for c, cnt in recalc.items():
        if cnt:
            faltantes.append({"columna": c, "tipo": "numerico", "faltantes": cnt,
                              "pct": round(100 * cnt / n, 2), "clasificacion": "recalculable",
                              "p_valor": None, "depende_de": f"{c_qty}, {c_pre}, {c_tot}",
                              "estrategia": "recalculo exacto desde otras columnas"})
    imputar: dict[str, str] = {}
    medianas: dict[str, float] = {}
    for c, t in tipos.items():
        m = nulos2[c]
        if m == 0:
            continue
        pct = m / n
        tipo_f, p, pred = "N/A", None, None
        if m < n:
            mejor_p, mejor_c = 1.0, None
            for L in cand_chi2:
                if L == c or L not in faltan_por or c not in faltan_por[L].columns:
                    continue
                falta = faltan_por[L][c]
                tam = tam_por[L].reindex(falta.index).fillna(0)
                tabla = np.column_stack([tam - falta, falta])
                tabla = tabla[tabla.sum(axis=1) > 0]
                if tabla.shape[0] < 2 or (tabla[:, 1] == 0).all() or (tabla[:, 0] == 0).all():
                    continue
                try:
                    pv = chi2_contingency(tabla)[1]
                except ValueError:
                    continue
                if pv < mejor_p:
                    mejor_p, mejor_c = pv, L
            tipo_f, p, pred = ("MAR" if mejor_p < ALFA else "MCAR"), round(mejor_p, 5), mejor_c
        accion = {"columna": c, "tipo": t, "faltantes": m, "pct": round(100 * pct, 2),
                  "clasificacion": tipo_f, "p_valor": p, "depende_de": pred}
        if pct > UMBRAL_COLUMNA_VACIA:
            accion["estrategia"] = f"no se imputa (mas de {UMBRAL_COLUMNA_VACIA:.0%} vacia)"
        elif t == "numerico":
            r = res[c]
            med = float(pd.Series(r.vals).median()) if len(r.vals) else 0.0
            medianas[c] = med
            imputar[c] = str(med)
            accion.update(estrategia="mediana", valor=med)
        elif t in ("categorico", "booleano", "texto"):
            imputar[c] = "DESCONOCIDO"
            accion.update(estrategia="etiqueta controlada", valor="DESCONOCIDO")
        elif t == "correo":
            imputar[c] = "ANONIMO"
            accion.update(estrategia="etiqueta controlada", valor="ANONIMO")
        else:
            accion["estrategia"] = "se conserva nulo (id o fecha: imputar inventaria el dato)"
        faltantes.append(accion)
    log("2", f"{nombre}: {len(faltantes)} columnas con faltantes tratadas "
        f"(pasada 2 de 3, {tiempos['pasada2']}s)", "ok")

    # --- fase 3 y 5 globales: IQR, Min-Max y cortes de segmentacion
    limites: dict[str, tuple[float, float]] = {}
    info_iqr: dict[str, dict] = {}
    minmax: dict[str, tuple[float, float]] = {}
    rango: dict[str, tuple[float, float]] = {}
    acotados: dict[str, np.ndarray] = {}
    exactos = True
    for c in numericas:
        r = res[c]
        exactos &= r.completo
        vals = np.where(r.marca, r.vals * TC_USD_MXN, r.vals)
        lo_c, hi_c = vmin[c], vmax[c]
        extra: list[float] = []
        if c in medianas:
            med = medianas[c]
            f = 1.0 if r.completo else (len(r.vals) / max(r.total, 1))
            n_mxn, n_usd = int(round(nulo_mxn[c] * f)), int(round(nulo_usd[c] * f))
            vals = np.concatenate([vals, np.full(n_mxn, med), np.full(n_usd, med * TC_USD_MXN)])
            for v, cuantos in ((med, nulo_mxn[c]), (med * TC_USD_MXN, nulo_usd[c])):
                if cuantos:
                    extra.append(v)
                    lo_c, hi_c = min(lo_c, v), max(hi_c, v)
        serie = vals[np.isfinite(vals)]
        distintos_fin = finitos[c] | {v for v in extra if np.isfinite(v)}
        if len(distintos_fin) > 10 and len(serie):
            q1, q3 = pd.Series(serie).quantile([.25, .75])
            iqr = q3 - q1
            if iqr > 0:
                lo, hi = q1 - FACTOR_IQR * iqr, q3 + FACTOR_IQR * iqr
                limites[c] = (lo, hi)
                info_iqr[c] = {"columna": c, "q1": round(q1, 4), "q3": round(q3, 4),
                               "limite_inf": round(lo, 4), "limite_sup": round(hi, 4),
                               "recortados": 0}
        hay_datos = bool(nonulos[c] or extra)
        if c in limites:
            lo, hi = limites[c]
            vals = np.clip(vals, lo, hi)
            if hay_datos:              # el capping es monotono: min(clip(x)) = clip(min(x))
                lo_c, hi_c = float(np.clip(lo_c, lo, hi)), float(np.clip(hi_c, lo, hi))
        acotados[c] = vals[~np.isnan(vals)]
        if hay_datos and not np.isnan(lo_c) and not np.isnan(hi_c):
            rango[c] = (lo_c, hi_c)
            if hi_c != lo_c:
                minmax[c] = (lo_c, hi_c)
    prioridad = ("total", "monto", "importe", "amount", "venta", "saldo", "valor", "precio", "price")
    objetivo = next((c for p in prioridad for c in numericas if p in c), None)
    if objetivo is None and numericas:
        objetivo = max(numericas, key=lambda c: pd.Series(acotados[c]).std() or 0)
    disc = None
    if objetivo and len(acotados[objetivo]):
        s = pd.Series(acotados[objetivo])
        bins = s.quantile(np.linspace(0, 1, N_SEGMENTOS + 1)).to_numpy(dtype="float64")
        if objetivo in rango:          # extremos exactos aunque la muestra no los tenga
            bins[0], bins[-1] = rango[objetivo]
        if len(np.unique(bins)) == N_SEGMENTOS + 1 and np.isfinite(bins).all():
            disc = {"columna": objetivo, "bins": list(bins), "include_lowest": True}
        elif objetivo in rango and np.isfinite(rango[objetivo]).all():
            # mismo respaldo que en memoria: pd.cut con cortes de ancho igual
            disc = {"columna": objetivo, "bins": _bins_iguales(*rango[objetivo], N_SEGMENTOS),
                    "include_lowest": False}
        else:
            log("5", f"{nombre}: '{objetivo}' con valores no finitos; no se discretiza", "aviso")

    # --- relaciones con catalogos maestros que si caben en memoria
    relaciones = []
    for maestro, mv in maestros.items():
        dm, tm = mv["df"], mv["tipos"]
        for c in dm.columns:
            if tm.get(c) not in ("id", "categorico", "texto"):
                continue
            col = dm[c].dropna()
            if len(col) < 2 or not col.is_unique:
                continue
            llaves = set(col.astype(str).str.strip().str.upper())
            for hcol in keep:
                if tipos.get(hcol) not in ("id", "categorico", "texto"):
                    continue
                v = muestra[hcol].dropna().astype(str).str.strip().str.upper()
                if not (hcol == c or v.head(2000).isin(llaves).mean() >= 0.5):
                    continue
                if v.empty or v.isin(llaves).mean() < 0.5:
                    continue
                relaciones.append({"maestro": maestro, "llave": c, "hija": nombre,
                                   "columna": hcol, "huerfanos": 0, "_llaves": llaves})

    # ------------------------------------------------ pasada 3: transformar y cargar
    t2 = time.perf_counter()
    g34 = {"coma": coma, "epoch": epoch, "limites": limites}
    g5 = {"minmax": minmax, "discretizacion": disc}
    f_ok, f_q = salida / f"{nombre}_limpias.csv", salida / f"{nombre}_cuarentena.csv"
    p_ok, p_q = f_ok.with_suffix(".csv.parcial"), f_q.with_suffix(".csv.parcial")
    for p in (p_ok, p_q):
        p.unlink(missing_ok=True)
    base = normalizar_nombre(nombre, 0)
    t_ok, t_q = f"{base}_limpias", f"{base}_cuarentena"
    con = sqlite3.connect(bd, isolation_level=None)
    vistos: set[int] = set()
    cols_ok = cols_q = None
    validas = cuar = procesadas = n_bloque = 0
    causas: dict[str, int] = {}
    agg = {"recortados": {}, "imposibles": {}, "conv": {}, "noconv": {}, "formatos": {},
           "inconsistentes": 0, "patron_bot": 0, "duplicados": 0, "usd": 0, "reparto": {}}
    campos_esquema: list[str] = []
    m_ok: list[pd.DataFrame] = []
    m_q: list[pd.DataFrame] = []
    res_ok: pd.DataFrame | None = None
    claves_ok = np.empty(0)
    k_cmp = BLOQUES["filas_muestra_tipos"]

    def _crear(tabla: str, df: pd.DataFrame) -> None:
        defs = []
        for c, dt in df.dtypes.items():
            tipo = ("INTEGER" if pd.api.types.is_bool_dtype(dt) or pd.api.types.is_integer_dtype(dt)
                    else "REAL" if pd.api.types.is_float_dtype(dt) else "TEXT")
            defs.append(f'"{c}" {tipo}')
        con.execute(f'CREATE TABLE "{tabla}" ({", ".join(defs)})')

    def _insertar(tabla: str, df: pd.DataFrame) -> None:
        if len(df):
            filas_sql = df.astype(object).where(df.notna(), None).itertuples(index=False, name=None)
            con.executemany(f'INSERT INTO "{tabla}" VALUES ({",".join("?" * df.shape[1])})',
                            filas_sql)

    try:
        con.execute("BEGIN")
        con.execute(f'DROP TABLE IF EXISTS "{t_ok}"')
        con.execute(f'DROP TABLE IF EXISTS "{t_q}"')
        for bloque in _iterar_bloques(ruta, fmt, filas, {"malas": 0, "cortado": None}):
            df0 = _fase0_bloque(bloque, nombres)
            if df0.empty:
                continue
            n_bloque += 1
            hb = _hash_filas(df0)
            dup = np.zeros(len(hb), dtype=bool)
            if len(valores_dup):
                for i in np.flatnonzero(np.isin(hb, valores_dup)):
                    v = int(hb[i])
                    if v in vistos:
                        dup[i] = True
                    else:
                        vistos.add(v)
            df = df0[keep].reset_index(drop=True)
            df, _ = fase2_faltantes(df, tipos, nombre, _mudo, {"coma": coma, "imputar": imputar})
            df, i34 = fase34_tipos_outliers_fechas(
                df, tipos, nombre, _mudo, {**g34, "duplicados": pd.Series(dup, index=df.index)})
            for rel in relaciones:
                hcol = rel["columna"]
                huerf = df[hcol].notna() & ~df[hcol].astype(str).str.strip().str.upper().isin(
                    rel["_llaves"])
                df.loc[huerf, "motivo_previo"] += (f"llave huerfana: {hcol} no existe en "
                                                   f"{rel['maestro']}; ")
                rel["huerfanos"] += int(huerf.sum())
            df, _ = fase5_escalar(df, tipos, nombre, _mudo, g5)
            ok, q, i6 = fase6_validar(df, tipos, nombre, _mudo)
            if cols_ok is None:
                cols_ok, cols_q = list(ok.columns), list(q.columns)
                campos_esquema = i6.get("campos_esquema", [])
                _crear(t_ok, ok)
                _crear(t_q, q)
            ok, q = ok.reindex(columns=cols_ok), q.reindex(columns=cols_q)
            primero = n_bloque == 1
            ok.to_csv(p_ok, mode="a", header=primero, index=False,
                      encoding="utf-8-sig" if primero else "utf-8")
            q.to_csv(p_q, mode="a", header=primero, index=False,
                     encoding="utf-8-sig" if primero else "utf-8")
            _insertar(t_ok, ok)
            _insertar(t_q, q)
            # --- acumular reportes
            validas += len(ok)
            cuar += len(q)
            procesadas += len(df)
            _sumar(causas, i6.get("causas", {}))
            _sumar(agg["recortados"], {x["columna"]: x["recortados"] for x in i34["iqr"]})
            _sumar(agg["imposibles"], i34["imposibles"])
            for x in i34["fechas"]:
                _sumar(agg["conv"], {x["columna"]: x["convertidas"]})
                _sumar(agg["noconv"], {x["columna"]: x["no_convertibles"]})
                agg["formatos"].setdefault(x["columna"], set()).update(
                    f for f in x["formatos"].split(", ") if f and f != "ninguno")
            if i34.get("aritmetica"):
                agg["inconsistentes"] += i34["aritmetica"]["inconsistentes"]
                agg["patron_bot"] += i34["aritmetica"]["patron_bot"]
            agg["duplicados"] += i34["duplicados"]
            agg["usd"] += i34.get("filas_usd", 0)
            if "segmento" in df.columns:
                _sumar(agg["reparto"], df["segmento"].value_counts().to_dict())
            if sum(len(x) for x in m_ok) < 25:
                m_ok.append(ok.head(25))
            if len(q) and sum(len(x) for x in m_q) < 50:
                m_q.append(q.head(50))
            kk = rng.random(len(ok))           # muestra de limpias para la comparacion
            res_ok = ok if res_ok is None else pd.concat([res_ok, ok])
            claves_ok = np.concatenate([claves_ok, kk])
            if len(res_ok) > k_cmp:
                sel = np.argpartition(claves_ok, k_cmp)[:k_cmp]
                res_ok, claves_ok = res_ok.iloc[sel], claves_ok[sel]
            log("*", f"{nombre}: bloque {n_bloque}: {procesadas:,}/{n:,} filas "
                f"({100 * procesadas / n:.0f}%) en {time.perf_counter() - t2:,.1f}s", "info")
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        for p in (p_ok, p_q):
            p.unlink(missing_ok=True)
        raise
    finally:
        con.close()
    os.replace(p_ok, f_ok)
    os.replace(p_q, f_q)
    tiempos["pasada3"] = round(time.perf_counter() - t2, 2)
    tiempos["total"] = round(time.perf_counter() - t0, 2)

    for c, inf in info_iqr.items():
        inf["recortados"] = int(agg["recortados"].get(c, 0))
    fase34 = {
        "iqr": list(info_iqr.values()),
        "fechas": [{"columna": c, "convertidas": int(agg["conv"].get(c, 0)),
                    "no_convertibles": int(agg["noconv"].get(c, 0)),
                    "formatos": ", ".join(sorted(agg["formatos"].get(c, set()))) or "ninguno"}
                   for c in fechas],
        "moneda": {"columna": col_mon, "filas_usd": agg["usd"]} if col_mon else None,
        "imposibles": {k: int(v) for k, v in agg["imposibles"].items()},
        "aritmetica": {"total": c_tot, "cantidad": c_qty, "precio": c_pre,
                       "inconsistentes": agg["inconsistentes"], "patron_bot": agg["patron_bot"]}
        if c_qty and c_pre and c_tot else None,
        "duplicados": agg["duplicados"]}
    fase5 = {"minmax": [{"columna": c, "min": float(a), "max": float(b)} for c, (a, b) in minmax.items()],
             "discretizacion": {"columna": disc["columna"],
                                "cortes": [round(float(x), 4) for x in disc["bins"]],
                                "reparto": {k: int(v) for k, v in agg["reparto"].items()}}
             if disc else None}
    fase6 = {"validas": validas, "cuarentena": cuar, "causas": causas,
             "campos_esquema": campos_esquema}
    for rel in relaciones:
        log("4", f"{nombre}.{rel['columna']} -> {rel['maestro']}.{rel['llave']}: "
            f"{rel['huerfanos']:,} huerfanos", "ok")
    log("3", f"{nombre}: {sum(x['recortados'] for x in fase34['iqr']):,} valores recortados por "
        f"IQR en {len(fase34['iqr'])} columnas", "ok")
    log("5", f"{nombre}: Min-Max en {len(minmax)} columnas"
        + (f"; '{disc['columna']}' en {'/'.join(ETIQUETAS_SEGMENTO)} por {_nombre_cuantiles()}"
           if disc else ""), "ok")
    log("6", f"{nombre}: {validas:,} validas, {cuar:,} a cuarentena "
        f"({100 * cuar / max(validas + cuar, 1):.2f}%) (pasada 3 de 3, {tiempos['pasada3']}s)", "ok")
    log("7", f"{t_ok} y {t_q}: {validas + cuar:,} filas en una sola transaccion (commit)", "ok")
    q_cols = [c for c in ("motivo_rechazo_cuarentena", "causa_principal") if c in (cols_q or [])]
    m_q_df = pd.concat(m_q).head(50) if m_q else pd.DataFrame(columns=cols_q or [])
    despues = res_ok.sort_index() if res_ok is not None else pd.DataFrame(columns=cols_ok or [])
    return {
        "modo": "bloques",
        "bloques": {"mb": round(mb, 1), "filas_por_bloque": filas, "bloques": n_bloque,
                    "pasadas": 3, "muestra_tipos": len(muestra), "cuantiles_exactos": bool(exactos),
                    "lineas_omitidas": estado["malas"], "lectura_cortada": estado["cortado"],
                    "segundos": tiempos},
        "estructura": estructura, "perfil": perfil, "faltantes": faltantes, "fase34": fase34,
        "fase5": fase5, "fase6": fase6,
        "muestra_limpias": (pd.concat(m_ok).head(25) if m_ok else pd.DataFrame())
        .astype(str).to_dict(orient="records"),
        "muestra_cuarentena": m_q_df[q_cols + [c for c in m_q_df.columns[:6]]].astype(str)
        .to_dict(orient="records") if len(m_q_df) else [],
        "comparacion": comparar_columnas(
            antes, despues, tipos,
            None if (len(muestra) >= n and len(despues) >= validas)
            else f"estimada sobre muestras aleatorias de hasta {k_cmp:,} filas"),
        "_relaciones": [{k: v for k, v in r.items() if k != "_llaves"} for r in relaciones],
        "_totales": (validas, cuar), "_tablas": {t_ok: validas, t_q: cuar},
    }


def extraer_zip_grande(ruta: Path, log: Bitacora) -> list[Path]:
    """Extrae un ZIP enorme a disco por streaming (sin cargarlo en memoria)."""
    out = []
    destino = ruta.parent / "_zip"
    with zipfile.ZipFile(ruta) as z:
        for m in z.infolist():
            if m.is_dir() or "/." in m.filename or m.filename.startswith(("__MACOSX", ".")):
                continue
            d = destino / Path(m.filename).name
            d.parent.mkdir(parents=True, exist_ok=True)
            with z.open(m) as src, open(d, "wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            out.append(d)
    log("0", f"{ruta.name}: ZIP de {ruta.stat().st_size / 1024**2:,.1f} MB extraido a disco "
        f"({len(out)} archivos)", "info")
    return out


# ================================================================ orquestador
def procesar_dataframe(df: pd.DataFrame, nombre: str, log: Bitacora) -> dict:
    df, est = fase0_estructura(df, nombre, log)
    if df.empty:
        log("0", f"{nombre}: sin filas utiles tras limpiar la estructura", "aviso")
        return {"nombre": nombre, "vacio": True, "estructura": est}
    tipos = inferir_tipos(df)
    antes = _estadisticas_antes(df, tipos, log, nombre)
    perfil = fase1_perfil(df, tipos, nombre, log)
    df, faltantes = fase2_faltantes(df, tipos, nombre, log)
    df, f34 = fase34_tipos_outliers_fechas(df, tipos, nombre, log)
    return {"nombre": nombre, "df": df, "tipos": tipos, "estructura": est, "perfil": perfil,
            "faltantes": faltantes, "fase34": f34, "antes": antes}


MAX_FILAS_COMPARACION = 200_000      # por encima, la comparacion usa una muestra fija


def _estadisticas_antes(df: pd.DataFrame, tipos: dict, log: Bitacora, nombre: str) -> dict | None:
    """Solo es un reporte: si fallara, el pipeline sigue igual."""
    try:
        m = df.sample(MAX_FILAS_COMPARACION, random_state=1) if len(df) > MAX_FILAS_COMPARACION \
            else df
        return {c: estadisticas_columna(m[c], tipos[c], True) for c in df.columns if c in tipos}
    except Exception as e:  # noqa: BLE001
        log("1", f"{nombre}: comparacion antes/despues omitida ({type(e).__name__}: {e})", "aviso")
        return None


def _comparacion_memoria(v: dict, ok: pd.DataFrame, log: Bitacora, nombre: str) -> dict | None:
    if not v.get("antes"):
        return None
    try:
        m = ok.sample(MAX_FILAS_COMPARACION, random_state=1) if len(ok) > MAX_FILAS_COMPARACION \
            else ok
        nota = None if len(ok) <= MAX_FILAS_COMPARACION and len(v["df"]) <= MAX_FILAS_COMPARACION \
            else f"estimada sobre muestras aleatorias de {MAX_FILAS_COMPARACION:,} filas"
        return comparar_columnas(v["antes"], m, v["tipos"], nota)
    except Exception as e:  # noqa: BLE001
        log("6", f"{nombre}: comparacion antes/despues omitida ({type(e).__name__}: {e})", "aviso")
        return None


def planear_entradas(entradas: list[Path], umbral_mb: float, log: Bitacora
                     ) -> tuple[list[Path], list[Path]]:
    """Decide que archivos se leen completos a memoria y cuales se procesan por bloques."""
    memoria, bloques = [], []
    for p in entradas:
        try:
            mb = p.stat().st_size / 1024**2
        except OSError:
            mb = 0.0
        if mb > umbral_mb and p.suffix.lower() == ".zip":
            try:
                miembros = extraer_zip_grande(p, log)
            except Exception as e:  # noqa: BLE001
                log("0", f"{p.name}: no se pudo extraer el ZIP ({type(e).__name__}: {e})", "error")
                continue
            for m in miembros:
                grande = m.stat().st_size / 1024**2 > umbral_mb
                (bloques if grande and es_streamable(m) else memoria).append(m)
        elif mb > umbral_mb and es_streamable(p):
            try:
                fmt = _detectar_texto(p)
            except Exception as e:  # noqa: BLE001  (vacio, binario, no decodificable)
                log("0", f"{p.name}: no se pudo interpretar ({type(e).__name__}: {e})", "error")
                continue
            if fmt["tipo"] == "json":
                log("0", f"{p.name}: {mb:,.0f} MB de JSON en un solo documento, que no admite "
                    "bloques; se intenta en memoria (para archivos enormes usa JSON Lines)", "aviso")
                memoria.append(p)
            else:
                bloques.append(p)
        else:
            if mb > umbral_mb:
                log("0", f"{p.name}: {mb:,.0f} MB en formato {p.suffix or 'sin extension'}, que no "
                    "admite bloques; se intenta en memoria (para archivos enormes usa CSV o "
                    "JSON Lines)", "aviso")
            memoria.append(p)
    return memoria, bloques


def ejecutar(entradas: list[Path], salida: Path, avisar: Callable[[str], None] | None = None,
             umbral_mb: float | None = None) -> dict:
    """
    umbral_mb: archivos CSV/TSV/TXT/JSONL mas grandes se procesan por bloques.
    None = motor.bloques.umbral_mb de config.yaml.
    """
    salida.mkdir(parents=True, exist_ok=True)
    log = Bitacora(avisar)
    reporte: dict[str, Any] = {"inicio": datetime.now().isoformat(timespec="seconds"),
                               "fuentes": {}, "ok": False}
    try:
        cfg = aplicar_configuracion()
        reporte["configuracion"] = cfg.huella()
        umbral = BLOQUES["umbral_mb"] if umbral_mb is None else float(umbral_mb)
        en_memoria, por_bloques = planear_entradas(entradas, umbral, log)
        crudas: dict[str, pd.DataFrame] = {}
        for p in en_memoria:
            for k, v in leer_archivo(p, log).items():
                clave = normalizar_nombre(k, 0)
                while clave in crudas:
                    clave += "_b"
                crudas[clave] = v
                log("0", f"fuente '{clave}': {len(v):,} filas leidas", "ok")
        grandes_rutas: dict[str, Path] = {}
        for p in por_bloques:
            clave = normalizar_nombre(p.stem, 0)
            while clave in crudas or clave in grandes_rutas:
                clave += "_b"
            grandes_rutas[clave] = p
        if not crudas and not grandes_rutas:
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

        # archivos enormes: por bloques, cruzados contra los catalogos que si caben en memoria
        grandes: dict[str, dict] = {}
        for k, p in grandes_rutas.items():
            try:
                grandes[k] = procesar_por_bloques(p, k, salida, salida / "resultado.db",
                                                  vivas, log)
            except Exception as e:  # noqa: BLE001
                log("*", f"{k}: fallo el procesamiento por bloques ({type(e).__name__}: {e}); "
                    "fuente omitida sin escribir salidas parciales", "error",
                    traza=traceback.format_exc(limit=4))
        for g in grandes.values():
            reporte["relaciones"].extend(g.pop("_relaciones", []))

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
                "modo": "memoria",
                "estructura": v["estructura"], "perfil": v["perfil"],
                "faltantes": v["faltantes"], "fase34": v["fase34"], "fase5": f5, "fase6": f6,
                "muestra_limpias": ok.head(25).astype(str).to_dict(orient="records"),
                "muestra_cuarentena": q.head(50)[[c for c in ("motivo_rechazo_cuarentena",
                    "causa_principal") if c in q] + [c for c in q.columns[:6]]].astype(str)
                    .to_dict(orient="records") if len(q) else [],
                "comparacion": _comparacion_memoria(v, ok, log, k),
            }
        for k, v in etapa.items():
            if v.get("vacio"):
                reporte["fuentes"][k] = {"vacio": True, "estructura": v["estructura"]}

        for k, g in grandes.items():
            reporte["fuentes"][k] = {c: x for c, x in g.items() if not c.startswith("_")}

        reporte["bd"] = fase7_cargar(cargas, salida / "resultado.db", log)
        for g in grandes.values():
            reporte["bd"].update(g.get("_tablas", {}))
        tot_ok = sum(len(a) for a, _ in cargas.values()) + \
            sum(g["_totales"][0] for g in grandes.values() if "_totales" in g)
        tot_q = sum(len(b) for _, b in cargas.values()) + \
            sum(g["_totales"][1] for g in grandes.values() if "_totales" in g)
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
    argumentos = sys.argv[1:]
    umbral_cli = None
    if "--umbral-mb" in argumentos:
        i = argumentos.index("--umbral-mb")
        try:
            umbral_cli = float(argumentos[i + 1])
        except (IndexError, ValueError):
            print("--umbral-mb necesita un numero, p. ej. --umbral-mb 0.5")
            sys.exit(2)
        del argumentos[i:i + 2]
    if not argumentos:
        print(__doc__)
        sys.exit(2)
    origen = Path(argumentos[0])
    destino = Path(argumentos[1]) if len(argumentos) > 1 else Path("salida_universal")
    if not origen.exists():
        print(f"No existe {origen}")
        sys.exit(2)
    archivos = sorted(p for p in origen.iterdir() if p.is_file() and not p.name.startswith("_")) \
        if origen.is_dir() else [origen]
    r = ejecutar(archivos, destino, umbral_mb=umbral_cli)
    sys.exit(0 if r.get("ok") else 1)
