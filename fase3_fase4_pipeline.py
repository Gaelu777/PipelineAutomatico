#!/usr/bin/env python3
"""
Proyecto Integrador ETL - Ingeniería de Datos (ATD-2602)
FASE 3 (Outliers con IQR / capping) y FASE 4 (Integración, unificación y huérfanos)

Uso:  python fase3_fase4_pipeline.py <carpeta_fase2> <carpeta_datasets> <carpeta_salida>
      python fase3_fase4_pipeline.py salidas_fase2 datasets salidas_fase4

Entrada : <fuente>_fase2.csv (salida de fase1_fase2_pipeline.py) + catálogos maestros crudos.
Salida  : <fuente>_fase4.csv, unificado_grupo1.csv / unificado_grupo2.csv, umbrales_iqr.csv,
          log_outliers.csv, eliminados_imposibles.csv, log_fase4.csv, resumen_fase3_fase4.json

Orden por fuente: moneda USD->MXN y homologación de mayúsculas (F4) -> imposibles (F3) -> IQR/capping (F3)
-> fechas y cruce con catálogo (F4). La moneda va primero para que los cuartiles no mezclen USD con MXN,
y la homologación para que 'RETIRO' y 'retiro' caigan en el mismo grupo del IQR por tipo_operacion.
NO hace Min-Max ni discretización (Fase 5), ni valida correos/CLABEs ni desvía a cuarentena (Fase 6).
"""
import json, re, sys
from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("salidas_fase2")
DATASETS = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("datasets")
OUT = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("salidas_fase4")
FACTOR_IQR = 1.5
TASA_USD = 19.50
ZONA = "America/Mexico_City"          # todas las fechas se expresan en hora local de México
MIN_GRUPO_IQR = 8                     # con menos datos los cuartiles no son confiables

MAESTROS = {
    "catalogo_productos": dict(archivo="catalogo_productos.csv", llave="id_producto",
                               traer=["nombre_producto", "categoria"]),
    "catalogo_cuentas":   dict(archivo="catalogo_cuentas.csv", llave="id_cuenta",
                               traer=["tipo_cuenta", "estado_cuenta"]),
}

# imposibles: (columna, regla, motivo). capping: (columna, columna de grupo o None, es_entero)
FUENTES = {
    "ventas_tienda_fisica": dict(grupo=1, canal="FISICA", llave="id_ticket", fk="id_producto",
        maestro="catalogo_productos", fecha="fecha_venta", tipo_fecha="texto",
        formatos=["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"],
        numericas=["cantidad", "precio_unitario", "monto_total_mxn"],
        imposibles=[("cantidad", "<=0", "cantidad <= 0")],
        capping=[("cantidad", None, True), ("monto_total_mxn", None, False)],
        homologar=["sucursal", "metodo_pago"]),
    "ventas_online": dict(grupo=1, canal="ONLINE", llave="order_id", fk="product_id",
        maestro="catalogo_productos", fecha="order_timestamp", tipo_fecha="iso_epoch",
        numericas=["units", "unit_price", "total_amount"],
        moneda=["unit_price", "total_amount"],
        imposibles=[("units", "<=0", "cantidad <= 0")],
        capping=[("units", None, True), ("total_amount", None, False)],
        homologar=["order_status", "payment_gateway"]),
    "transacciones_atm": dict(grupo=2, canal="ATM", llave="id_transaccion", fk="id_cuenta",
        maestro="catalogo_cuentas", fecha="fecha_hora", tipo_fecha="texto",
        formatos=["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M",
                  "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"],
        numericas=["monto_mxn", "comision_aplicada"],
        imposibles=[("monto_mxn", "<0", "monto negativo")],
        capping=[("monto_mxn", "tipo_operacion", False)],
        homologar=["tipo_operacion", "resultado"]),
    "transferencias_digitales": dict(grupo=2, canal="DIGITAL", llave="transfer_id", fk="account_id",
        maestro="catalogo_cuentas", fecha="timestamp", tipo_fecha="iso_epoch",
        numericas=["amount"],
        moneda=["amount"],
        imposibles=[],
        capping=[("amount", None, False)],
        homologar=["concept", "status", "platform"]),
}

# esquema común por grupo: columna destino -> columna de cada fuente (None = no aplica)
ESQUEMA = {
    1: {"id_registro":         {"ventas_tienda_fisica": "id_ticket", "ventas_online": "order_id"},
        "id_producto":         {"ventas_tienda_fisica": "id_producto", "ventas_online": "product_id"},
        "nombre_producto":     {"ventas_tienda_fisica": "nombre_producto", "ventas_online": "nombre_producto"},
        "categoria":           {"ventas_tienda_fisica": "categoria", "ventas_online": "categoria"},
        "cliente":             {"ventas_tienda_fisica": "id_cliente", "ventas_online": "customer_email"},
        "cantidad":            {"ventas_tienda_fisica": "cantidad", "ventas_online": "units"},
        "precio_unitario_mxn": {"ventas_tienda_fisica": "precio_unitario", "ventas_online": "unit_price"},
        "monto_mxn":           {"ventas_tienda_fisica": "monto_total_mxn", "ventas_online": "total_amount"},
        "monto_mxn_original":  {"ventas_tienda_fisica": "monto_total_mxn_original", "ventas_online": "total_amount_original"},
        "metodo_pago":         {"ventas_tienda_fisica": "metodo_pago", "ventas_online": "payment_gateway"},
        "estatus":             {"ventas_tienda_fisica": None, "ventas_online": "order_status"}},
    2: {"id_registro":         {"transacciones_atm": "id_transaccion", "transferencias_digitales": "transfer_id"},
        "id_cuenta":           {"transacciones_atm": "id_cuenta", "transferencias_digitales": "account_id"},
        "tipo_cuenta":         {"transacciones_atm": "tipo_cuenta", "transferencias_digitales": "tipo_cuenta"},
        "estado_cuenta":       {"transacciones_atm": "estado_cuenta", "transferencias_digitales": "estado_cuenta"},
        "tipo_operacion":      {"transacciones_atm": "tipo_operacion", "transferencias_digitales": "concept"},
        "monto_mxn":           {"transacciones_atm": "monto_mxn", "transferencias_digitales": "amount"},
        "monto_mxn_original":  {"transacciones_atm": "monto_mxn_original", "transferencias_digitales": "amount_original"},
        "comision_mxn":        {"transacciones_atm": "comision_aplicada", "transferencias_digitales": None},
        "estatus":             {"transacciones_atm": "resultado", "transferencias_digitales": "status"}},
}
COMUNES = ["fecha", "moneda_original", "campos_imputados", "flag_outlier", "columnas_recortadas",
           "flag_fecha_invalida", "flag_huerfano", "motivo_rechazo_cuarentena"]

ISO_FECHA = re.compile(r"\d{4}-\d{2}-\d{2}")

# ----------------------------------------------------------------------------- utilidades
def cargar_fase2(ruta: Path, cfg: dict, nombre: str, log: list) -> pd.DataFrame:
    """Todo se lee como texto y solo las columnas numéricas se convierten (errores -> NaN, registrados)."""
    df = pd.read_csv(ruta, dtype=str, keep_default_na=False, na_values=[""])
    for c in cfg["numericas"]:
        if c not in df:
            registrar(log, nombre, f"columna faltante {c}", 0, 0, "se omite")
            continue
        num = pd.to_numeric(df[c], errors="coerce")
        malos = int((num.isna() & df[c].notna()).sum())
        if malos:
            registrar(log, nombre, f"{c} no numerico", malos, 0, "convertido a NaN")
        df[c] = num
    if "flag_total_inconsistente" in df:
        df["flag_total_inconsistente"] = df["flag_total_inconsistente"].str.upper().eq("TRUE")
    if "campos_imputados" not in df:
        df["campos_imputados"] = "NINGUNO"
    return df

def registrar(log: list, fuente: str, metrica: str, antes, despues, detalle=""):
    log.append(dict(fuente=fuente, metrica=metrica, antes=antes, despues=despues, detalle=detalle))

# ----------------------------------------------------------------------------- FASE 4 (moneda, antes del IQR)
def convertir_moneda(df: pd.DataFrame, cfg: dict, nombre: str, log: list) -> pd.DataFrame:
    """USD -> MXN a 19.50 según currency. Guarda el valor sin convertir y la moneda original."""
    cols = [c for c in cfg.get("moneda", []) if c in df]
    if not cols or "currency" not in df:
        return df
    df["currency"] = df["currency"].str.strip().str.upper()
    usd = df["currency"].eq("USD")
    otras = ~df["currency"].isin(["USD", "MXN"])
    df["moneda_original"] = df["currency"]
    for c in cols:
        df[f"{c}_sin_convertir"] = df[c]
        df.loc[usd, c] = (df.loc[usd, c] * TASA_USD).round(2)
    df.loc[usd, "currency"] = "MXN"
    if "total_recalculado" in df:
        # el total recalculado de la fase 2 estaba en la moneda original
        df["total_recalculado"] = (df["units"] * df["unit_price"]).round(2)
    registrar(log, nombre, "filas en USD", int(usd.sum()), int(df["currency"].eq("USD").sum()),
              f"{', '.join(cols)} x {TASA_USD}")
    if otras.any():
        registrar(log, nombre, "moneda desconocida", int(otras.sum()), int(otras.sum()),
                  "sin convertir; revisar en fase 6")
    return df

# ----------------------------------------------------------------------------- FASE 3
def eliminar_imposibles(df: pd.DataFrame, cfg: dict, nombre: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quita solo valores físicamente imposibles, antes de calcular cuartiles."""
    quitar = pd.Series(False, index=df.index)
    motivo = pd.Series("", index=df.index)
    for col, regla, texto in cfg["imposibles"]:
        if col not in df:
            continue
        m = df[col] <= 0 if regla == "<=0" else df[col] < 0
        motivo[m & ~quitar] = texto
        quitar |= m
    eliminados = df[quitar].assign(fuente=nombre, motivo_eliminacion=motivo[quitar])
    return df[~quitar].reset_index(drop=True), eliminados

def limites_iqr(s: pd.Series, entero: bool) -> dict | None:
    s = s.dropna()
    if len(s) < MIN_GRUPO_IQR:
        return None
    q1, q3 = s.quantile([.25, .75])
    iqr = q3 - q1
    lo, hi = q1 - FACTOR_IQR * iqr, q3 + FACTOR_IQR * iqr
    if entero:
        # en conteos el límite se redondea hacia dentro para no inventar fracciones de unidad
        lo, hi = np.ceil(lo) + 0.0, np.floor(hi) + 0.0
    else:
        lo, hi = round(lo, 2), round(hi, 2)
    return dict(q1=q1, q3=q3, iqr=iqr, lo=lo, hi=hi)

def capping(df: pd.DataFrame, cfg: dict, nombre: str, umbrales: list, log_out: list, log: list) -> pd.DataFrame:
    """Winsorizing con IQR x 1.5 (global o por grupo). Conserva <col>_original y marca flag_outlier."""
    df["flag_outlier"] = False
    df["columnas_recortadas"] = ""
    for col, por, entero in cfg["capping"]:
        if col not in df:
            continue
        if por and por not in df:
            registrar(log, nombre, f"columna faltante {por}", 0, 0, f"IQR de {col} se calcula global")
            por = None
        df[col] = df[col].astype(float)
        df[f"{col}_original"] = df[col]
        grupos = df.groupby(df[por].fillna("SIN_GRUPO")) if por else [("(global)", df)]
        for g, sub in grupos:
            s = sub[col].copy()
            fila = dict(fuente=nombre, columna=col, grupo=g, n=int(s.notna().sum()),
                        media_antes=round(s.mean(), 2), max_antes=round(s.max(), 2))
            lim = limites_iqr(s, entero)
            if lim is None or lim["iqr"] == 0:
                # iqr = 0 (p. ej. CONSULTA_SALDO siempre en 0) recortaría todo a un solo valor
                nota = "muy pocos datos" if lim is None else "IQR = 0"
                umbrales.append({**fila, "nota": f"sin recorte ({nota})", "n_recortados": 0,
                                 "media_despues": fila["media_antes"], "max_despues": fila["max_antes"]})
                continue
            nuevo = s.clip(lim["lo"], lim["hi"])
            cambio = nuevo.ne(s) & s.notna()
            df.loc[sub.index, col] = nuevo
            idx = cambio[cambio].index
            df.loc[idx, "flag_outlier"] = True
            df.loc[idx, "columnas_recortadas"] = df.loc[idx, "columnas_recortadas"].where(
                df.loc[idx, "columnas_recortadas"] == "", df.loc[idx, "columnas_recortadas"] + ";") + col
            for i in idx:
                log_out.append(dict(fuente=nombre, llave=df.at[i, cfg["llave"]], columna=col, grupo=g,
                                    valor_original=s[i], valor_recortado=nuevo[i],
                                    limite="superior" if s[i] > lim["hi"] else "inferior"))
            umbrales.append({**fila, "q1": round(lim["q1"], 2), "q3": round(lim["q3"], 2),
                             "iqr": round(lim["iqr"], 2), "limite_inf": round(lim["lo"], 2),
                             "limite_sup": round(lim["hi"], 2),
                             "n_recortados_inf": int((s < lim["lo"]).sum()),
                             "n_recortados_sup": int((s > lim["hi"]).sum()),
                             "n_recortados": int(cambio.sum()),
                             "media_despues": round(nuevo.mean(), 2), "max_despues": round(nuevo.max(), 2),
                             "nota": "capping"})
        if entero:
            df[col] = df[col].round().astype("Int64")
            df[f"{col}_original"] = df[f"{col}_original"].round().astype("Int64")
    df["columnas_recortadas"] = df["columnas_recortadas"].replace("", "NINGUNO")
    return df

# ----------------------------------------------------------------------------- FASE 4
def fechas_texto(s: pd.Series, formatos: list) -> tuple[pd.Series, dict]:
    """Prueba cada formato explícito en orden; cada valor se queda con el primero que encaja."""
    s = s.str.strip()
    res = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    usados = {}
    for fmt in formatos:
        pend = res.isna() & s.notna()
        if not pend.any():
            break
        p = pd.to_datetime(s[pend], format=fmt, errors="coerce")
        res[pend] = p
        if p.notna().sum():
            usados[fmt] = int(p.notna().sum())
    return res, usados

def fechas_iso_epoch(s: pd.Series) -> tuple[pd.Series, dict]:
    """
    Mezcla de texto ISO y epoch entero. Epoch: >= 1e11 se toma como milisegundos, si no segundos
    (1e11 s cae en el año 5138, así que no hay ambigüedad para fechas reales). Epoch y textos con zona
    se pasan a UTC y luego a hora de México; textos ISO sin zona ya se consideran hora local.
    """
    s = s.str.strip()
    num = pd.to_numeric(s, errors="coerce")
    es_epoch = num.notna()
    # fuera de 1973-5138 (s) o 1973-5138 (ms) el número no es una fecha real y se deja como inválido
    seg = es_epoch & num.between(1e8, 1e11, inclusive="left")
    ms = es_epoch & num.between(1e11, 1e14, inclusive="left")
    con_zona = ~es_epoch & s.str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", regex=True, na=False)
    sin_zona = ~es_epoch & ~con_zona & s.notna()
    res = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    local = lambda x: x.dt.tz_convert(ZONA).dt.tz_localize(None)
    if seg.any():
        res[seg] = local(pd.to_datetime(num[seg], unit="s", utc=True, errors="coerce"))
    if ms.any():
        res[ms] = local(pd.to_datetime(num[ms], unit="ms", utc=True, errors="coerce"))
    if con_zona.any():
        res[con_zona] = local(pd.to_datetime(s[con_zona], format="ISO8601", utc=True, errors="coerce"))
    if sin_zona.any():
        res[sin_zona] = pd.to_datetime(s[sin_zona], format="ISO8601", errors="coerce")
    usados = {"epoch_segundos": int(seg.sum()), "epoch_milisegundos": int(ms.sum()),
              "epoch_fuera_de_rango": int((es_epoch & ~seg & ~ms).sum()),
              "iso_con_zona": int(con_zona.sum()), "iso_sin_zona": int(sin_zona.sum())}
    return res, {k: v for k, v in usados.items() if v}

def unificar_fechas(df: pd.DataFrame, cfg: dict, nombre: str, log: list) -> pd.DataFrame:
    col = cfg["fecha"]
    if col not in df:
        registrar(log, nombre, f"columna faltante {col}", 0, 0, "sin fecha")
        df["fecha"], df["flag_fecha_invalida"] = np.nan, True
        return df
    original = df[col].astype("string")
    no_iso = int((~original.str.fullmatch(ISO_FECHA.pattern, na=False)).sum())
    if cfg["tipo_fecha"] == "texto":
        dt, usados = fechas_texto(original, cfg["formatos"])
    else:
        dt, usados = fechas_iso_epoch(original)
    df[f"{col}_original"] = df[col]
    df["fecha"] = dt.dt.strftime("%Y-%m-%d")
    df["flag_fecha_invalida"] = df["fecha"].isna()
    fallidas = int(df["flag_fecha_invalida"].sum())
    registrar(log, nombre, f"fechas fuera de YYYY-MM-DD ({col})", no_iso,
              int((~df["fecha"].str.fullmatch(ISO_FECHA.pattern, na=False)).sum()),
              json.dumps(usados, ensure_ascii=False))
    if fallidas:
        ejemplos = df.loc[df["flag_fecha_invalida"], col].astype(str).head(5).tolist()
        registrar(log, nombre, "fechas no convertibles", fallidas, fallidas, f"ejemplos: {ejemplos}")
    return df

def homologar(df: pd.DataFrame, cfg: dict, nombre: str, log: list) -> pd.DataFrame:
    """Mayúsculas y espacios: 'efectivo', 'Efectivo ' y 'EFECTIVO' pasan a un solo valor."""
    for c in cfg["homologar"]:
        if c not in df:
            continue
        antes = df[c].nunique(dropna=True)
        df[c] = df[c].str.strip().str.upper()
        registrar(log, nombre, f"categorias distintas en {c}", antes, df[c].nunique(dropna=True),
                  json.dumps(sorted(df[c].dropna().unique().tolist()), ensure_ascii=False))
    return df

def preparar_maestro(cat: pd.DataFrame, nombre: str, cfg: dict, log: list) -> pd.DataFrame:
    """Verifica llaves duplicadas antes del merge; si las hay se deja la primera para no duplicar filas."""
    cat = cat.copy()
    cat[cfg["llave"]] = cat[cfg["llave"]].astype(str).str.strip()
    dup = int(cat[cfg["llave"]].duplicated().sum())
    registrar(log, nombre, "llaves duplicadas en catalogo", dup, 0,
              "se conserva la primera aparicion" if dup else "catalogo sin duplicados")
    if dup:
        cat = cat.drop_duplicates(cfg["llave"], keep="first")
    return cat[[cfg["llave"]] + [c for c in cfg["traer"] if c in cat]]

def cruzar_catalogo(df: pd.DataFrame, cfg: dict, nombre: str, maestros: dict, log: list) -> pd.DataFrame:
    """merge left con indicator; los huérfanos se marcan pero NO se borran (el desvío es en fase 6)."""
    mcfg, fk = MAESTROS[cfg["maestro"]], cfg["fk"]
    if cfg["maestro"] not in maestros or fk not in df:
        registrar(log, nombre, "cruce con catalogo", len(df), len(df), "omitido: falta catalogo o llave")
        df["flag_huerfano"] = False
        df["motivo_rechazo_cuarentena"] = ""
        return df
    df[fk] = df[fk].str.strip()
    cat = maestros[cfg["maestro"]].rename(columns={mcfg["llave"]: fk})
    n = len(df)
    df = df.merge(cat, on=fk, how="left", indicator=True, validate="many_to_one")
    assert len(df) == n, "el merge duplicó filas"
    df["flag_huerfano"] = df.pop("_merge").eq("left_only")
    motivos = pd.Series("", index=df.index)
    motivos[df["flag_huerfano"]] = "llave huérfana"
    # la fecha no convertible también se reporta, pero se deja la fila para que la fase 6 decida
    sin_fecha = df["flag_fecha_invalida"]
    motivos[sin_fecha] = motivos[sin_fecha].where(motivos[sin_fecha] == "", motivos[sin_fecha] + "; ") + "fecha no convertible"
    df["motivo_rechazo_cuarentena"] = motivos
    huerf = int(df["flag_huerfano"].sum())
    ejemplos = sorted(df.loc[df["flag_huerfano"], fk].dropna().unique().tolist())[:10]
    registrar(log, nombre, f"huerfanos ({fk} vs {cfg['maestro']}.{mcfg['llave']})", n, huerf,
              f"{round(huerf / n * 100, 2) if n else 0}% | llaves: {ejemplos}")
    return df

def unificar(grupo: int, tablas: dict) -> pd.DataFrame:
    """Esquema común del grupo + columna canal; lo que no aplica a una fuente queda vacío."""
    partes = []
    for nombre, df in tablas.items():
        u = pd.DataFrame({"fuente": nombre, "canal": FUENTES[nombre]["canal"]}, index=df.index)
        for destino, origen in ESQUEMA[grupo].items():
            o = origen.get(nombre)
            u[destino] = df[o] if o and o in df else np.nan
        for c in COMUNES:
            u[c] = df[c] if c in df else ("MXN" if c == "moneda_original" else np.nan)
        partes.append(u)
    return pd.concat(partes, ignore_index=True)

# ----------------------------------------------------------------------------- main
def procesar(nombre: str, cfg: dict, maestros: dict, salidas: dict) -> pd.DataFrame:
    log = salidas["log"]
    df = cargar_fase2(BASE / f"{nombre}_fase2.csv", cfg, nombre, log)
    n_in = len(df)
    df = convertir_moneda(df, cfg, nombre, log)                     # F4 antes del IQR
    df = homologar(df, cfg, nombre, log)
    df, elim = eliminar_imposibles(df, cfg, nombre)                 # F3
    salidas["eliminados"].append(elim)
    df = capping(df, cfg, nombre, salidas["umbrales"], salidas["outliers"], log)
    if nombre == "ventas_online":
        n_bot = int(df["flag_total_inconsistente"].sum()) if "flag_total_inconsistente" in df else 0
        registrar(log, nombre, "patron bot (total x80)", n_bot, n_bot,
                  "se recorta total_amount (lo reportado por la tienda); total_recalculado = units * unit_price "
                  "y flag_total_inconsistente se conservan sin recortar para auditoria")
    df = unificar_fechas(df, cfg, nombre, log)                      # F4
    df = cruzar_catalogo(df, cfg, nombre, maestros, log)
    registrar(log, nombre, "filas (entrada - eliminadas = salida)", n_in, len(df),
              f"{n_in} - {len(elim)} = {n_in - len(elim)}")
    df.to_csv(OUT / f"{nombre}_fase4.csv", index=False, encoding="utf-8-sig")
    salidas["filas"].append(dict(fuente=nombre, entrada=n_in, eliminadas=len(elim), salida=len(df),
                                 cuadra=n_in - len(elim) == len(df), outliers=int(df["flag_outlier"].sum()),
                                 huerfanos=int(df["flag_huerfano"].sum()),
                                 fechas_invalidas=int(df["flag_fecha_invalida"].sum())))
    return df

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    salidas = dict(log=[], eliminados=[], umbrales=[], outliers=[], filas=[])
    maestros = {}
    for nombre, mcfg in MAESTROS.items():
        ruta = DATASETS / mcfg["archivo"]
        if not ruta.exists():
            print(f"[omitido] {ruta} no existe"); continue
        maestros[nombre] = preparar_maestro(pd.read_csv(ruta, dtype=str), nombre, mcfg, salidas["log"])

    por_grupo = {1: {}, 2: {}}
    for nombre, cfg in FUENTES.items():
        ruta = BASE / f"{nombre}_fase2.csv"
        if not ruta.exists():
            print(f"[omitido] {ruta} no existe"); continue
        try:
            por_grupo[cfg["grupo"]][nombre] = procesar(nombre, cfg, maestros, salidas)
        except Exception as e:                                       # una fuente rota no detiene el lote
            registrar(salidas["log"], nombre, "error de procesamiento", 0, 0, repr(e))
            print(f"[error] {nombre}: {e!r}")

    for g, tablas in por_grupo.items():
        if tablas:
            unificar(g, tablas).to_csv(OUT / f"unificado_grupo{g}.csv", index=False, encoding="utf-8-sig")

    cols_umb = ["fuente", "columna", "grupo", "n", "q1", "q3", "iqr", "limite_inf", "limite_sup",
                "n_recortados_inf", "n_recortados_sup", "n_recortados", "media_antes", "media_despues",
                "max_antes", "max_despues", "nota"]
    umbrales = pd.DataFrame(salidas["umbrales"]).reindex(columns=cols_umb)
    umbrales.to_csv(OUT / "umbrales_iqr.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(salidas["outliers"], columns=["fuente", "llave", "columna", "grupo", "valor_original",
                 "valor_recortado", "limite"]).to_csv(OUT / "log_outliers.csv", index=False, encoding="utf-8-sig")
    elim = [e for e in salidas["eliminados"] if len(e)]
    (pd.concat(elim, ignore_index=True) if elim else pd.DataFrame(columns=["fuente", "motivo_eliminacion"])
     ).to_csv(OUT / "eliminados_imposibles.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(salidas["log"]).to_csv(OUT / "log_fase4.csv", index=False, encoding="utf-8-sig")
    resumen = dict(parametros=dict(factor_iqr=FACTOR_IQR, tasa_usd_mxn=TASA_USD, zona_horaria=ZONA,
                                   min_datos_por_grupo_iqr=MIN_GRUPO_IQR),
                   filas=salidas["filas"], umbrales=umbrales.to_dict("records"), log=salidas["log"])
    json.dump(resumen, open(OUT / "resumen_fase3_fase4.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2, default=str)
    print("Listo ->", OUT.resolve())

if __name__ == "__main__":
    main()
