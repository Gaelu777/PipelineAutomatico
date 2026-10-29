#!/usr/bin/env python3
"""
Proyecto Integrador ETL - Ingeniería de Datos (ATD-2602)
Ingeniero/a 1 (Calidad, Nulos y Outliers) -> FASE 1 (Diagnóstico) y FASE 2 (Faltantes e imputación)

Uso:  python fase1_fase2_pipeline.py <carpeta_datasets> <carpeta_salida>
      python fase1_fase2_pipeline.py datasets salidas_fase2

Entrada : los 6 archivos crudos (ambos grupos; si solo tienes uno, se omite el otro).
Salida  : <fuente>_fase2.csv (limpio de nulos), perfil_fase1.csv, clasificacion_faltantes.csv,
          log_imputaciones.csv, duplicados_removidos.csv, resumen_fase1_fase2.json
NO toca outliers (Fase 3), fechas/moneda/huérfanos (Fase 4) ni validación (Fase 6).
"""
import json, sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

from configuracion import cargar_o_salir

CFG = cargar_o_salir()
BASE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets")
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("salidas_fase2")
ALPHA = CFG.alfa                     # config.yaml: faltantes.alfa
FACTOR_IQR = CFG.factor_iqr         # config.yaml: outliers.factor_iqr (solo diagnostico)
TASA_USD = CFG.tipo_cambio_usd_mxn  # config.yaml: moneda.tipo_cambio_usd_mxn
VACIOS = {"", "NA", "N/A", "NULL", "NONE", "NAN"}          # texto que en realidad es "dato faltante"

# Configuración por fuente: qué se recalcula, qué se imputa con mediana y qué con etiqueta controlada
FUENTES = {
    "catalogo_productos":   dict(archivo="catalogo_productos.csv", llave="id_producto", grupo=1, maestro=True),
    "ventas_tienda_fisica": dict(archivo="ventas_tienda_fisica.csv", llave="id_ticket", grupo=1,
        recalculos=[("monto_total_mxn", "cantidad", "precio_unitario")],
        etiquetas={"id_cliente": "ANONIMO"}),
    "ventas_online":        dict(archivo="ventas_online.json", llave="order_id", grupo=1,
        etiquetas={"customer_email": "ANONIMO", "shipping_address": "SIN_DIRECCION"}),
    "catalogo_cuentas":     dict(archivo="catalogo_cuentas.csv", llave="id_cuenta", grupo=2, maestro=True),
    "transacciones_atm":    dict(archivo="transacciones_atm.csv", llave="id_transaccion", grupo=2,
        medianas=[("monto_mxn", "tipo_operacion")],
        mar_estructural={"monto_mxn": "tipo_operacion"},   # CONSULTA_SALDO siempre tiene monto 0 -> nunca falta
        etiquetas={"id_cajero": "DESCONOCIDO"}),
    "transferencias_digitales": dict(archivo="transferencias_digitales.json", llave="transfer_id", grupo=2,
        etiquetas={"notification_email": "ANONIMO"}),
}

# ----------------------------------------------------------------------------- utilidades
def cargar(ruta: Path) -> pd.DataFrame:
    if ruta.suffix == ".json":
        return pd.DataFrame(json.load(open(ruta, encoding="utf-8")))
    return pd.read_csv(ruta)

def es_vacio(x) -> bool:
    return isinstance(x, str) and x.strip().upper() in VACIOS

def tipos_inferidos(s: pd.Series) -> str:
    """dtype de pandas; si la columna mezcla tipos Python (p. ej. timestamp ISO + epoch) lo indica."""
    py = sorted({type(v).__name__ for v in s.dropna()})
    return f"mixto ({'/'.join(py)})" if len(py) > 1 else str(s.dtype)

# ----------------------------------------------------------------------------- FASE 1
def perfilar(df: pd.DataFrame, nombre: str, llave: str) -> tuple[pd.DataFrame, dict]:
    """Completitud (nulos reales + vacíos ocultos), unicidad (duplicados exactos) y tipos inferidos."""
    filas = []
    for c in df.columns:
        reales = int(df[c].isna().sum())
        ocultos = int(df[c].map(es_vacio).sum())
        filas.append(dict(fuente=nombre, columna=c, tipo_inferido=tipos_inferidos(df[c]),
                          nulos_reales=reales, vacios_ocultos=ocultos,
                          pct_faltante=round((reales + ocultos) / len(df) * 100, 2),
                          valores_unicos=int(df[c].nunique(dropna=True))))
    g = dict(fuente=nombre, filas=len(df), columnas=df.shape[1],
             duplicados_exactos=int(df.duplicated().sum()),
             duplicados_por_llave=int(df[llave].duplicated().sum()))
    return pd.DataFrame(filas), g

# ----------------------------------------------------------------------------- FASE 2
def clasificar_faltantes(df: pd.DataFrame, col: str, max_niveles: int = 12) -> dict:
    """
    MCAR vs MAR. Se contrasta la indicadora 'dato faltante en col' contra cada variable observada de
    baja cardinalidad: (a) chi-cuadrada de independencia; (b) binomial por nivel contra la tasa global
    con Bonferroni por nº de niveles. Si algún p < 0.05 -> MAR (depende de otra variable observada);
    si ninguno -> MCAR (no se puede probar MNAR con los datos; se asume MCAR por falta de evidencia).
    """
    miss = df[col].isna()
    tasa, mejor = miss.mean(), dict(p=1.0, pred=None, nivel=None, prueba=None)
    for p in df.columns:
        if p == col:
            continue
        s = df[p].dropna().astype(str).str.upper()
        if s.nunique() < 2 or s.nunique() > max_niveles:
            continue
        m = miss.loc[s.index]
        pv = stats.chi2_contingency(pd.crosstab(s, m))[1]
        if pv < mejor["p"]:
            mejor = dict(p=pv, pred=p, nivel="(global)", prueba="chi2")
        for nivel, idx in s.groupby(s).groups.items():
            k, n = int(m.loc[idx].sum()), len(idx)
            pb = min(1.0, stats.binomtest(k, n, tasa).pvalue * s.nunique())
            if pb < mejor["p"]:
                mejor = dict(p=pb, pred=p, nivel=nivel, prueba="binomial+Bonferroni")
    mecanismo = "MAR" if mejor["p"] < ALPHA else "MCAR"
    return dict(columna=col, n_faltantes=int(miss.sum()), pct=round(tasa * 100, 2), mecanismo=mecanismo,
                p_min=round(mejor["p"], 4), variable_asociada=mejor["pred"] if mecanismo == "MAR" else "",
                nivel=mejor["nivel"] if mecanismo == "MAR" else "", prueba=mejor["prueba"] or "")

def marcar(df, mask, col):
    df.loc[mask, "campos_imputados"] = df.loc[mask, "campos_imputados"].where(
        df.loc[mask, "campos_imputados"] == "", df.loc[mask, "campos_imputados"] + ";") + col

def imputar(df: pd.DataFrame, cfg: dict, nombre: str, log: list) -> pd.DataFrame:
    df = df.copy()
    df["campos_imputados"] = ""
    # 1) Recalcular donde la información existe en otras columnas (monto = cantidad * precio)
    for destino, a, b in cfg.get("recalculos", []):
        m = df[destino].isna() & df[a].notna() & df[b].notna()
        df.loc[m, destino] = (df.loc[m, a] * df.loc[m, b]).round(2)
        marcar(df, m, destino)
        log.append(dict(fuente=nombre, columna=destino, estrategia=f"Recalculo {a} * {b}", n_imputados=int(m.sum()),
                        valor_usado="fila a fila", faltantes_restantes=int(df[destino].isna().sum())))
    # 2) Numéricas sesgadas -> mediana (global o por grupo observado). Mediana calculada con datos observados.
    for col, grupo in cfg.get("medianas", []):
        m = df[col].isna()
        med_g = df[col].median()
        med = df.groupby(grupo)[col].transform("median").fillna(med_g)
        df.loc[m, col] = med[m]
        marcar(df, m, col)
        detalle = df.loc[m].groupby(grupo)[col].first().round(2).to_dict()
        log.append(dict(fuente=nombre, columna=col, estrategia=f"Mediana por {grupo}", n_imputados=int(m.sum()),
                        valor_usado=json.dumps(detalle, ensure_ascii=False), faltantes_restantes=int(df[col].isna().sum())))
    # 3) Categóricas -> etiqueta controlada
    for col, etiqueta in cfg.get("etiquetas", {}).items():
        if col not in df.columns:            # la fuente no trajo esta columna
            continue
        m = df[col].isna()
        # Una columna 100% vacia la infiere pandas como float64; escribir texto lanza TypeError.
        if m.all() or df[col].dtype.kind in "fiub":
            df[col] = df[col].astype(object)
        df.loc[m, col] = etiqueta
        marcar(df, m, col)
        log.append(dict(fuente=nombre, columna=col, estrategia="Etiqueta controlada", n_imputados=int(m.sum()),
                        valor_usado=etiqueta, faltantes_restantes=int(df[col].isna().sum())))
    df["campos_imputados"] = df["campos_imputados"].replace("", "NINGUNO")
    return df

# ----------------------------------------------------------------------------- hallazgos para fases siguientes (solo conteos)
def iqr_info(s: pd.Series) -> dict:
    s = s.dropna(); q1, q3 = s.quantile([.25, .75]); i = q3 - q1
    lo, hi = q1 - FACTOR_IQR * i, q3 + FACTOR_IQR * i
    return dict(q1=round(q1, 2), q3=round(q3, 2), limite_inf=round(lo, 2), limite_sup=round(hi, 2),
                outliers=int(((s < lo) | (s > hi)).sum()))

def hallazgos(nombre: str, d: pd.DataFrame, maestro: dict) -> list:
    h = []
    pat = lambda s: s.astype(str).str.replace(r"\d", "9", regex=True).nunique()
    mal_email = lambda s: int((s.dropna().astype(str).map(lambda x: not __import__("re").fullmatch(r"[\w.+-]+@[\w-]+\.[\w.]+", x)) & (s.dropna() != "ANONIMO")).sum())
    if nombre == "ventas_tienda_fisica":
        h += [("Fechas NO ISO (DD/MM/AAAA y DD-MM-AAAA; el día va primero, hay días > 12)", int((~d.fecha_venta.str.fullmatch(r"\d{4}-\d{2}-\d{2}")).sum()), 4),
              ("metodo_pago con 'Efectivo' y 'efectivo' (mayúsculas inconsistentes)", int((d.metodo_pago == "efectivo").sum()), 4),
              ("Cantidades <= 0 (valores imposibles: eliminar)", int((d.cantidad <= 0).sum()), 3),
              ("Cantidades extremas (99 y 500 unidades) -> outliers", int((d.cantidad >= 99).sum()), 3),
              ("id_producto huérfano (no existe en catalogo_productos)", int((~d.id_producto.isin(maestro["catalogo_productos"].id_producto)).sum()), 4)]
    if nombre == "ventas_online":
        tot = (d.units * d.unit_price - d.total_amount).abs() > 0.01
        h += [("Timestamps mezclados: ISO texto y epoch entero", int(d.order_timestamp.map(lambda x: isinstance(x, int)).sum()), 4),
              (f"Pedidos en USD (convertir a MXN @ {TASA_USD:.2f})", int((d.currency == "USD").sum()), 4),
              ("total_amount != units * unit_price (patrón bot, total x80)", int(tot.sum()), 3),
              ("Correos malformados (no vacíos) -> cuarentena/validación", mal_email(d.customer_email), 6),
              ("order_status con 'completado'/'COMPLETADO' (mayúsculas inconsistentes)", int((d.order_status == "completado").sum()), 4),
              ("product_id huérfano (no existe en catalogo_productos)", int((~d.product_id.isin(maestro["catalogo_productos"].id_producto)).sum()), 4)]
    if nombre == "transacciones_atm":
        h += [("Fechas_hora NO ISO (AAAA/MM/DD y DD/MM/AAAA)", int((~d.fecha_hora.str.fullmatch(r"\d{4}-\d{2}-\d{2} .*")).sum()), 4),
              ("resultado con 'EXITOSA'/'exitosa' (mayúsculas inconsistentes)", int((d.resultado == "exitosa").sum()), 4),
              ("id_cuenta huérfana (no existe en catalogo_cuentas)", int((~d.id_cuenta.isin(maestro["catalogo_cuentas"].id_cuenta)).sum()), 4)]
    if nombre == "transferencias_digitales":
        h += [("Timestamps mezclados: ISO con zona horaria y epoch entero", int(d.timestamp.map(lambda x: isinstance(x, int)).sum()), 4),
              (f"Transferencias en USD (convertir a MXN @ {TASA_USD:.2f})", int((d.currency == "USD").sum()), 4),
              ("CLABE con menos de 18 dígitos -> cuarentena", int((d.beneficiary_clabe.astype(str).str.len() != 18).sum()), 6),
              ("Correos malformados (no vacíos) -> cuarentena/validación", mal_email(d.notification_email), 6),
              ("status con 'liquidada'/'LIQUIDADA' (mayúsculas inconsistentes)", int((d.status == "liquidada").sum()), 4),
              ("account_id huérfano (no existe en catalogo_cuentas)", int((~d.account_id.isin(maestro["catalogo_cuentas"].id_cuenta)).sum()), 4)]
    return [dict(fuente=nombre, hallazgo=a, registros=b, fase_responsable=c) for a, b, c in h]

# ----------------------------------------------------------------------------- main
def main():
    OUT.mkdir(parents=True, exist_ok=True)
    perfiles, globales, clasif, logs, dups, hall, sesgo, iqr = [], [], [], [], [], [], [], {}
    crudos, maestros = {}, {}
    for nombre, cfg in FUENTES.items():
        ruta = BASE / cfg["archivo"]
        if not ruta.exists():
            print(f"[omitido] {ruta} no existe"); continue
        crudos[nombre] = cargar(ruta)
        if cfg.get("maestro"):
            maestros[nombre] = crudos[nombre]

    for nombre, raw in crudos.items():
        cfg = FUENTES[nombre]
        # ---- FASE 1: perfilado del dato CRUDO (antes de cualquier transformación)
        p, g = perfilar(raw, nombre, cfg["llave"])
        perfiles.append(p); globales.append(g)
        if cfg.get("maestro"):
            continue                                            # catálogos: solo diagnóstico
        # ---- FASE 2
        d = raw.map(lambda x: np.nan if es_vacio(x) else x)    # vacíos ocultos ('' / 'N/A') -> NaN real
        mask_dup = d.duplicated(keep="first")                   # duplicados exactos (reintentos POS / cajero)
        dups.append(d[mask_dup].assign(fuente=nombre))
        d = d[~mask_dup].reset_index(drop=True)
        for c in d.select_dtypes("number").columns:             # justificación mediana vs media
            s = d[c].dropna()
            sesgo.append(dict(fuente=nombre, columna=c, media=round(s.mean(), 2), mediana=round(s.median(), 2),
                              maximo=round(s.max(), 2), asimetria=round(s.skew(), 2)))
        cols_faltantes = [c for c in d.columns if d[c].isna().any()]
        for c in cols_faltantes:
            r = clasificar_faltantes(d, c)
            pred = cfg.get("mar_estructural", {}).get(c)
            if pred and r["mecanismo"] == "MCAR":            # evidencia estructural: niveles que nunca pueden faltar
                for nivel, g in d.groupby(pred):
                    if len(g) >= 20 and g[c].isna().sum() == 0:
                        r.update(mecanismo="MAR", variable_asociada=pred, nivel=str(nivel),
                                 prueba=f"estructural (0 de {len(g)} faltantes; test formal p={r['p_min']})")
                        break
            clasif.append(dict(fuente=nombre, **r))
        hall += hallazgos(nombre, d.copy(), maestros)
        limpio = imputar(d, cfg, nombre, logs)
        if nombre == "ventas_online":                           # auditoría (NO se sobreescribe el total original)
            limpio["total_recalculado"] = (limpio.units * limpio.unit_price).round(2)
            limpio["flag_total_inconsistente"] = (limpio.total_amount - limpio.total_recalculado).abs() > 0.01
        # IQR informativo para la Fase 3 (no se modifica nada)
        for c, ref in [("monto_total_mxn", None), ("total_amount", None), ("monto_mxn", None), ("amount", None)]:
            if c in limpio:
                iqr[f"{nombre}.{c}"] = iqr_info(limpio[c])
        limpio.to_csv(OUT / f"{nombre}_fase2.csv", index=False, encoding="utf-8-sig")
        # perfil DESPUÉS (para el reporte antes/después)
        pd_, _ = perfilar(limpio.drop(columns="campos_imputados"), nombre, cfg["llave"])
        perfiles.append(pd_.assign(momento="despues"))

    perfil = pd.concat([x if "momento" in x else x.assign(momento="antes") for x in perfiles], ignore_index=True)
    perfil.to_csv(OUT / "perfil_fase1.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(clasif).to_csv(OUT / "clasificacion_faltantes.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(logs).to_csv(OUT / "log_imputaciones.csv", index=False, encoding="utf-8-sig")
    if dups:
        pd.concat(dups).to_csv(OUT / "duplicados_removidos.csv", index=False, encoding="utf-8-sig")
    resumen = dict(globales=globales, clasificacion=clasif, log=logs, sesgo=sesgo, iqr=iqr, hallazgos=hall,
                   perfil=perfil.to_dict("records"))
    json.dump(resumen, open(OUT / "resumen_fase1_fase2.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("Listo ->", OUT.resolve())

if __name__ == "__main__":
    main()
