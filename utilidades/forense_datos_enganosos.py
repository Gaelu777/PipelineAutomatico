"""
Analisis forense: datos que PARECEN correctos pero no lo son.

No busca nulos ni formatos rotos (eso ya lo atrapan las fases 1 y 2). Busca los
errores silenciosos: registros que pasan cualquier validacion de tipo y formato,
que se ven normales en un head(), y que aun asi son falsos.

Uso:  python utilidades/forense_datos_enganosos.py datasets salidas_fase4
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

DATASETS = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets")
FASE4 = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("salidas_fase4")

TOL = 0.01  # tolerancia de redondeo en pesos
hallazgos: list[dict] = []


def reportar(codigo, titulo, afectados, total, porque, evidencia, accion):
    hallazgos.append({
        "codigo": codigo,
        "titulo": titulo,
        "afectados": afectados,
        "total": total,
        "pct": round(100 * afectados / total, 2) if total else 0.0,
        "por_que_enganan": porque,
        "evidencia": evidencia,
        "accion_fase6": accion,
    })
    print(f"\n[{codigo}] {titulo}")
    print(f"  Afectados: {afectados} de {total} ({100*afectados/total:.2f}%)" if total else "  sin datos")
    print(f"  Por que engana: {porque}")
    print(f"  Evidencia: {evidencia}")
    print(f"  Accion Fase 6: {accion}")


print("=" * 78)
print("  DATOS QUE PARECEN CORRECTOS PERO NO LO SON")
print("=" * 78)

cat = pd.read_csv(DATASETS / "catalogo_productos.csv")
fis = pd.read_csv(DATASETS / "ventas_tienda_fisica.csv")
onl = pd.json_normalize(json.load(open(DATASETS / "ventas_online.json", encoding="utf-8")))

# ---------------------------------------------------------------- E-01
# Aritmetica interna rota: el total no es cantidad * precio.
fis_n = fis.copy()
for c in ("cantidad", "precio_unitario", "monto_total_mxn"):
    fis_n[c] = pd.to_numeric(fis_n[c], errors="coerce")
calc = fis_n["cantidad"] * fis_n["precio_unitario"]
mal_fis = fis_n[(calc - fis_n["monto_total_mxn"]).abs() > TOL].dropna(subset=["monto_total_mxn"])

onl_n = onl.copy()
for c in ("units", "unit_price", "total_amount"):
    onl_n[c] = pd.to_numeric(onl_n[c], errors="coerce")
calc_o = onl_n["units"] * onl_n["unit_price"]
mal_onl = onl_n[(calc_o - onl_n["total_amount"]).abs() > TOL]
ratio = (onl_n["total_amount"] / calc_o).round(2)
bots = onl_n[ratio == 80.0]

ej = mal_onl.head(1)
reportar(
    "E-01", "Totales que no cuadran con cantidad x precio",
    len(mal_fis) + len(mal_onl), len(fis) + len(onl),
    "Los tres campos son numeros validos y positivos. Ninguna validacion de tipo "
    "los rechaza: solo se detectan recalculando la operacion.",
    f"Fisica {len(mal_fis)} filas, online {len(mal_onl)}. Ej. {ej.iloc[0]['order_id']}: "
    f"{ej.iloc[0]['units']} x {ej.iloc[0]['unit_price']} = {calc_o.loc[ej.index[0]]:.2f} "
    f"pero total_amount = {ej.iloc[0]['total_amount']:.2f}",
    "Validador que recalcula el total y rechaza si difiere mas de 1 centavo.",
)

reportar(
    "E-02", "Bots: total inflado exactamente 80 veces",
    len(bots), len(onl),
    "El factor es constante y exacto (x80), no es ruido aleatorio. Un promedio de "
    "ticket los absorbe y parecen ventas grandes legitimas.",
    f"{len(bots)} pedidos con total = 80 x (units x unit_price). "
    f"Ej. {bots.iloc[0]['order_id']}: {bots.iloc[0]['units']} x {bots.iloc[0]['unit_price']:.2f} "
    f"-> {bots.iloc[0]['total_amount']:.2f}",
    "Marcar como fraude_bot y enviar a cuarentena, no solo recortar por IQR.",
)

# ---------------------------------------------------------------- E-03
# Valor por defecto disfrazado de dato real.
cat_p = cat.set_index("id_producto")["precio_base_mxn"].to_dict()
fis_c = fis_n[fis_n["id_producto"].isin(cat_p)].copy()
fis_c["precio_catalogo"] = fis_c["id_producto"].map(cat_p)
desv = (fis_c["precio_unitario"] - fis_c["precio_catalogo"]).abs() / fis_c["precio_catalogo"]
coherentes = int((desv < 1e-9).sum())

huerf_fis = fis_n[~fis_n["id_producto"].isin(cat_p)]
magico = huerf_fis[huerf_fis["precio_unitario"] == 500.0]
reportar(
    "E-03", "Precio 500.00 exacto: valor por defecto, no precio real",
    len(magico), len(fis),
    "500.00 es un importe perfectamente plausible para una venta. Pasa cualquier "
    "validacion de rango o de tipo. Lo delata la repeticion: el precio real nunca "
    "cae 29 veces en el mismo numero redondo.",
    f"Los {coherentes} productos del catalogo cotizan a su precio_base_mxn exacto "
    f"(desviacion 0). Los {len(magico)} huerfanos cotizan todos a 500.00, un relleno "
    "que el sistema origen usa cuando no encuentra el producto.",
    "Regla cruzada: precio == 500.00 junto con llave huerfana se trata como "
    "'producto inexistente con precio de relleno', no como venta valida.",
)

# ---------------------------------------------------------------- E-04
# Huerfanos con formato de llave impecable.
ids_cat = set(cat["id_producto"])
huerf_f = fis[~fis["id_producto"].isin(ids_cat)]
huerf_o = onl[~onl["product_id"].isin(ids_cat)]
patron_ok = huerf_o["product_id"].astype(str).str.match(r"^PROD-\d{3}$").sum()
reportar(
    "E-04", "Llaves huerfanas con formato perfecto",
    len(huerf_f) + len(huerf_o), len(fis) + len(onl),
    "Cumplen el patron PROD-NNN, pasan cualquier regex y se ven como un id real. "
    "Solo el cruce contra el catalogo revela que el producto no existe.",
    f"Fisica {len(huerf_f)}, online {len(huerf_o)}. {patron_ok} de los online tienen "
    f"formato valido. Rango real del catalogo: {min(ids_cat)} a {max(ids_cat)}.",
    "Marcar flag_huerfano y desviar a cuarentena con motivo 'llave huerfana'.",
)

# ---------------------------------------------------------------- E-05
# Correos sintacticamente validos pero inservibles.
RE_ESTRICTO = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")
mail = onl["customer_email"].fillna("").astype(str).str.strip()

# Validacion ingenua que mucha gente escribe: "tiene arroba y tiene punto".
pasa_ingenua = mail.str.contains("@", regex=False) & mail.str.contains(".", regex=False)
pasa_estricta = mail.str.match(RE_ESTRICTO)

# Los peligrosos: la ingenua los aprueba, la estricta los rechaza.
enganosos = mail[pasa_ingenua & ~pasa_estricta]
reportar(
    "E-05", "Correos que una validacion ingenua aprueba",
    int(len(enganosos)), len(onl),
    "Tienen arroba, tienen dominio conocido y terminan en .com. Un chequeo del tipo "
    "'@' in correo los da por buenos y quedan en la base como clientes contactables. "
    "Solo un regex estricto ve la doble arroba y el punto duplicado.",
    f"{int(len(enganosos))} correos pasan la validacion ingenua pero fallan la estricta. "
    f"Patrones: {', '.join(sorted(set(enganosos))[:3])}. "
    f"Ademas {int((~pasa_ingenua).sum())} son rechazados por ambas "
    f"(sin arroba o vacios).",
    "Regex estricto con anclas, mas rechazo explicito de '..' y '@@'; a cuarentena.",
)

# ---------------------------------------------------------------- E-06
# Duplicados que no son duplicados exactos.
clave = ["fecha_venta", "sucursal", "id_producto", "cantidad", "monto_total_mxn", "id_cliente"]
clave = [c for c in clave if c in fis.columns]
dup_exactos = int(fis.duplicated().sum())
dup_negocio = int(fis.duplicated(subset=clave, keep=False).sum())
reportar(
    "E-06", "Duplicados de negocio con id_ticket distinto",
    dup_negocio, len(fis),
    "drop_duplicates() no los toca porque el id_ticket es unico. Para el sistema son "
    "dos ventas; en la realidad es la misma venta capturada dos veces.",
    f"Duplicados exactos: {dup_exactos}. Duplicados por llave de negocio "
    f"({', '.join(clave)}): {dup_negocio} filas involucradas.",
    "Deduplicar por llave de negocio conservando el primer ticket; el resto a cuarentena.",
)

# ---------------------------------------------------------------- E-07
# Fechas validas pero fuera del periodo operativo.
f4 = pd.read_csv(FASE4 / "unificado_grupo1.csv", parse_dates=["fecha"])
hoy = pd.Timestamp("2026-10-26")
futuro = f4[f4["fecha"] > hoy]
muy_viejo = f4[f4["fecha"] < pd.Timestamp("2026-01-01")]
reportar(
    "E-07", "Fechas bien formadas pero imposibles para el negocio",
    len(futuro) + len(muy_viejo), len(f4),
    "Son fechas reales del calendario y se convierten sin error a YYYY-MM-DD. "
    "El problema no es el formato sino que caen fuera del periodo de operacion.",
    f"Futuras: {len(futuro)}. Anteriores a 2026: {len(muy_viejo)}. "
    f"Rango observado: {f4['fecha'].min().date()} a {f4['fecha'].max().date()}.",
    "Validador de rango operativo [2026-01-01, hoy]; fuera de rango a cuarentena.",
)

# ---------------------------------------------------------------- E-08
# Moneda: importes ya convertidos o etiquetados al reves.
usd = onl_n[onl_n["currency"].astype(str).str.upper() == "USD"]
if len(usd):
    caros = usd[usd["unit_price"] > 2000]
    reportar(
        "E-08", "Importes en USD con magnitud de pesos",
        len(caros), len(usd),
        "Un unit_price de 3,000 marcado como USD equivale a 58,500 MXN. El numero y la "
        "etiqueta son validos por separado; juntos producen un importe absurdo.",
        f"{len(usd)} pedidos en USD. {len(caros)} con unit_price mayor a 2,000 USD "
        f"(mas de 39,000 MXN al tipo de cambio 19.50).",
        "Validar rango por moneda ANTES de convertir; fuera de rango a cuarentena.",
    )

# ---------------------------------------------------------------- E-09
# Capping del IQR que deja la fila internamente incoherente.
inco = f4[(f4["flag_outlier"]) & f4["columnas_recortadas"].astype(str).str.contains("monto")]
if len(inco):
    sub = inco.dropna(subset=["cantidad", "precio_unitario_mxn", "monto_mxn"])
    rec = sub["cantidad"] * sub["precio_unitario_mxn"]
    rotas = sub[(rec - sub["monto_mxn"]).abs() > TOL]
    reportar(
        "E-09", "Filas que el propio capping dejo incoherentes",
        len(rotas), len(f4),
        "Tras recortar el monto al limite del IQR, el total ya no es cantidad x precio. "
        "La fila se ve limpia y ordenada, pero su aritmetica interna quedo rota por "
        "nuestra propia transformacion.",
        f"{len(rotas)} filas recortadas donde monto != cantidad x precio.",
        "Recalcular el monto tras el capping, o excluir las recortadas del validador "
        "aritmetico usando monto_mxn_original.",
    )

# ---------------------------------------------------------------- E-10
# Estatus que parece terminal pero contradice el resto de la fila.
if "estatus" in f4.columns:
    est = f4["estatus"].fillna("").astype(str).str.upper()
    vals = sorted(v for v in est.unique() if v)
    cancel_con_monto = f4[(est.isin(["CANCELADO", "CANCELLED"])) & (f4["monto_mxn"] > 0)]
    reportar(
        "E-10", "Pedidos cancelados que conservan importe cobrado",
        len(cancel_con_monto), len(f4),
        "El estatus es un valor valido del catalogo y el monto es un numero correcto. "
        "La contradiccion solo existe al leer los dos campos juntos.",
        f"{len(cancel_con_monto)} pedidos CANCELADO con monto_mxn mayor a 0. "
        f"Estatus presentes: {vals[:6]}",
        "Regla cruzada: si estatus es CANCELADO, el monto facturable debe ser 0.",
    )

# ---------------------------------------------------------------- salida
out = pd.DataFrame(hallazgos)
FASE4.mkdir(parents=True, exist_ok=True)
out.to_csv(FASE4 / "datos_enganosos.csv", index=False, encoding="utf-8-sig")

print("\n" + "=" * 78)
print(f"  {len(hallazgos)} tipos de error silencioso detectados")
print(f"  Guardado en {FASE4 / 'datos_enganosos.csv'}")
print("=" * 78)
