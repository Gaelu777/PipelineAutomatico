"""
Generador de datasets sinteticos con VERDAD CONOCIDA (ground truth).

La diferencia con un generador normal: aqui sabemos exactamente cuantos errores
de cada tipo se plantaron y en que registros. Eso permite que la suite de QA no
solo verifique "no truena", sino "detecto exactamente los 847 huerfanos que
plante, ni uno mas ni uno menos".

Perfiles de esquema (para probar discordancia entre tienda fisica y digital):
    canonico   nombres identicos a los datasets oficiales del docente
    ingles     la tienda fisica llega con nombres en ingles (ticket_id, qty...)
    erp        nombres de un ERP generico (DOC_NUM, ART_COD, IMP_TOT...)
    minimo     faltan columnas no criticas
    ruidoso    sobran columnas que el pipeline no conoce

Uso:
    python qa/generador_datasets.py <carpeta_salida> --filas 100000 --perfil ingles
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import numpy as np
import pandas as pd

# --------------------------------------------------------------- parametros
CATEGORIAS = ["Juguetería", "Hogar", "Alimentos", "Deportes", "Electrónica", "Ropa"]
SUCURSALES = ["Colima Centro", "Villa de Álvarez", "Manzanillo Puerto", "Tecomán", "Comala"]
METODOS = ["Efectivo", "Tarjeta Crédito", "Tarjeta Débito", "Transferencia", "efectivo"]
ESTATUS_ONLINE = ["COMPLETADO", "PENDIENTE", "CANCELADO", "DEVUELTO", "PAGADO", "completado"]
GATEWAYS = ["MercadoPago", "PayPal", "Stripe", "Conekta"]
PRECIO_RELLENO = 500.00
FACTOR_BOT = 80
TC_USD = 19.50

#: Renombrados por perfil. Clave = nombre canonico.
PERFILES_FISICA = {
    "canonico": {},
    "ingles": {
        "id_ticket": "ticket_id", "fecha_venta": "sale_date", "sucursal": "branch",
        "id_producto": "product_code", "cantidad": "qty", "precio_unitario": "unit_cost",
        "monto_total_mxn": "total_mxn", "id_cliente": "customer_code", "metodo_pago": "pay_method",
    },
    "erp": {
        "id_ticket": "DOC_NUM", "fecha_venta": "FEC_DOC", "sucursal": "CEDI",
        "id_producto": "ART_COD", "cantidad": "CANT", "precio_unitario": "PRE_UNI",
        "monto_total_mxn": "IMP_TOT", "id_cliente": "CLI_COD", "metodo_pago": "FOR_PAG",
    },
    "minimo": {},
    "ruidoso": {},
}
PERFILES_ONLINE = {
    "canonico": {},
    "ingles": {},
    "erp": {
        "order_id": "WEB_FOLIO", "order_timestamp": "WEB_TS", "product_id": "ART_COD",
        "units": "CANT", "unit_price": "PRE_UNI", "total_amount": "IMP_TOT",
        "customer_email": "MAIL_CLI", "order_status": "EST_PED",
    },
    "minimo": {},
    "ruidoso": {},
}


def generar(
    filas_fisica: int,
    filas_online: int,
    n_productos: int = 50,
    perfil: str = "canonico",
    tasa_corrupcion: float = 1.0,
    semilla: int = 20261026,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], dict]:
    """
    Devuelve (catalogo, tienda_fisica, ventas_online, verdad_conocida).

    `tasa_corrupcion` escala la cantidad de errores plantados: 1.0 reproduce
    proporciones parecidas a los datos reales del docente, 0.0 genera datos
    perfectos y 3.0 triplica la suciedad.
    """
    rng = np.random.default_rng(semilla)
    pyrng = random.Random(semilla)
    verdad: dict[str, int] = {}

    # ------------------------------------------------------------- catalogo
    cat = pd.DataFrame({
        "id_producto": [f"PROD-{i:03d}" for i in range(1, n_productos + 1)],
        "nombre_producto": [
            f"Artículo {CATEGORIAS[i % len(CATEGORIAS)]} Ref-{i:03d}"
            for i in range(1, n_productos + 1)
        ],
        "categoria": [CATEGORIAS[i % len(CATEGORIAS)] for i in range(1, n_productos + 1)],
        "precio_base_mxn": np.round(rng.uniform(80, 3000, n_productos), 2),
        "stock_minimo": rng.integers(5, 40, n_productos),
        "proveedor": [f"Proveedor-{pyrng.choice(['Norte','Centro','Sur'])}"
                      for _ in range(n_productos)],
    })
    precio_cat = dict(zip(cat["id_producto"], cat["precio_base_mxn"]))

    # --------------------------------------------------------- tienda fisica
    n = filas_fisica
    prod = rng.choice(cat["id_producto"], n)
    cantidad = rng.integers(1, 6, n).astype(float)
    precio = np.array([precio_cat[p] for p in prod])

    fis = pd.DataFrame({
        "id_ticket": [f"POS-2026-{i:07d}" for i in range(1, n + 1)],
        "fecha_venta": pd.to_datetime("2026-09-01") + pd.to_timedelta(rng.integers(0, 46, n), "D"),
        "sucursal": rng.choice(SUCURSALES, n),
        "id_producto": prod,
        "cantidad": cantidad,
        "precio_unitario": precio,
        "monto_total_mxn": np.round(cantidad * precio, 2),
        "id_cliente": [f"CTE-{x:04d}" for x in rng.integers(1, 900, n)],
        "metodo_pago": rng.choice(METODOS, n),
    })

    def cuantos(pct: float) -> int:
        return max(0, int(n * pct * tasa_corrupcion))

    # Fechas en tres formatos distintos (un tercio cada uno).
    tercio = n // 3
    fechas = fis["fecha_venta"]
    fis["fecha_venta"] = (
        fechas.dt.strftime("%Y-%m-%d").where(fis.index < tercio,
        fechas.dt.strftime("%d/%m/%Y").where(fis.index < 2 * tercio,
        fechas.dt.strftime("%d-%m-%Y")))
    )
    verdad["fisica_fechas_no_iso"] = n - tercio

    # --- huerfanos: llave con formato valido que no existe en el catalogo
    k = cuantos(0.045)
    idx = rng.choice(n, k, replace=False)
    fis.loc[idx, "id_producto"] = [f"PROD-{x:03d}" for x in rng.integers(900, 999, k)]
    fis.loc[idx, "precio_unitario"] = PRECIO_RELLENO
    fis.loc[idx, "monto_total_mxn"] = np.round(fis.loc[idx, "cantidad"] * PRECIO_RELLENO, 2)
    verdad["fisica_huerfanos"] = k
    verdad["fisica_precio_relleno"] = k

    # --- cantidades imposibles (<= 0)
    k = cuantos(0.018)
    idx = rng.choice(n, k, replace=False)
    fis.loc[idx, "cantidad"] = rng.choice([0, -1], k)
    verdad["fisica_cantidad_imposible"] = k

    # --- outliers de monto (sin tocar la aritmetica)
    k = cuantos(0.012)
    idx = rng.choice(n, k, replace=False)
    fis.loc[idx, "cantidad"] = rng.integers(80, 500, k)
    fis.loc[idx, "monto_total_mxn"] = np.round(
        fis.loc[idx, "cantidad"] * fis.loc[idx, "precio_unitario"], 2)
    verdad["fisica_outliers_monto"] = k

    # --- nulos en monto (recalculables desde cantidad x precio)
    k = cuantos(0.030)
    idx = rng.choice(n, k, replace=False)
    fis.loc[idx, "monto_total_mxn"] = np.nan
    verdad["fisica_nulos_monto"] = k

    # --- nulos en cliente (se imputan con etiqueta ANONIMO)
    k = cuantos(0.025)
    idx = rng.choice(n, k, replace=False)
    fis.loc[idx, "id_cliente"] = np.nan
    verdad["fisica_nulos_cliente"] = k

    # --- duplicados exactos
    k = cuantos(0.033)
    if k:
        dup = fis.sample(k, random_state=semilla).copy()
        fis = pd.concat([fis, dup], ignore_index=True)
    verdad["fisica_duplicados_exactos"] = k
    verdad["fisica_filas_total"] = len(fis)

    # --------------------------------------------------------------- online
    m = filas_online
    prod_o = rng.choice(cat["id_producto"], m)
    units = rng.integers(1, 4, m).astype(float)
    precio_o = np.array([precio_cat[p] for p in prod_o])
    moneda = rng.choice(["MXN", "USD"], m, p=[0.68, 0.32])
    precio_fuente = np.where(moneda == "USD", np.round(precio_o / TC_USD, 2), precio_o)

    onl = pd.DataFrame({
        "order_id": [f"WEB-{i:07d}" for i in range(1, m + 1)],
        "order_timestamp": pd.to_datetime("2026-09-01") + pd.to_timedelta(
            rng.integers(0, 46 * 24 * 3600, m), "s"),
        "product_id": prod_o,
        "units": units,
        "currency": moneda,
        "unit_price": precio_fuente,
        "total_amount": np.round(units * precio_fuente, 2),
        "customer_email": [f"usuario_{x}@gmail.com" for x in rng.integers(1000, 9999, m)],
        "shipping_address": [f"Calle {x} No. {y}, Colima"
                             for x, y in zip(rng.integers(1, 99, m), rng.integers(100, 999, m))],
        "order_status": rng.choice(ESTATUS_ONLINE, m),
        "payment_gateway": rng.choice(GATEWAYS, m),
    })

    def cuantos_o(pct: float) -> int:
        return max(0, int(m * pct * tasa_corrupcion))

    # Mitad de las marcas de tiempo como epoch (una en segundos, otra en ms).
    mitad = m // 2
    ts = onl["order_timestamp"]
    # Resolucion explicita a segundos: pandas 3.0 usa microsegundos por defecto,
    # asi que un astype("int64") crudo daria epochs de 7 digitos en vez de 10.
    epoch_s = ts.astype("datetime64[s]").astype("int64").astype(object)
    onl["order_timestamp"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S").astype(object)
    onl.loc[mitad:, "order_timestamp"] = epoch_s.loc[mitad:]
    verdad["online_epoch"] = m - mitad

    # --- bots: total inflado exactamente x80
    k = cuantos_o(0.020)
    idx = rng.choice(m, k, replace=False)
    onl.loc[idx, "total_amount"] = np.round(
        onl.loc[idx, "units"] * onl.loc[idx, "unit_price"] * FACTOR_BOT, 2)
    verdad["online_bots"] = k

    # --- huerfanos
    k = cuantos_o(0.055)
    idx = rng.choice(m, k, replace=False)
    onl.loc[idx, "product_id"] = [f"PROD-{x:03d}" for x in rng.integers(900, 999, k)]
    verdad["online_huerfanos"] = k

    # --- correos: doble arroba, sin arroba y vacios
    k1, k2, k3 = cuantos_o(0.018), cuantos_o(0.015), cuantos_o(0.013)
    libres = list(rng.permutation(m))
    i1, i2, i3 = libres[:k1], libres[k1:k1 + k2], libres[k1 + k2:k1 + k2 + k3]
    onl.loc[i1, "customer_email"] = "usuario@@gmail..com"
    onl.loc[i2, "customer_email"] = "sin_arroba_dominio.org"
    onl.loc[i3, "customer_email"] = ""
    verdad["online_correo_doble_arroba"] = k1
    verdad["online_correo_sin_arroba"] = k2
    verdad["online_correo_vacio"] = k3
    verdad["online_correos_malos"] = k1 + k2 + k3

    # --- nulos en direccion
    k = cuantos_o(0.022)
    onl.loc[rng.choice(m, k, replace=False), "shipping_address"] = np.nan
    verdad["online_nulos_direccion"] = k

    verdad["online_usd"] = int((moneda == "USD").sum())
    verdad["online_filas_total"] = len(onl)

    # ------------------------------------------------- aplicar perfil de esquema
    ren_f = PERFILES_FISICA.get(perfil, {})
    ren_o = PERFILES_ONLINE.get(perfil, {})
    if ren_f:
        fis = fis.rename(columns=ren_f)
    if ren_o:
        onl = onl.rename(columns=ren_o)

    if perfil == "minimo":
        fis = fis.drop(columns=["metodo_pago", "id_cliente"], errors="ignore")
        onl = onl.drop(columns=["payment_gateway", "shipping_address"], errors="ignore")
    if perfil == "ruidoso":
        fis["campo_extra_erp"] = "X"
        fis["observaciones_libres"] = "sin observaciones"
        onl["utm_source"] = "google"
        onl["ab_test_bucket"] = rng.choice(["A", "B"], len(onl))

    verdad["perfil_esquema"] = perfil
    verdad["tasa_corrupcion"] = tasa_corrupcion

    registros = json.loads(onl.to_json(orient="records", date_format="iso"))
    return cat, fis, registros, verdad


def main() -> None:
    ap = argparse.ArgumentParser(description="Genera datasets sinteticos con verdad conocida")
    ap.add_argument("salida", type=Path)
    ap.add_argument("--filas", type=int, default=10_000, help="filas de tienda fisica")
    ap.add_argument("--filas-online", type=int, default=None)
    ap.add_argument("--productos", type=int, default=50)
    ap.add_argument("--perfil", default="canonico",
                    choices=list(PERFILES_FISICA.keys()))
    ap.add_argument("--corrupcion", type=float, default=1.0)
    ap.add_argument("--semilla", type=int, default=20261026)
    a = ap.parse_args()

    online = a.filas_online if a.filas_online is not None else a.filas
    a.salida.mkdir(parents=True, exist_ok=True)

    cat, fis, onl, verdad = generar(
        a.filas, online, a.productos, a.perfil, a.corrupcion, a.semilla)

    cat.to_csv(a.salida / "catalogo_productos.csv", index=False, encoding="utf-8")
    fis.to_csv(a.salida / "ventas_tienda_fisica.csv", index=False, encoding="utf-8")
    with open(a.salida / "ventas_online.json", "w", encoding="utf-8") as f:
        json.dump(onl, f, ensure_ascii=False)
    with open(a.salida / "_verdad_conocida.json", "w", encoding="utf-8") as f:
        json.dump(verdad, f, indent=2, ensure_ascii=False)

    print(f"Generado en {a.salida}  (perfil={a.perfil}, corrupcion={a.corrupcion}x)")
    print(f"  catalogo_productos.csv    {len(cat):>9,} filas")
    print(f"  ventas_tienda_fisica.csv  {len(fis):>9,} filas")
    print(f"  ventas_online.json        {len(onl):>9,} filas")
    print("  verdad conocida:")
    for k, v in verdad.items():
        print(f"    {k:<34} {v}")


if __name__ == "__main__":
    main()
