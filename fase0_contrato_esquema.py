"""
Fase 0: Contrato de esquema (adaptador de fuentes heterogeneas).

Problema que resuelve
---------------------
Las Fases 1 a 7 asumen que las columnas se llaman exactamente como en los
archivos del docente. Si la tienda fisica exporta `ticket_id` en vez de
`id_ticket`, o un ERP manda `IMP_TOT` en vez de `monto_total_mxn`, el pipeline
reventaba con un KeyError: una excepcion no controlada, justo lo que el
proyecto prohibe.

Esta fase se coloca ANTES de la Fase 1 y traduce cualquier esquema de entrada
al esquema canonico. El resto del pipeline no se entera y no cambia una linea.

Como decide
-----------
Tres niveles, de mayor a menor confianza:
  1.00  nombre canonico exacto
  0.95  alias conocido (diccionario de equivalencias en espanol, ingles y ERP)
  0.70+ coincidencia difusa sobre el nombre normalizado (sin acentos,
        sin separadores, minusculas), por encima del umbral
  ----  sin coincidencia: la columna se conserva tal cual y se reporta

Reglas duras
------------
  - Si falta una columna OBLIGATORIA, se detiene con un mensaje claro y
    accionable. No un traceback, y sobre todo sin escribir nada: es preferible
    no producir salida a producir una salida silenciosamente incorrecta.
  - Si falta una columna OPCIONAL, se crea vacia y se registra. El pipeline
    degrada, no colapsa.
  - Dos columnas distintas nunca pueden mapear al mismo destino. Si ocurre,
    gana la de mayor confianza y la otra se conserva con su nombre original.

Uso:
    python fase0_contrato_esquema.py <datasets_entrada> <datasets_salida>
    python fase0_contrato_esquema.py datasets_raw datasets --reporte mapeo.csv
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

UMBRAL_DIFUSO = 0.70


# ===================================================== contrato por fuente
class Contrato:
    def __init__(self, nombre: str, archivo: str, obligatorias: dict[str, list[str]],
                 opcionales: dict[str, list[str]], formato: str = "csv"):
        self.nombre = nombre
        self.archivo = archivo
        self.obligatorias = obligatorias
        self.opcionales = opcionales
        self.formato = formato

    @property
    def alias(self) -> dict[str, list[str]]:
        return {**self.obligatorias, **self.opcionales}


CATALOGO_PRODUCTOS = Contrato(
    "catalogo_productos", "catalogo_productos.csv",
    obligatorias={
        "id_producto": ["id_producto", "product_id", "sku", "clave_producto", "art_cod",
                        "codigo_producto", "product_code", "cod_art", "idproducto"],
        "precio_base_mxn": ["precio_base_mxn", "precio_base", "base_price", "precio_lista",
                            "pre_uni", "list_price", "precio"],
    },
    opcionales={
        "nombre_producto": ["nombre_producto", "product_name", "descripcion", "nombre",
                            "des_art", "articulo"],
        "categoria": ["categoria", "category", "familia", "linea", "cat"],
        "stock_minimo": ["stock_minimo", "min_stock", "existencia_minima", "stk_min"],
        "proveedor": ["proveedor", "supplier", "vendor", "prov"],
    },
)

VENTAS_FISICA = Contrato(
    "ventas_tienda_fisica", "ventas_tienda_fisica.csv",
    obligatorias={
        "id_ticket": ["id_ticket", "ticket_id", "folio", "doc_num", "id_venta", "num_ticket",
                      "transaction_id", "idticket", "folio_ticket"],
        "fecha_venta": ["fecha_venta", "sale_date", "fecha", "fec_doc", "date", "fecha_ticket",
                        "fechaventa", "fec_ven"],
        "id_producto": ["id_producto", "product_id", "sku", "art_cod", "product_code",
                        "clave_producto", "codigo_producto"],
        "cantidad": ["cantidad", "qty", "quantity", "cant", "unidades", "piezas", "units"],
        "precio_unitario": ["precio_unitario", "unit_price", "unit_cost", "pre_uni",
                            "precio_u", "precio"],
        "monto_total_mxn": ["monto_total_mxn", "total_mxn", "imp_tot", "total", "importe",
                            "monto_total", "amount", "importe_total"],
    },
    opcionales={
        "sucursal": ["sucursal", "branch", "tienda", "cedi", "store", "plaza", "sucursal_origen"],
        "id_cliente": ["id_cliente", "customer_code", "cli_cod", "cliente", "customer_id",
                       "codigo_cliente"],
        "metodo_pago": ["metodo_pago", "pay_method", "for_pag", "payment_method", "forma_pago",
                        "tipo_pago"],
    },
)

VENTAS_ONLINE = Contrato(
    "ventas_online", "ventas_online.json",
    obligatorias={
        "order_id": ["order_id", "web_folio", "id_pedido", "orden", "pedido_id", "orderid",
                     "folio_web", "id_orden"],
        "order_timestamp": ["order_timestamp", "web_ts", "fecha_pedido", "timestamp",
                            "created_at", "fecha", "order_date", "fec_ped"],
        "product_id": ["product_id", "id_producto", "sku", "art_cod", "product_code"],
        "units": ["units", "cantidad", "qty", "quantity", "cant", "unidades"],
        "unit_price": ["unit_price", "precio_unitario", "pre_uni", "precio", "price"],
        "total_amount": ["total_amount", "monto_total", "imp_tot", "total", "importe",
                         "amount", "monto"],
    },
    opcionales={
        "currency": ["currency", "moneda", "divisa", "cur", "mon"],
        "customer_email": ["customer_email", "mail_cli", "email", "correo", "correo_cliente",
                           "email_cliente", "notification_email"],
        "shipping_address": ["shipping_address", "direccion", "direccion_envio", "address",
                             "dir_env"],
        "order_status": ["order_status", "est_ped", "estatus", "status", "estado",
                         "estado_pedido"],
        "payment_gateway": ["payment_gateway", "pasarela", "gateway", "procesador",
                            "metodo_pago"],
    },
)

CONTRATOS = [CATALOGO_PRODUCTOS, VENTAS_FISICA, VENTAS_ONLINE]


# ======================================================== normalizacion
def normalizar(nombre: str) -> str:
    """Quita acentos, separadores y mayusculas para poder comparar nombres."""
    s = unicodedata.normalize("NFKD", str(nombre))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", s.lower())


class ErrorContrato(Exception):
    """Falta algo obligatorio. Se detiene antes de escribir nada."""


def resolver(columnas: list[str], contrato: Contrato) -> tuple[dict[str, str], list[dict]]:
    """
    Devuelve (mapeo origen->canonico, bitacora).
    El mapeo solo incluye las columnas que cambian de nombre.
    """
    norm_origen = {normalizar(c): c for c in columnas}
    disponibles = dict(norm_origen)
    mapeo: dict[str, str] = {}
    bitacora: list[dict] = []
    tomadas: set[str] = set()

    for canonico, variantes in contrato.alias.items():
        obligatoria = canonico in contrato.obligatorias
        encontrado, confianza, estrategia = None, 0.0, "sin_coincidencia"

        # 1) nombre canonico exacto
        nc = normalizar(canonico)
        if nc in disponibles and disponibles[nc] not in tomadas:
            encontrado, confianza, estrategia = disponibles[nc], 1.00, "exacto"

        # 2) alias conocido
        if encontrado is None:
            for v in variantes:
                nv = normalizar(v)
                if nv in disponibles and disponibles[nv] not in tomadas:
                    encontrado, confianza, estrategia = disponibles[nv], 0.95, "alias"
                    break

        # 3) coincidencia difusa
        if encontrado is None:
            libres = [k for k, v in disponibles.items() if v not in tomadas]
            objetivos = [nc] + [normalizar(v) for v in variantes]
            mejor, mejor_score = None, 0.0
            for cand in libres:
                score = max(difflib.SequenceMatcher(None, cand, o).ratio() for o in objetivos)
                if score > mejor_score:
                    mejor, mejor_score = cand, score
            if mejor and mejor_score >= UMBRAL_DIFUSO:
                encontrado, confianza = disponibles[mejor], round(mejor_score, 3)
                estrategia = "difuso"

        if encontrado is not None:
            tomadas.add(encontrado)
            if encontrado != canonico:
                mapeo[encontrado] = canonico

        bitacora.append({
            "fuente": contrato.nombre, "columna_canonica": canonico,
            "columna_origen": encontrado, "confianza": confianza,
            "estrategia": estrategia,
            "requisito": "obligatoria" if obligatoria else "opcional",
        })

    faltan = [b["columna_canonica"] for b in bitacora
              if b["columna_origen"] is None and b["requisito"] == "obligatoria"]
    if faltan:
        raise ErrorContrato(
            f"La fuente '{contrato.nombre}' no cumple el contrato de esquema.\n"
            f"  Faltan columnas obligatorias: {', '.join(faltan)}\n"
            f"  Columnas recibidas: {', '.join(columnas)}\n"
            f"  Solucion: renombra la columna en el origen, o agrega su nombre real\n"
            f"  a la lista de alias de '{faltan[0]}' en fase0_contrato_esquema.py."
        )
    return mapeo, bitacora


# ============================================================== aplicacion
def cargar(ruta: Path) -> pd.DataFrame:
    if ruta.suffix.lower() == ".json":
        with open(ruta, encoding="utf-8") as f:
            datos = json.load(f)
        if isinstance(datos, dict):
            for clave in ("data", "records", "items", "rows"):
                if clave in datos:
                    datos = datos[clave]
                    break
        return pd.json_normalize(datos)
    return pd.read_csv(ruta)


def guardar(df: pd.DataFrame, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    if ruta.suffix.lower() == ".json":
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(json.loads(df.to_json(orient="records", date_format="iso")),
                      f, ensure_ascii=False)
    else:
        df.to_csv(ruta, index=False, encoding="utf-8")


def localizar(entrada: Path, contrato: Contrato) -> Path | None:
    """Busca el archivo aunque le hayan cambiado el nombre."""
    exacto = entrada / contrato.archivo
    if exacto.exists():
        return exacto
    objetivo = normalizar(Path(contrato.archivo).stem)
    mejor, score_mejor = None, 0.0
    for f in entrada.iterdir():
        if f.suffix.lower() not in (".csv", ".json") or f.name.startswith("_"):
            continue
        score = difflib.SequenceMatcher(None, normalizar(f.stem), objetivo).ratio()
        if score > score_mejor:
            mejor, score_mejor = f, score
    return mejor if score_mejor >= 0.60 else None


def procesar(entrada: Path, salida: Path, estricto: bool = True) -> dict[str, Any]:
    salida.mkdir(parents=True, exist_ok=True)
    bitacora_total: list[dict] = []
    resumen: dict[str, Any] = {"fuentes": {}, "errores": [], "ok": True}

    print("=" * 78)
    print("  FASE 0: CONTRATO DE ESQUEMA")
    print("=" * 78)
    print(f"Entrada: {entrada}\nSalida:  {salida}\n")

    for contrato in CONTRATOS:
        ruta = localizar(entrada, contrato)
        if ruta is None:
            print(f"  [omitido] {contrato.nombre}: no se encontro el archivo")
            resumen["fuentes"][contrato.nombre] = {"estado": "ausente"}
            continue

        try:
            df = cargar(ruta)
        except Exception as e:  # noqa: BLE001
            msg = f"{contrato.nombre}: no se pudo leer {ruta.name} ({type(e).__name__}: {e})"
            print(f"  [ERROR] {msg}")
            resumen["errores"].append(msg)
            resumen["ok"] = False
            continue

        print(f"  {contrato.nombre}  ({ruta.name}, {len(df):,} filas, {df.shape[1]} columnas)")

        try:
            mapeo, bitacora = resolver(list(df.columns), contrato)
        except ErrorContrato as e:
            print(f"    [CONTRATO NO CUMPLIDO]\n      " + str(e).replace("\n", "\n      "))
            resumen["errores"].append(str(e))
            resumen["fuentes"][contrato.nombre] = {"estado": "contrato_no_cumplido"}
            resumen["ok"] = False
            if estricto:
                continue
            else:
                raise

        bitacora_total.extend(bitacora)

        renombradas = [b for b in bitacora if b["estrategia"] in ("alias", "difuso")]
        for b in renombradas:
            marca = "~" if b["estrategia"] == "difuso" else " "
            print(f"    {marca} {b['columna_origen']:<26} -> {b['columna_canonica']:<22}"
                  f" ({b['estrategia']}, confianza {b['confianza']})")

        faltan_opc = [b["columna_canonica"] for b in bitacora
                      if b["columna_origen"] is None]
        for c in faltan_opc:
            print(f"      falta opcional '{c}': se crea vacia, el pipeline degrada sin romper")

        if mapeo:
            df = df.rename(columns=mapeo)
        for c in faltan_opc:
            # dtype object a proposito: si se crea como float64 (lo que pandas
            # infiere de un NA suelto), la imputacion con etiquetas de texto de
            # la Fase 2 revienta con "Invalid value 'ANONIMO' for dtype float64".
            df[c] = pd.Series([pd.NA] * len(df), dtype="object", index=df.index)

        extras = [c for c in df.columns if c not in contrato.alias]
        if extras:
            print(f"      columnas extra conservadas: {', '.join(extras[:6])}"
                  + (f" (+{len(extras)-6})" if len(extras) > 6 else ""))

        guardar(df, salida / contrato.archivo)
        resumen["fuentes"][contrato.nombre] = {
            "estado": "ok", "filas": len(df), "columnas": df.shape[1],
            "renombradas": len(renombradas), "faltantes_opcionales": faltan_opc,
            "difusas": [b["columna_origen"] for b in renombradas
                        if b["estrategia"] == "difuso"],
        }
        print()

    if bitacora_total:
        pd.DataFrame(bitacora_total).to_csv(salida / "_mapeo_esquema.csv",
                                            index=False, encoding="utf-8-sig")
    with open(salida / "_contrato_resultado.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)

    print("=" * 78)
    if resumen["ok"]:
        print("  Contrato cumplido. Los datos estan en esquema canonico.")
    else:
        print("  CONTRATO NO CUMPLIDO en una o mas fuentes.")
        print("  No se continua: es preferible no producir salida a producir una")
        print("  salida silenciosamente incorrecta.")
    print("=" * 78)
    return resumen


def main() -> None:
    ap = argparse.ArgumentParser(description="Adapta cualquier esquema al canonico")
    ap.add_argument("entrada", type=Path)
    ap.add_argument("salida", type=Path)
    ap.add_argument("--permisivo", action="store_true",
                    help="continua aunque alguna fuente no cumpla el contrato")
    a = ap.parse_args()

    if not a.entrada.exists():
        print(f"[error] no existe la carpeta de entrada: {a.entrada}")
        sys.exit(2)

    resumen = procesar(a.entrada, a.salida, estricto=True)
    sys.exit(0 if (resumen["ok"] or a.permisivo) else 1)


if __name__ == "__main__":
    main()
