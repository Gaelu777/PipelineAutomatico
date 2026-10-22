#!/usr/bin/env python3
"""
Proyecto Integrador ETL - Ingeniería de Datos (ATD-2602)
Utilidad: reconstruye los datasets del Grupo 1 a partir de los PDF impresos.

Uso:  python utilidades/extraer_pdf.py <carpeta_pdf> <carpeta_datasets>
      python utilidades/extraer_pdf.py . datasets

Entrada : catalogo_productos.pdf, ventas_tienda_fisica.pdf, ventas_online.pdf
Salida  : catalogo_productos.csv, ventas_tienda_fisica.csv, ventas_online.json

Las tablas se leen carácter por carácter y cada uno se asigna a su columna por la coordenada x,
porque algunas celdas se pegan con la vecina. En ventas_tienda_fisica las páginas alternan:
impares con id_ticket..precio_unitario y pares con monto_total_mxn..metodo_pago de las mismas filas.
Limitación: el PDF trunca el texto largo de las celdas ('TRANSFERENC', 'Manzanillo Puert',
nombre_producto); números, fechas y llaves llegan completos.
"""
import json, sys
from pathlib import Path
import pandas as pd
import pymupdf

SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
DST = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("datasets")
BORDES = [53, 129, 204, 280, 356, 432]          # x donde empieza cada columna de la tabla

def filas_pagina(page, ncols: int) -> list:
    """Agrupa los caracteres por renglón (y) y por columna (x)."""
    celdas = {}
    for b in page.get_text("rawdict")["blocks"]:
        for l in b.get("lines", []):
            for s in l["spans"]:
                for ch in s["chars"]:
                    x0, y = ch["bbox"][0], round(ch["origin"][1])
                    col = max(i for i, x in enumerate(BORDES) if x0 >= x - 1)
                    celdas.setdefault(y, {}).setdefault(col, []).append((x0, ch["c"]))
    filas = []
    for y in sorted(celdas):
        fila = [None] * ncols
        for col, chars in celdas[y].items():
            if col < ncols:
                fila[col] = "".join(c for _, c in sorted(chars)).strip() or None
        filas.append(fila)
    return filas

def tabla(pdf: Path, cols_a: list, cols_b: list | None = None) -> pd.DataFrame:
    """Tabla simple, o partida en pares de páginas (cols_a en la impar, cols_b en la par)."""
    d = pymupdf.open(pdf)
    if cols_b is None:
        filas = [f for p in d for f in filas_pagina(p, len(cols_a)) if f[0] != cols_a[0]]
        return pd.DataFrame(filas, columns=cols_a)
    partes = []
    for i in range(0, len(d), 2):
        a = [f for f in filas_pagina(d[i], len(cols_a)) if f[0] != cols_a[0]]
        b = [f for f in filas_pagina(d[i + 1], len(cols_b)) if not (f[0] or "").startswith(cols_b[0])]
        if len(a) != len(b):
            raise ValueError(f"páginas {i + 1} y {i + 2} no tienen el mismo número de filas")
        partes += [x + y for x, y in zip(a, b)]
    return pd.DataFrame(partes, columns=cols_a + cols_b)

def json_pdf(pdf: Path) -> list:
    """Une las líneas del JSON impreso; si una línea termina en guion se pega sin espacio (PROD-\\n890)."""
    texto = ""
    for l in (l for p in pymupdf.open(pdf) for l in p.get_text().splitlines()):
        texto += l if (not texto or texto.endswith("-")) else " " + l
    return json.loads(texto)

def main():
    DST.mkdir(parents=True, exist_ok=True)
    if (SRC / "catalogo_productos.pdf").exists():
        cat = tabla(SRC / "catalogo_productos.pdf",
                    ["id_producto", "nombre_producto", "categoria", "precio_base_mxn", "stock_minimo", "proveedor"])
        cat.to_csv(DST / "catalogo_productos.csv", index=False, encoding="utf-8")
        print("catalogo_productos:", len(cat), "filas")
    if (SRC / "ventas_tienda_fisica.pdf").exists():
        tf = tabla(SRC / "ventas_tienda_fisica.pdf",
                   ["id_ticket", "fecha_venta", "sucursal", "id_producto", "cantidad", "precio_unitario"],
                   ["monto_total_mxn", "id_cliente", "metodo_pago"])
        tf.to_csv(DST / "ventas_tienda_fisica.csv", index=False, encoding="utf-8")
        print("ventas_tienda_fisica:", len(tf), "filas")
    if (SRC / "ventas_online.pdf").exists():
        datos = json_pdf(SRC / "ventas_online.pdf")
        json.dump(datos, open(DST / "ventas_online.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print("ventas_online:", len(datos), "registros")

if __name__ == "__main__":
    main()
