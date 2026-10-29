"""
Pruebas de tortura del motor universal: archivos raros, rotos y enormes.
Criterio: el motor NUNCA lanza una excepcion no controlada y nunca pierde filas
(limpias + cuarentena = filas utiles leidas).

Uso:  python qa/tortura_universal.py [--grande]
"""

from __future__ import annotations

import io
import json
import random
import shutil
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
from motor.pipeline_universal import ejecutar  # noqa: E402

TMP = RAIZ / "qa" / "tmp_tortura"
rng = np.random.default_rng(7)


def base(n=2000):
    df = pd.DataFrame({
        "ID Venta": [f"V{i:06d}" for i in range(n)],
        "Fecha": pd.date_range("2026-01-01", periods=n, freq="h").strftime("%d/%m/%Y"),
        "Cliente Email": [f"c{i}@mail.com" for i in range(n)],
        "Cantidad": rng.integers(1, 6, n),
        "Precio": np.round(rng.uniform(10, 900, n), 2),
        "Sucursal": rng.choice(["Norte", "Sur", "centro ", "CENTRO"], n),
    })
    df["Total"] = (df["Cantidad"] * df["Precio"]).round(2)
    df.loc[rng.choice(n, 40, replace=False), "Total"] = None
    df.loc[rng.choice(n, 15, replace=False), "Cliente Email"] = "malo@@x..com"
    df.loc[rng.choice(n, 10, replace=False), "Cantidad"] = -2
    df.loc[rng.choice(n, 8, replace=False), "Total"] = 999999
    return df


def casos(grande: bool):
    d = base()
    yield "csv_normal", {"a.csv": d.to_csv(index=False).encode()}
    yield "csv_cp1252_puntoycoma_coma_decimal", {
        "b.csv": d.assign(Precio=d["Precio"].map(lambda x: f"{x:,.2f}".replace(",", "X")
                          .replace(".", ",").replace("X", ".")))
        .to_csv(index=False, sep=";").encode("cp1252")}
    yield "tsv_con_signos_de_pesos", {"c.tsv": d.assign(Total=d["Total"].map(
        lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")).to_csv(index=False, sep="\t").encode()}
    b = io.BytesIO()
    with pd.ExcelWriter(b, engine="openpyxl") as w:
        d.to_excel(w, sheet_name="Ventas", index=False)
        d.head(50).to_excel(w, sheet_name="Hoja vacia parcial", index=False)
        pd.DataFrame().to_excel(w, sheet_name="Vacia", index=False)
    yield "excel_multihoja", {"d.xlsx": b.getvalue()}
    yield "json_lista", {"e.json": d.to_json(orient="records").encode()}
    yield "json_envoltorio_anidado", {"f.json": json.dumps({"meta": {"v": 1}, "data": [
        {"id": i, "cliente": {"nombre": f"N{i}", "email": f"n{i}@x.com"}, "tags": ["a", "b"],
         "monto": float(rng.uniform(1, 100))} for i in range(500)]}).encode()}
    yield "json_lines", {"g.jsonl": "\n".join(d.head(300).apply(
        lambda r: r.to_json(), axis=1)).encode()}
    yield "columnas_duplicadas_y_sin_nombre", {"h.csv": b"id,id,,monto,monto\n1,1,x,5,6\n2,2,y,7,8\n"}
    yield "solo_encabezado", {"i.csv": b"a,b,c\n"}
    yield "archivo_vacio", {"j.csv": b""}
    yield "basura_binaria", {"k.csv": bytes(rng.integers(0, 255, 5000, dtype=np.uint8))}
    yield "json_corrupto", {"l.json": b'[{"a":1},{"a":2'}
    yield "lineas_rotas_csv", {"m.csv": b"a,b\n1,2\n3,4,5,6\n7,8\n\"sin cerrar,9\n"}
    yield "una_sola_columna_toda_nula", {"n.csv": b"x\n\n\nNA\nnull\n"}
    yield "fechas_mezcladas_y_epoch", {"o.csv": ("fecha,monto\n2026-01-05,10\n05/01/2026,11\n"
                                                  "1789105020,12\n31/02/2026,13\nayer,14\n").encode()}
    yield "texto_gigante_en_celda", {"p.csv": ("a,b\n" + "x" * 2_000_000 + ",1\nq,2\n").encode()}
    pq = io.BytesIO()
    try:
        d.to_parquet(pq)
    except ImportError:
        # pyarrow es opcional (requirements.txt). Sin el no se puede ni FABRICAR el archivo de
        # prueba; antes esto tumbaba la suite completa. Ahora el caso se omite y se reporta.
        yield "parquet", None
    else:
        yield "parquet", {"q.parquet": pq.getvalue()}
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w") as f:
        f.writestr("dentro/ventas.csv", d.to_csv(index=False))
        f.writestr("catalogo.json", json.dumps([{"ID Venta": "X"}]))
    yield "zip_con_varios", {"r.zip": z.getvalue()}
    yield "extension_mentirosa_json_como_txt", {"s.txt": d.head(100).to_json(orient="records").encode()}
    yield "infinitos_y_numeros_raros", {"t.csv": b"monto,cantidad\ninf,1\n-inf,2\n1e308,3\n(500),4\n1.234,56,5\n"}
    if grande:
        n = 1_000_000
        g = pd.DataFrame({"id": np.arange(n), "fecha": pd.date_range("2025-01-01", periods=n,
                          freq="min").strftime("%Y-%m-%d"), "cantidad": rng.integers(1, 9, n),
                          "precio": np.round(rng.uniform(5, 500, n), 2),
                          "region": rng.choice(list("ABCDE"), n)})
        g["total"] = (g.cantidad * g.precio).round(2)
        g.loc[rng.choice(n, 5000, replace=False), "total"] = np.nan
        yield "un_millon_de_filas", {"grande.csv": g.to_csv(index=False).encode()}


def main():
    grande = "--grande" in sys.argv
    shutil.rmtree(TMP, ignore_errors=True)
    filas = []
    for nombre, archivos in casos(grande):
        if archivos is None:
            filas.append({"caso": nombre, "crash": False, "ok": None, "entrada": 0, "limpias": 0,
                          "cuarentena": 0, "seg": 0.0,
                          "nota": "omitido: falta pyarrow (pip install -r requirements.txt)"})
            print(f"  {nombre:<38} {'omitido':<20} falta pyarrow para fabricar el archivo de prueba")
            continue
        d = TMP / nombre
        (d / "in").mkdir(parents=True)
        for f, data in archivos.items():
            (d / "in" / f).write_bytes(data)
        t0 = time.perf_counter()
        try:
            r = ejecutar(sorted((d / "in").iterdir()), d / "out", avisar=lambda s: None)
            crash = False
        except Exception as e:  # noqa: BLE001
            r, crash = {"error": repr(e)}, True
        dt = time.perf_counter() - t0
        tot = r.get("totales", {})
        filas.append({"caso": nombre, "crash": crash, "ok": r.get("ok"),
                      "entrada": tot.get("entrada", 0), "limpias": tot.get("limpias", 0),
                      "cuarentena": tot.get("cuarentena", 0), "seg": round(dt, 1),
                      "nota": (r.get("error") or "")[:70]})
        e = filas[-1]
        estado = "CRASH" if crash else ("ok" if e["ok"] else "detenido controlado")
        print(f"  {nombre:<38} {estado:<20} entrada={e['entrada']:>9,} limpias={e['limpias']:>9,} "
              f"cuar={e['cuarentena']:>7,} {dt:6.1f}s {e['nota']}")
    res = pd.DataFrame(filas)
    out = RAIZ / "qa" / "reportes"
    out.mkdir(parents=True, exist_ok=True)
    res.to_csv(out / "tortura_universal.csv", index=False, encoding="utf-8-sig")
    crashes = int(res.crash.sum())
    omitidos = int(res.nota.astype(str).str.startswith("omitido").sum())
    print(f"\n  {len(res)} casos, {crashes} excepciones no controladas"
          + (f" ({omitidos} omitido por falta de una dependencia opcional)" if omitidos else ""))
    shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(1 if crashes else 0)


if __name__ == "__main__":
    main()
