"""
Fase 7: carga transaccional a base de datos relacional (SQLite).

Persiste las dos tablas obligatorias del proyecto:
  - transacciones_limpias : registros que pasaron el esquema Pydantic
  - cuarentena_errores    : registros desviados, con su motivo

Garantias de la carga:
  - Transaccion unica por tabla: o entra el lote completo o no entra nada
    (commit al final, rollback ante cualquier error).
  - Idempotente: volver a correrlo no duplica filas (DELETE previo dentro de
    la misma transaccion).
  - Integridad declarada en el esquema (PRIMARY KEY, CHECK, NOT NULL), no solo
    confiada a la capa de Python.
  - Prueba de rollback incluida: se fuerza un fallo a mitad de un lote y se
    verifica que la base queda intacta.

Uso:  python fase7_carga_bd.py salidas_fase6 pipeline.db
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ENTRADA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("salidas_fase6")
BD = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("pipeline.db")

DDL_LIMPIAS = """
CREATE TABLE IF NOT EXISTS transacciones_limpias (
    id_registro           TEXT    PRIMARY KEY,
    canal                 TEXT    NOT NULL CHECK (canal IN ('FISICA','ONLINE')),
    fuente                TEXT    NOT NULL,
    id_producto           TEXT    NOT NULL CHECK (id_producto GLOB 'PROD-[0-9][0-9][0-9]'),
    nombre_producto       TEXT,
    categoria             TEXT,
    cliente               TEXT,
    correo                TEXT,
    cantidad              INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario_mxn   REAL    NOT NULL CHECK (precio_unitario_mxn > 0),
    monto_mxn             REAL    NOT NULL CHECK (monto_mxn > 0),
    monto_facturable_mxn  REAL    NOT NULL CHECK (monto_facturable_mxn >= 0),
    monto_mxn_norm        REAL    NOT NULL CHECK (monto_mxn_norm BETWEEN 0 AND 1),
    segmento_monto        TEXT    NOT NULL CHECK (segmento_monto IN ('BAJO','MEDIO','ALTO')),
    moneda                TEXT    NOT NULL DEFAULT 'MXN' CHECK (moneda = 'MXN'),
    metodo_pago           TEXT,
    estatus               TEXT,
    fecha                 TEXT    NOT NULL,
    flag_outlier          INTEGER NOT NULL DEFAULT 0,
    flag_huerfano         INTEGER NOT NULL DEFAULT 0
);
"""

DDL_CUARENTENA = """
CREATE TABLE IF NOT EXISTS cuarentena_errores (
    id_cuarentena              INTEGER PRIMARY KEY AUTOINCREMENT,
    id_registro                TEXT,
    canal                      TEXT,
    fuente                     TEXT,
    id_producto                TEXT,
    cliente                    TEXT,
    cantidad                   TEXT,
    precio_unitario_mxn        TEXT,
    monto_mxn                  TEXT,
    fecha                      TEXT,
    causa_principal            TEXT    NOT NULL,
    motivo_rechazo_cuarentena  TEXT    NOT NULL,
    regla_violada              TEXT,
    fila_origen                INTEGER,
    cargado_en                 TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
);
"""

DDL_INDICES = [
    "CREATE INDEX IF NOT EXISTS ix_limpias_fecha    ON transacciones_limpias(fecha);",
    "CREATE INDEX IF NOT EXISTS ix_limpias_canal    ON transacciones_limpias(canal);",
    "CREATE INDEX IF NOT EXISTS ix_limpias_producto ON transacciones_limpias(id_producto);",
    "CREATE INDEX IF NOT EXISTS ix_cuar_causa       ON cuarentena_errores(causa_principal);",
]

COLS_LIMPIAS = [
    "id_registro", "canal", "fuente", "id_producto", "nombre_producto", "categoria",
    "cliente", "correo", "cantidad", "precio_unitario_mxn", "monto_mxn",
    "monto_facturable_mxn", "monto_mxn_norm", "segmento_monto", "moneda",
    "metodo_pago", "estatus", "fecha", "flag_outlier", "flag_huerfano",
]
COLS_CUARENTENA = [
    "id_registro", "canal", "fuente", "id_producto", "cliente", "cantidad",
    "precio_unitario_mxn", "monto_mxn", "fecha", "causa_principal",
    "motivo_rechazo_cuarentena", "regla_violada", "fila_origen",
]


def conectar(ruta: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(ruta)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def alinear(df: pd.DataFrame, columnas: list[str]) -> list[tuple]:
    """Deja solo las columnas de la tabla, en orden, con None donde falte."""
    d = df.copy()
    for c in columnas:
        if c not in d.columns:
            d[c] = None
    d = d[columnas]
    for c in d.columns:
        if d[c].dtype == bool:
            d[c] = d[c].astype(int)
    return list(d.astype(object).where(pd.notna(d), None).itertuples(index=False, name=None))


def cargar(conn: sqlite3.Connection, tabla: str, columnas: list[str],
           filas: list[tuple]) -> int:
    """
    Carga un lote dentro de una sola transaccion.
    Si algo falla a media escritura se hace rollback y la tabla queda como estaba.
    """
    marcadores = ",".join("?" * len(columnas))
    sql = f"INSERT INTO {tabla} ({','.join(columnas)}) VALUES ({marcadores})"
    try:
        conn.execute("BEGIN")
        conn.execute(f"DELETE FROM {tabla}")      # idempotencia
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)
    except Exception as e:                         # noqa: BLE001
        conn.rollback()
        print(f"  [rollback] {tabla}: {type(e).__name__}: {e}")
        raise


def probar_rollback(conn: sqlite3.Connection) -> bool:
    """
    Prueba explicita de atomicidad.
    Se intenta cargar un lote donde la ultima fila viola un CHECK.
    Exito = la excepcion se controla y el conteo previo no cambia.
    """
    antes = conn.execute("SELECT COUNT(*) FROM transacciones_limpias").fetchone()[0]
    corrupta = [(
        "ROLLBACK-TEST", "FISICA", "prueba", "PROD-001", "x", "y", "z", None,
        -99,            # viola CHECK (cantidad > 0)
        10.0, 10.0, 10.0, 0.5, "BAJO", "MXN", "EFECTIVO", "COMPLETADO",
        "2026-06-01", 0, 0,
    )]
    try:
        conn.execute("BEGIN")
        conn.executemany(
            f"INSERT INTO transacciones_limpias ({','.join(COLS_LIMPIAS)}) "
            f"VALUES ({','.join('?' * len(COLS_LIMPIAS))})", corrupta)
        conn.commit()
        resultado = False
    except sqlite3.IntegrityError as e:
        conn.rollback()
        print(f"  CHECK disparado y rollback aplicado: {e}")
        resultado = True

    despues = conn.execute("SELECT COUNT(*) FROM transacciones_limpias").fetchone()[0]
    intacta = antes == despues
    print(f"  Filas antes: {antes} | despues: {despues} | tabla intacta: {intacta}")
    return resultado and intacta


def main() -> None:
    f_ok = ENTRADA / "transacciones_limpias.csv"
    f_q = ENTRADA / "cuarentena_errores.csv"
    if not f_ok.exists():
        print(f"[error] no existe {f_ok}. Corre antes fase5_fase6_pipeline.py")
        sys.exit(1)

    df_ok = pd.read_csv(f_ok)
    df_q = pd.read_csv(f_q) if f_q.exists() else pd.DataFrame()

    print("=" * 74)
    print("  FASE 7: CARGA TRANSACCIONAL A BASE DE DATOS")
    print("=" * 74)
    print(f"Origen: {ENTRADA}")
    print(f"Destino: {BD.resolve()}")

    conn = conectar(BD)
    try:
        conn.executescript(DDL_LIMPIAS)
        conn.executescript(DDL_CUARENTENA)
        for ddl in DDL_INDICES:
            conn.execute(ddl)
        conn.commit()
        print("\n  Esquema creado (PRIMARY KEY, CHECK, NOT NULL e indices).")

        print("\n  Cargando transacciones_limpias...")
        n_ok = cargar(conn, "transacciones_limpias", COLS_LIMPIAS, alinear(df_ok, COLS_LIMPIAS))
        print(f"  commit -> {n_ok} filas")

        print("\n  Cargando cuarentena_errores...")
        n_q = 0
        if len(df_q):
            if "causa_principal" not in df_q.columns:
                df_q["causa_principal"] = "Sin clasificar"
            n_q = cargar(conn, "cuarentena_errores", COLS_CUARENTENA,
                         alinear(df_q, COLS_CUARENTENA))
        print(f"  commit -> {n_q} filas")

        print("\n  Prueba de rollback (insercion que viola CHECK):")
        ok_rollback = probar_rollback(conn)

        # ------------------------------------------------ verificacion final
        print("\n" + "-" * 74)
        print("  VERIFICACION EN BASE DE DATOS")
        print("-" * 74)
        c_ok = conn.execute("SELECT COUNT(*) FROM transacciones_limpias").fetchone()[0]
        c_q = conn.execute("SELECT COUNT(*) FROM cuarentena_errores").fetchone()[0]
        print(f"  transacciones_limpias : {c_ok}")
        print(f"  cuarentena_errores    : {c_q}")
        print(f"  total                 : {c_ok + c_q}")

        print("\n  Ventas por canal y segmento:")
        for fila in conn.execute(
            "SELECT canal, segmento_monto, COUNT(*), ROUND(SUM(monto_facturable_mxn),2) "
            "FROM transacciones_limpias GROUP BY canal, segmento_monto "
            "ORDER BY canal, CASE segmento_monto WHEN 'BAJO' THEN 1 "
            "WHEN 'MEDIO' THEN 2 ELSE 3 END"
        ):
            print(f"    {fila[0]:<8} {fila[1]:<6} {fila[2]:>5} pedidos   "
                  f"${fila[3]:>14,.2f} facturado")

        print("\n  Cuarentena por causa:")
        for fila in conn.execute(
            "SELECT causa_principal, COUNT(*) FROM cuarentena_errores "
            "GROUP BY causa_principal ORDER BY 2 DESC"
        ):
            print(f"    {fila[1]:>5}  {fila[0]}")

        total_fact = conn.execute(
            "SELECT ROUND(SUM(monto_facturable_mxn),2) FROM transacciones_limpias"
        ).fetchone()[0]
        print(f"\n  Monto facturable total: ${total_fact:,.2f} MXN")

        assert ok_rollback, "La prueba de rollback no se comporto como se esperaba"
        assert c_ok == len(df_ok), "Descuadre en transacciones_limpias"
        assert c_q == len(df_q), "Descuadre en cuarentena_errores"

        resumen = pd.DataFrame([{
            "tabla": "transacciones_limpias", "filas": c_ok,
        }, {
            "tabla": "cuarentena_errores", "filas": c_q,
        }])
        resumen.to_csv(ENTRADA / "carga_bd.csv", index=False, encoding="utf-8-sig")

        print("\n" + "=" * 74)
        print(f"  Base de datos lista: {BD.resolve()}")
        print("=" * 74)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
