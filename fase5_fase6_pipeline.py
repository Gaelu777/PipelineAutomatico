"""
Fases 5 y 6 del pipeline ETL.

Fase 5  Transformacion, discretizacion y escalamiento
        - Min-Max de monto_mxn al rango [0, 1].
        - Discretizacion del importe en 3 segmentos (BAJO, MEDIO, ALTO) por terciles.

Fase 6  Validacion con Pydantic y zona de cuarentena
        - Esquema estricto con @field_validator y @model_validator.
        - Cada registro se valida de forma aislada: una fila corrupta nunca
          detiene el lote. Lo que no pasa el esquema se desvia a cuarentena
          con su motivo_rechazo_cuarentena.

Las reglas de validacion salen del analisis forense
(utilidades/forense_datos_enganosos.py): atacan los errores silenciosos, no
solo los nulos y los formatos rotos.

Uso:  python fase5_fase6_pipeline.py salidas_fase4 salidas_fase6
"""

from __future__ import annotations

import json
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

ENTRADA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("salidas_fase4")
SALIDA = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("salidas_fase6")
SALIDA.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------- parametros
TOLERANCIA_PESOS = 0.01          # diferencia maxima aceptada al recalcular un total
PRECIO_RELLENO = 500.00          # valor por defecto que usa el POS cuando no halla el producto
FACTOR_BOT = 80                  # factor exacto con el que los bots inflan el total
INICIO_OPERACION = date(2026, 1, 1)
FIN_OPERACION = date(2026, 12, 31)
RE_PRODUCTO = re.compile(r"^PROD-\d{3}$")
RE_CORREO = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")

ESTATUS_VALIDOS = {"COMPLETADO", "PAGADO", "PENDIENTE", "CANCELADO", "DEVUELTO", "ENVIADO"}
ESTATUS_NO_FACTURABLE = {"CANCELADO", "DEVUELTO"}

log: list[dict] = []


def registrar(fase: str, metrica: str, valor: Any, detalle: str = "") -> None:
    log.append({"fase": fase, "metrica": metrica, "valor": valor, "detalle": detalle})
    print(f"  {metrica}: {valor}" + (f"  ({detalle})" if detalle else ""))


# =============================================================== FASE 5
def minmax(s: pd.Series) -> tuple[pd.Series, dict]:
    """
    Escalamiento Min-Max a [0, 1].

    Se aplica DESPUES del capping de la Fase 3 a proposito: Min-Max usa el
    minimo y el maximo, asi que un solo outlier sin tratar comprime todos los
    demas valores contra el 0. Con los extremos ya recortados, el rango
    resultante reparte la escala entre los datos reales.
    """
    s = pd.to_numeric(s, errors="coerce")
    lo, hi = s.min(), s.max()
    if pd.isna(lo) or pd.isna(hi) or hi == lo:
        return pd.Series(np.zeros(len(s)), index=s.index), {"min": lo, "max": hi, "degenerado": True}
    return ((s - lo) / (hi - lo)).round(6), {"min": float(lo), "max": float(hi), "degenerado": False}


def discretizar(s: pd.Series) -> tuple[pd.Series, dict]:
    """
    Discretizacion del importe en 3 segmentos por terciles (qcut con q=3).

    Se eligen terciles y no cortes fijos de negocio porque el catalogo mezcla
    categorias con precios muy distintos (Alimentos contra Electronica): un
    corte fijo en pesos dejaria una categoria entera dentro del mismo segmento.
    Los terciles garantizan tres grupos de tamano comparable y por lo tanto
    con poder estadistico similar.
    """
    s = pd.to_numeric(s, errors="coerce")
    etiquetas = ["BAJO", "MEDIO", "ALTO"]
    try:
        seg, cortes = pd.qcut(s, q=3, labels=etiquetas, retbins=True, duplicates="drop")
        if seg.cat.categories.size < 3:
            raise ValueError("terciles colapsados")
    except (ValueError, IndexError):
        seg, cortes = pd.cut(s, bins=3, labels=etiquetas, retbins=True)
    return seg.astype(str), {"cortes": [round(float(c), 2) for c in cortes]}


def fase5(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 74)
    print("  FASE 5: ESCALAMIENTO MIN-MAX Y DISCRETIZACION")
    print("=" * 74)

    df = df.copy()
    df["monto_mxn"] = pd.to_numeric(df["monto_mxn"], errors="coerce")

    df["monto_mxn_norm"], info_mm = minmax(df["monto_mxn"])
    registrar("5", "minmax_monto_mxn", f"[{info_mm['min']:.2f}, {info_mm['max']:.2f}] -> [0, 1]",
              "escalado despues del capping de la Fase 3")

    df["cantidad_norm"], info_c = minmax(df["cantidad"])
    registrar("5", "minmax_cantidad", f"[{info_c['min']:.0f}, {info_c['max']:.0f}] -> [0, 1]")

    df["segmento_monto"], info_d = discretizar(df["monto_mxn"])
    registrar("5", "cortes_terciles", info_d["cortes"], "BAJO / MEDIO / ALTO")

    reparto = df["segmento_monto"].value_counts().reindex(["BAJO", "MEDIO", "ALTO"]).to_dict()
    registrar("5", "reparto_segmentos", reparto)

    # Verificacion dura: el escalamiento no puede salirse del rango pedido.
    fuera = int(((df["monto_mxn_norm"] < 0) | (df["monto_mxn_norm"] > 1)).sum())
    registrar("5", "valores_fuera_de_rango", fuera, "debe ser 0")
    assert fuera == 0, "Min-Max produjo valores fuera de [0, 1]"

    resumen_seg = (
        df.groupby("segmento_monto")["monto_mxn"]
          .agg(n="count", minimo="min", maximo="max", promedio="mean")
          .round(2).reindex(["BAJO", "MEDIO", "ALTO"])
    )
    print("\n  Rango real de cada segmento:")
    print(resumen_seg.to_string())
    resumen_seg.to_csv(SALIDA / "segmentos_fase5.csv", encoding="utf-8-sig")

    with open(SALIDA / "parametros_escalamiento.json", "w", encoding="utf-8") as f:
        json.dump({"minmax_monto": info_mm, "minmax_cantidad": info_c,
                   "terciles": info_d, "reparto": {k: int(v) for k, v in reparto.items()}},
                  f, indent=2, ensure_ascii=False)
    return df


# =============================================================== FASE 6
def limpiar_texto(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = unicodedata.normalize("NFKC", str(v)).strip()
    return s or None


class TransaccionLimpia(BaseModel):
    """
    Esquema estricto de una venta apta para la tabla final.

    Cada validador corresponde a un hallazgo del analisis forense: no basta con
    que los tipos sean correctos, la fila debe ser coherente consigo misma y
    con el catalogo maestro.
    """

    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore", validate_assignment=True)

    # --- identidad
    id_registro: str = Field(min_length=3, max_length=40)
    canal: Literal["FISICA", "ONLINE"]
    fuente: str
    id_producto: str
    nombre_producto: str | None = None
    categoria: str | None = None

    # --- cliente
    cliente: str | None = None
    correo: str | None = None

    # --- importes
    cantidad: int = Field(gt=0, le=1000)
    precio_unitario_mxn: float = Field(gt=0, le=1_000_000)
    monto_mxn: float = Field(gt=0, le=10_000_000)
    monto_facturable_mxn: float = Field(ge=0)
    monto_mxn_norm: float = Field(ge=0.0, le=1.0)
    segmento_monto: Literal["BAJO", "MEDIO", "ALTO"]
    moneda: Literal["MXN"] = "MXN"

    # --- operacion
    metodo_pago: str | None = None
    estatus: str | None = None
    fecha: date

    # --- trazabilidad de fases previas
    flag_outlier: bool = False
    flag_huerfano: bool = False

    # ---------------------------------------------------------- validadores

    @field_validator("id_producto")
    @classmethod
    def formato_producto(cls, v: str) -> str:
        """El id debe cumplir PROD-NNN. No garantiza que exista: eso lo ve la regla cruzada."""
        v = (v or "").strip().upper()
        if not RE_PRODUCTO.match(v):
            raise ValueError(f"id_producto con formato invalido: '{v}'")
        return v

    @field_validator("correo")
    @classmethod
    def correo_estricto(cls, v: str | None) -> str | None:
        """
        Regex con anclas, mas rechazo explicito de '@@' y '..'.
        Una comprobacion ingenua ("tiene arroba y punto") aprueba
        'usuario@@gmail..com' y lo deja en la base como cliente contactable.
        """
        if v is None:
            return None
        v = v.strip().lower()
        if not v:
            return None
        if "@@" in v or ".." in v:
            raise ValueError(f"correo con arroba o punto duplicado: '{v}'")
        if not RE_CORREO.match(v):
            raise ValueError(f"correo con sintaxis invalida: '{v}'")
        return v

    @field_validator("fecha")
    @classmethod
    def dentro_del_periodo(cls, v: date) -> date:
        """Una fecha puede ser valida en el calendario y aun asi imposible para el negocio."""
        if not (INICIO_OPERACION <= v <= FIN_OPERACION):
            raise ValueError(
                f"fecha {v} fuera del periodo operativo "
                f"[{INICIO_OPERACION}, {FIN_OPERACION}]"
            )
        return v

    @field_validator("estatus")
    @classmethod
    def estatus_del_catalogo(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().upper()
        if v and v not in ESTATUS_VALIDOS:
            raise ValueError(f"estatus desconocido: '{v}'")
        return v or None

    @model_validator(mode="after")
    def reglas_cruzadas(self) -> "TransaccionLimpia":
        """
        Las reglas que ningun campo puede validar por si solo.
        Aqui viven los errores silenciosos: cada campo es correcto, la
        combinacion no lo es.
        """
        # R1. Llave huerfana: el formato es perfecto pero el producto no existe.
        if self.flag_huerfano:
            raise ValueError(f"llave huerfana: {self.id_producto} no esta en el catalogo maestro")

        # R2. Precio de relleno. 500.00 exacto es el valor por defecto del POS.
        if abs(self.precio_unitario_mxn - PRECIO_RELLENO) < 1e-9:
            raise ValueError(
                f"precio de relleno {PRECIO_RELLENO:.2f} detectado, no es un precio real"
            )

        # R3. Aritmetica interna. Si la Fase 3 recorto el monto, la incoherencia
        #     la causamos nosotros y no se castiga la fila.
        if not self.flag_outlier:
            esperado = round(self.cantidad * self.precio_unitario_mxn, 2)
            if abs(esperado - self.monto_mxn) > TOLERANCIA_PESOS:
                razon = (
                    f"inflado x{FACTOR_BOT} (patron de bot)"
                    if esperado > 0 and abs(self.monto_mxn / esperado - FACTOR_BOT) < 0.01
                    else "total inconsistente"
                )
                raise ValueError(
                    f"{razon}: {self.cantidad} x {self.precio_unitario_mxn:.2f} "
                    f"= {esperado:.2f} pero monto_mxn = {self.monto_mxn:.2f}"
                )

        # R4. Coherencia entre estatus e importe facturable.
        if self.estatus in ESTATUS_NO_FACTURABLE and self.monto_facturable_mxn != 0:
            raise ValueError(
                f"estatus {self.estatus} con monto facturable "
                f"{self.monto_facturable_mxn:.2f}, deberia ser 0"
            )

        # R5. Las ventas online deben traer correo de contacto.
        if self.canal == "ONLINE" and not self.correo:
            raise ValueError("venta online sin correo de contacto")

        return self


#: Etiqueta legible por tipo de rechazo, para el resumen y el reporte.
ETIQUETAS_MOTIVO = [
    ("llave huerfana", "Llave huerfana (producto inexistente)"),
    ("precio de relleno", "Precio de relleno 500.00"),
    ("patron de bot", "Total inflado x80 (bot)"),
    ("total inconsistente", "Total distinto de cantidad x precio"),
    ("arroba o punto duplicado", "Correo con @@ o .."),
    ("sintaxis invalida", "Correo con sintaxis invalida"),
    ("sin correo de contacto", "Venta online sin correo"),
    ("duplicado de negocio", "Duplicado de negocio"),
    ("fuera del periodo", "Fecha fuera del periodo operativo"),
    ("estatus desconocido", "Estatus fuera de catalogo"),
    ("deberia ser 0", "Cancelado con importe facturable"),
    ("formato invalido", "id_producto mal formado"),
]


def etiquetar_motivo(texto: str) -> str:
    t = str(texto).lower()
    for clave, etiqueta in ETIQUETAS_MOTIVO:
        if clave in t:
            return etiqueta
    return "Otro error de esquema"


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Deriva las columnas que el esquema necesita y marca duplicados de negocio."""
    df = df.copy()

    # El correo viaja en 'cliente' cuando la fuente es online.
    df["correo"] = np.where(
        df["canal"].eq("ONLINE"), df["cliente"].apply(limpiar_texto), None
    )

    # Monto facturable: un pedido cancelado o devuelto no factura.
    est = df["estatus"].fillna("").astype(str).str.upper()
    df["monto_facturable_mxn"] = np.where(
        est.isin(ESTATUS_NO_FACTURABLE), 0.0, pd.to_numeric(df["monto_mxn"], errors="coerce")
    )

    # Duplicados de negocio: misma venta con folio distinto.
    llave = ["canal", "fecha", "id_producto", "cantidad", "monto_mxn", "cliente"]
    llave = [c for c in llave if c in df.columns]
    df["flag_duplicado_negocio"] = df.duplicated(subset=llave, keep="first")

    return df


def fase6(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n" + "=" * 74)
    print("  FASE 6: VALIDACION PYDANTIC Y ZONA DE CUARENTENA")
    print("=" * 74)

    df = preparar(df)
    limpias: list[dict] = []
    cuarentena: list[dict] = []

    for pos, fila in enumerate(df.to_dict(orient="records")):
        crudo = {k: (None if pd.isna(v) else v) for k, v in fila.items()
                 if not isinstance(v, (list, dict))}

        # Los duplicados de negocio se desvian antes del esquema: no son
        # invalidos en si mismos, sobran.
        if crudo.get("flag_duplicado_negocio"):
            cuarentena.append({**crudo, "motivo_rechazo_cuarentena":
                               "duplicado de negocio (misma venta con folio distinto)",
                               "regla_violada": "DEDUP", "fila_origen": pos})
            continue

        try:
            valido = TransaccionLimpia(**crudo)
            limpias.append(valido.model_dump())
        except ValidationError as e:
            motivos = []
            for err in e.errors():
                campo = ".".join(str(x) for x in err["loc"]) or "fila"
                msg = err["msg"].replace("Value error, ", "")
                motivos.append(f"{campo}: {msg}")
            cuarentena.append({
                **crudo,
                "motivo_rechazo_cuarentena": " | ".join(motivos),
                "regla_violada": ";".join(sorted({str(x["loc"][0]) if x["loc"] else "cruzada"
                                                  for x in e.errors()})),
                "fila_origen": pos,
            })
        except Exception as e:  # noqa: BLE001  ninguna fila puede tumbar el lote
            cuarentena.append({
                **crudo,
                "motivo_rechazo_cuarentena": f"error no controlado: {type(e).__name__}: {e}",
                "regla_violada": "EXCEPCION", "fila_origen": pos,
            })

    df_ok = pd.DataFrame(limpias)
    df_q = pd.DataFrame(cuarentena)

    registrar("6", "registros_procesados", len(df))
    registrar("6", "registros_validos", len(df_ok),
              f"{100*len(df_ok)/len(df):.2f}%" if len(df) else "")
    registrar("6", "registros_en_cuarentena", len(df_q),
              f"{100*len(df_q)/len(df):.2f}%" if len(df) else "")

    if len(df_q):
        print("\n  Motivos de cuarentena:")
        causa = df_q["motivo_rechazo_cuarentena"].apply(etiquetar_motivo)
        df_q["causa_principal"] = causa
        for motivo, n in causa.value_counts().items():
            print(f"    {n:>4}  {motivo}")
        causa.value_counts().rename_axis("causa").reset_index(name="registros").to_csv(
            SALIDA / "resumen_cuarentena.csv", index=False, encoding="utf-8-sig")

    return df_ok, df_q


# =============================================================== estres
def pruebas_de_estres() -> pd.DataFrame:
    """
    Inyecta registros deliberadamente corruptos.
    El criterio de exito no es que pasen: es que el pipeline los rechace
    sin lanzar una excepcion no controlada.
    """
    print("\n" + "=" * 74)
    print("  PRUEBAS DE ESTRES (inyeccion de datos corruptos)")
    print("=" * 74)

    base = {
        "id_registro": "TEST-001", "canal": "FISICA", "fuente": "estres",
        "id_producto": "PROD-001", "nombre_producto": "Prueba", "categoria": "Hogar",
        "cliente": "CTE-001", "correo": None, "cantidad": 2,
        "precio_unitario_mxn": 100.0, "monto_mxn": 200.0, "monto_facturable_mxn": 200.0,
        "monto_mxn_norm": 0.5, "segmento_monto": "MEDIO", "metodo_pago": "EFECTIVO",
        "estatus": "COMPLETADO", "fecha": date(2026, 6, 15),
        "flag_outlier": False, "flag_huerfano": False,
    }

    casos = [
        ("texto en campo numerico", {"cantidad": "muchos"}),
        ("cantidad negativa", {"cantidad": -5}),
        ("cantidad cero", {"cantidad": 0}),
        ("precio cero", {"precio_unitario_mxn": 0}),
        ("fecha imposible 31/02", {"fecha": "2026-02-31"}),
        ("fecha fuera de operacion", {"fecha": date(1999, 1, 1)}),
        ("fecha en el futuro lejano", {"fecha": date(2099, 12, 31)}),
        ("id_producto mal formado", {"id_producto": "PRODUCTO-1"}),
        ("id_producto vacio", {"id_producto": ""}),
        ("llave huerfana", {"flag_huerfano": True}),
        ("precio de relleno 500", {"precio_unitario_mxn": 500.0, "monto_mxn": 1000.0,
                                   "monto_facturable_mxn": 1000.0}),
        ("total inflado x80 (bot)", {"monto_mxn": 16000.0, "monto_facturable_mxn": 16000.0}),
        ("total inconsistente", {"monto_mxn": 999.0, "monto_facturable_mxn": 999.0}),
        ("correo con doble arroba", {"canal": "ONLINE", "correo": "usuario@@gmail..com"}),
        ("correo sin arroba", {"canal": "ONLINE", "correo": "correo_invalido.com"}),
        ("online sin correo", {"canal": "ONLINE", "correo": None}),
        ("canal inexistente", {"canal": "TELEFONO"}),
        ("estatus desconocido", {"estatus": "EN_LIMBO"}),
        ("cancelado con importe", {"estatus": "CANCELADO", "monto_facturable_mxn": 200.0}),
        ("segmento invalido", {"segmento_monto": "ALTISIMO"}),
        ("normalizado fuera de rango", {"monto_mxn_norm": 7.5}),
        ("campo obligatorio ausente", {"id_registro": None}),
        ("nulo en toda la fila", {k: None for k in base}),
        ("caso valido de control", {}),
    ]

    filas = []
    for nombre, mutacion in casos:
        datos = {**base, **mutacion}
        try:
            TransaccionLimpia(**datos)
            resultado, detalle = "ACEPTADO", ""
        except ValidationError as e:
            resultado = "RECHAZADO"
            detalle = e.errors()[0]["msg"].replace("Value error, ", "")[:70]
        except Exception as e:  # noqa: BLE001
            resultado = "CRASH"
            detalle = f"{type(e).__name__}: {e}"[:70]
        filas.append({"caso": nombre, "resultado": resultado, "detalle": detalle})
        marca = {"ACEPTADO": "ok", "RECHAZADO": "ok", "CRASH": "FALLA"}[resultado]
        print(f"  [{marca:>5}] {nombre:<34} -> {resultado:<10} {detalle}")

    res = pd.DataFrame(filas)
    crashes = int((res["resultado"] == "CRASH").sum())
    aceptados = res[res["resultado"] == "ACEPTADO"]["caso"].tolist()

    print(f"\n  Excepciones no controladas: {crashes} (debe ser 0)")
    print(f"  Casos aceptados: {aceptados}")
    registrar("6", "pruebas_estres", len(res), f"{crashes} crashes")
    assert crashes == 0, "El esquema lanzo una excepcion no controlada"
    assert aceptados == ["caso valido de control"], f"Se colo un caso invalido: {aceptados}"

    res.to_csv(SALIDA / "pruebas_estres.csv", index=False, encoding="utf-8-sig")
    return res


# =============================================================== main
def main() -> None:
    origen = ENTRADA / "unificado_grupo1.csv"
    if not origen.exists():
        print(f"[error] no existe {origen}. Corre antes fase3_fase4_pipeline.py")
        sys.exit(1)

    df = pd.read_csv(origen, parse_dates=["fecha"])
    df["fecha"] = df["fecha"].dt.date
    print(f"Entrada: {origen}  ({len(df)} filas)")

    df = fase5(df)
    df.to_csv(SALIDA / "unificado_fase5.csv", index=False, encoding="utf-8-sig")

    df_ok, df_q = fase6(df)
    df_ok.to_csv(SALIDA / "transacciones_limpias.csv", index=False, encoding="utf-8-sig")
    df_q.to_csv(SALIDA / "cuarentena_errores.csv", index=False, encoding="utf-8-sig")

    pruebas_de_estres()

    # Cuadre: ninguna fila puede perderse entre la entrada y las dos salidas.
    total = len(df_ok) + len(df_q)
    registrar("6", "cuadre_de_filas", f"{len(df_ok)} + {len(df_q)} = {total}",
              "igual a la entrada" if total == len(df) else "DESCUADRE")
    assert total == len(df), f"Se perdieron filas: {len(df)} -> {total}"

    pd.DataFrame(log).to_csv(SALIDA / "log_fase5_fase6.csv", index=False, encoding="utf-8-sig")
    with open(SALIDA / "resumen_fase5_fase6.json", "w", encoding="utf-8") as f:
        json.dump({"entrada": len(df), "validos": len(df_ok), "cuarentena": len(df_q),
                   "tasa_aceptacion_pct": round(100 * len(df_ok) / len(df), 2)},
                  f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 74)
    print(f"  Listo -> {SALIDA.resolve()}")
    print(f"  transacciones_limpias.csv  {len(df_ok)} filas")
    print(f"  cuarentena_errores.csv     {len(df_q)} filas")
    print("=" * 74)


if __name__ == "__main__":
    main()
