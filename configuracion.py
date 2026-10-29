"""
Cargador de `config.yaml`: los umbrales del pipeline en un solo archivo versionado.

Decisiones de diseno:
- SOLO libreria estandar, para que tambien lo pueda usar el panel (app/).
  Si PyYAML esta instalado se usa; si no, un lector interno que entiende el
  subconjunto de YAML que usa config.yaml (mapas anidados, numeros, textos,
  booleanos y listas en linea). La suite de extensiones verifica que ambos
  lectores producen exactamente el mismo resultado.
- Validacion estricta: clave desconocida, clave faltante, tipo incorrecto o
  valor fuera de rango -> ErrorConfiguracion con un mensaje claro. Un error de
  dedo nunca se convierte en un umbral distinto sin que nadie lo note.
- Si config.yaml no existe se usan los valores documentados (identicos a los
  del archivo versionado) y se avisa. Asi la defensa en vivo no depende de que
  el archivo exista, pero nunca se usa un valor distinto al documentado.
- Se puede apuntar a otro archivo con la variable de entorno ETL_CONFIG
  (la usan las pruebas para correr el pipeline con otros umbrales).

Uso:
    from configuracion import cargar_o_salir
    CFG = cargar_o_salir()          # en scripts: mensaje claro y codigo 2 si falla
    CFG.tipo_cambio_usd_mxn         # 19.5

    python configuracion.py         # muestra la configuracion efectiva y la valida
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

RAIZ = Path(__file__).resolve().parent
RUTA_POR_DEFECTO = RAIZ / "config.yaml"
VARIABLE_ENTORNO = "ETL_CONFIG"

# Valores documentados. Deben ser identicos a config.yaml (lo verifica la QA).
DEFECTOS: dict[str, Any] = {
    "version": 1,
    "moneda": {"tipo_cambio_usd_mxn": 19.50},
    "outliers": {"factor_iqr": 1.5, "min_datos_por_grupo": 8},
    "segmentacion": {"cuantiles": 3, "etiquetas": ["BAJO", "MEDIO", "ALTO"]},
    "faltantes": {"alfa": 0.05, "umbral_columna_vacia": 0.60},
    "validacion": {"tolerancia_pesos": 0.01, "precio_relleno": 500.00, "factor_bot": 80,
                   "inicio_operacion": "2026-01-01", "fin_operacion": "2026-12-31"},
    "contrato": {"umbral_difuso": 0.70},
    "motor": {"max_columnas": 500, "max_categorias_chi2": 15, "lote_validacion": 50000,
              "bloques": {"umbral_mb": 256, "filas_por_bloque": 200000,
                          "filas_muestra_tipos": 100000,
                          "valores_muestra_cuantiles": 1000000}},
}

RE_ETIQUETA = re.compile(r"^[A-Z][A-Z0-9_]{0,19}$")


class ErrorConfiguracion(ValueError):
    """config.yaml existe pero no se puede usar (sintaxis, esquema o rango)."""


@dataclass(frozen=True)
class Configuracion:
    version: int
    tipo_cambio_usd_mxn: float
    factor_iqr: float
    min_datos_por_grupo_iqr: int
    cuantiles: int
    etiquetas: tuple[str, ...]
    alfa: float
    umbral_columna_vacia: float
    tolerancia_pesos: float
    precio_relleno: float
    factor_bot: int | float
    inicio_operacion: date
    fin_operacion: date
    umbral_difuso: float
    max_columnas: int
    max_categorias_chi2: int
    lote_validacion: int
    bloques_umbral_mb: float
    filas_por_bloque: int
    filas_muestra_tipos: int
    valores_muestra_cuantiles: int
    origen: str
    sha256: str

    def resumen(self) -> dict:
        """Lo que se anota en los reportes para saber con que umbrales se genero cada salida."""
        d = asdict(self)
        d["etiquetas"] = list(self.etiquetas)
        d["inicio_operacion"] = self.inicio_operacion.isoformat()
        d["fin_operacion"] = self.fin_operacion.isoformat()
        return d

    def huella(self) -> dict:
        return {"archivo": self.origen, "version": self.version, "sha256": self.sha256[:16]}


# ============================================================ lector de YAML
def _quitar_comentario(linea: str) -> str:
    """Quita un comentario '#' que no este dentro de comillas."""
    comilla = None
    for i, c in enumerate(linea):
        if comilla:
            if c == comilla:
                comilla = None
        elif c in "'\"":
            comilla = c
        elif c == "#" and (i == 0 or linea[i - 1] in " \t"):
            return linea[:i]
    return linea


_RE_ENTERO = re.compile(r"^[-+]?[0-9][0-9_]*$")
_RE_FLOTANTE = re.compile(r"^[-+]?(?:[0-9][0-9_]*\.[0-9_]*|\.[0-9_]+)(?:[eE][-+][0-9]+)?$")
_VERDADERO = {"true", "True", "TRUE", "yes", "Yes", "YES", "on", "On", "ON"}
_FALSO = {"false", "False", "FALSE", "no", "No", "NO", "off", "Off", "OFF"}
_NULO = {"null", "Null", "NULL", "~"}


def _partir_lista(t: str, n: int) -> list[str]:
    partes, actual, comilla = [], "", None
    for c in t:
        if comilla:
            actual += c
            if c == comilla:
                comilla = None
        elif c in "'\"":
            comilla = c
            actual += c
        elif c == ",":
            partes.append(actual)
            actual = ""
        elif c in "[]{}":
            raise ErrorConfiguracion(f"linea {n}: listas anidadas no soportadas")
        else:
            actual += c
    if comilla:
        raise ErrorConfiguracion(f"linea {n}: comilla sin cerrar")
    partes.append(actual)
    if partes and not partes[-1].strip():
        partes.pop()                     # coma final permitida: [a, b,]
    return partes


def _escalar(t: str, n: int) -> Any:
    t = t.strip()
    if not t:
        return None
    if t[0] in "'\"":
        if len(t) < 2 or t[-1] != t[0]:
            raise ErrorConfiguracion(f"linea {n}: texto entre comillas sin cerrar")
        interior = t[1:-1]
        return interior.replace("''", "'") if t[0] == "'" else interior.replace('\\"', '"')
    if t[0] == "[":
        if t[-1] != "]":
            raise ErrorConfiguracion(f"linea {n}: lista sin cerrar")
        cuerpo = t[1:-1].strip()
        return [_escalar(x, n) for x in _partir_lista(cuerpo, n)] if cuerpo else []
    if t[0] in "{&*!|>":
        raise ErrorConfiguracion(f"linea {n}: sintaxis YAML no soportada ({t[0]!r})")
    if t in _VERDADERO:
        return True
    if t in _FALSO:
        return False
    if t in _NULO:
        return None
    if _RE_ENTERO.match(t):
        return int(t.replace("_", ""))
    if _RE_FLOTANTE.match(t):
        return float(t.replace("_", ""))
    return t


def leer_yaml_simple(texto: str) -> dict:
    """Lector del subconjunto de YAML que usa config.yaml. Falla con numero de linea."""
    raiz: dict = {}
    # cada nivel: [indentacion de su clave, dict, indentacion de sus hijos o None]
    pila: list[list] = [[-1, raiz, None]]
    for n, cruda in enumerate(texto.splitlines(), 1):
        linea = _quitar_comentario(cruda).rstrip()
        if not linea.strip():
            continue
        sangria = linea[:len(linea) - len(linea.lstrip())]
        if "\t" in sangria:
            raise ErrorConfiguracion(f"linea {n}: usa espacios, no tabuladores, para sangrar")
        ind = len(sangria)
        contenido = linea.strip()
        if contenido.startswith(("- ", "---", "...")) or contenido == "-":
            raise ErrorConfiguracion(f"linea {n}: sintaxis YAML no soportada; usa listas en linea [a, b]")
        while ind <= pila[-1][0]:
            pila.pop()
        nivel = pila[-1]
        if nivel[2] is None:
            nivel[2] = ind
        elif ind != nivel[2]:
            raise ErrorConfiguracion(f"linea {n}: sangria inconsistente")
        if ":" not in contenido:
            raise ErrorConfiguracion(f"linea {n}: se esperaba 'clave: valor'")
        clave, _, resto = contenido.partition(":")
        clave = clave.strip().strip("'\"")
        if not clave:
            raise ErrorConfiguracion(f"linea {n}: clave vacia")
        if resto and not resto[0].isspace():
            raise ErrorConfiguracion(f"linea {n}: falta un espacio despues de ':'")
        if clave in nivel[1]:
            raise ErrorConfiguracion(f"linea {n}: clave repetida '{clave}'")
        if resto.strip():
            nivel[1][clave] = _escalar(resto, n)
        else:
            nuevo: dict = {}
            nivel[1][clave] = nuevo
            pila.append([ind, nuevo, None])
    # una clave sin valor ni hijos ("clave:") vale null, igual que en PyYAML
    def _vacios_a_nulo(d: dict) -> dict:
        return {k: (None if v == {} else _vacios_a_nulo(v) if isinstance(v, dict) else v)
                for k, v in d.items()}
    return _vacios_a_nulo(raiz)


def leer_yaml(texto: str, lector: str = "auto") -> Any:
    """lector: 'auto' (PyYAML si existe), 'pyyaml' o 'interno'."""
    if lector in ("auto", "pyyaml"):
        try:
            import yaml  # type: ignore
        except ImportError:
            if lector == "pyyaml":
                raise
        else:
            try:
                return yaml.safe_load(texto)
            except yaml.YAMLError as e:
                raise ErrorConfiguracion(f"sintaxis YAML invalida: {e}") from None
    return leer_yaml_simple(texto)


# =============================================================== validacion
def _numero(v: Any, ruta: str, minimo=None, maximo=None, *, entero=False,
            min_excl=False, max_excl=False) -> int | float:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ErrorConfiguracion(f"'{ruta}' debe ser un numero; se encontro {v!r}")
    if entero and not (isinstance(v, int) or float(v).is_integer()):
        raise ErrorConfiguracion(f"'{ruta}' debe ser un entero; se encontro {v!r}")
    if v != v or v in (float("inf"), float("-inf")):
        raise ErrorConfiguracion(f"'{ruta}' debe ser un numero finito")
    if minimo is not None and (v <= minimo if min_excl else v < minimo):
        raise ErrorConfiguracion(f"'{ruta}' = {v} fuera de rango (debe ser "
                                 f"{'>' if min_excl else '>='} {minimo})")
    if maximo is not None and (v >= maximo if max_excl else v > maximo):
        raise ErrorConfiguracion(f"'{ruta}' = {v} fuera de rango (debe ser "
                                 f"{'<' if max_excl else '<='} {maximo})")
    return int(v) if entero else float(v)


def _fecha(v: Any, ruta: str) -> date:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except ValueError:
        raise ErrorConfiguracion(f"'{ruta}' debe ser una fecha AAAA-MM-DD; se encontro {v!r}") from None


def _comparar_esquema(datos: Any, modelo: dict, prefijo: str = "") -> None:
    if not isinstance(datos, dict):
        raise ErrorConfiguracion(f"'{prefijo or 'raiz'}' debe ser un mapa de claves")
    sobran = sorted(set(datos) - set(modelo))
    faltan = sorted(set(modelo) - set(datos))
    if sobran:
        raise ErrorConfiguracion(f"clave desconocida: {', '.join(prefijo + k for k in sobran)} "
                                 "(revisa la ortografia)")
    if faltan:
        raise ErrorConfiguracion(f"falta la clave: {', '.join(prefijo + k for k in faltan)}")
    for k, v in modelo.items():
        if isinstance(v, dict):
            _comparar_esquema(datos[k], v, f"{prefijo}{k}.")


def validar(datos: Any, origen: str = "(memoria)", sha256: str = "") -> Configuracion:
    """Convierte el dict leido en una Configuracion validada o lanza ErrorConfiguracion."""
    _comparar_esquema(datos, DEFECTOS)
    m, o, s = datos["moneda"], datos["outliers"], datos["segmentacion"]
    f, v, c = datos["faltantes"], datos["validacion"], datos["contrato"]
    mo, b = datos["motor"], datos["motor"]["bloques"]

    cuantiles = _numero(s["cuantiles"], "segmentacion.cuantiles", 2, 10, entero=True)
    etiquetas = s["etiquetas"]
    if not isinstance(etiquetas, list) or not all(isinstance(e, str) for e in etiquetas):
        raise ErrorConfiguracion("'segmentacion.etiquetas' debe ser una lista de textos")
    if len(etiquetas) != cuantiles:
        raise ErrorConfiguracion(f"'segmentacion.etiquetas' tiene {len(etiquetas)} etiquetas y "
                                 f"'cuantiles' es {cuantiles}: debe haber una por cuantil")
    if len(set(etiquetas)) != len(etiquetas):
        raise ErrorConfiguracion("'segmentacion.etiquetas' no puede repetir etiquetas")
    for e in etiquetas:
        if not RE_ETIQUETA.match(e):
            raise ErrorConfiguracion(f"etiqueta invalida {e!r}: usa MAYUSCULAS, digitos o _ "
                                     "(maximo 20 caracteres)")

    inicio = _fecha(v["inicio_operacion"], "validacion.inicio_operacion")
    fin = _fecha(v["fin_operacion"], "validacion.fin_operacion")
    if inicio > fin:
        raise ErrorConfiguracion("'validacion.inicio_operacion' es posterior a 'fin_operacion'")

    bot = _numero(v["factor_bot"], "validacion.factor_bot", 1, 100_000, min_excl=True)
    return Configuracion(
        version=_numero(datos["version"], "version", 1, entero=True),
        tipo_cambio_usd_mxn=_numero(m["tipo_cambio_usd_mxn"], "moneda.tipo_cambio_usd_mxn",
                                    0, 1000, min_excl=True),
        factor_iqr=_numero(o["factor_iqr"], "outliers.factor_iqr", 0, 10, min_excl=True),
        min_datos_por_grupo_iqr=_numero(o["min_datos_por_grupo"], "outliers.min_datos_por_grupo",
                                        4, 1_000_000, entero=True),
        cuantiles=cuantiles,
        etiquetas=tuple(etiquetas),
        alfa=_numero(f["alfa"], "faltantes.alfa", 0, 0.5, min_excl=True),
        umbral_columna_vacia=_numero(f["umbral_columna_vacia"], "faltantes.umbral_columna_vacia",
                                     0, 1, min_excl=True),
        tolerancia_pesos=_numero(v["tolerancia_pesos"], "validacion.tolerancia_pesos", 0, 100),
        precio_relleno=_numero(v["precio_relleno"], "validacion.precio_relleno", 0,
                               min_excl=True),
        factor_bot=int(bot) if float(bot).is_integer() else bot,
        inicio_operacion=inicio,
        fin_operacion=fin,
        umbral_difuso=_numero(c["umbral_difuso"], "contrato.umbral_difuso", 0, 1, min_excl=True),
        max_columnas=_numero(mo["max_columnas"], "motor.max_columnas", 1, 100_000, entero=True),
        max_categorias_chi2=_numero(mo["max_categorias_chi2"], "motor.max_categorias_chi2", 2,
                                    1000, entero=True),
        lote_validacion=_numero(mo["lote_validacion"], "motor.lote_validacion", 100, 10_000_000,
                                entero=True),
        bloques_umbral_mb=_numero(b["umbral_mb"], "motor.bloques.umbral_mb", 0, min_excl=True),
        filas_por_bloque=_numero(b["filas_por_bloque"], "motor.bloques.filas_por_bloque", 100,
                                 50_000_000, entero=True),
        filas_muestra_tipos=_numero(b["filas_muestra_tipos"], "motor.bloques.filas_muestra_tipos",
                                    100, 50_000_000, entero=True),
        valores_muestra_cuantiles=_numero(b["valores_muestra_cuantiles"],
                                          "motor.bloques.valores_muestra_cuantiles", 1000,
                                          200_000_000, entero=True),
        origen=origen,
        sha256=sha256,
    )


# ==================================================================== carga
def ruta_activa(ruta: str | Path | None = None) -> Path:
    if ruta is not None:
        return Path(ruta)
    env = os.environ.get(VARIABLE_ENTORNO)
    return Path(env) if env else RUTA_POR_DEFECTO


def cargar(ruta: str | Path | None = None, lector: str = "auto") -> Configuracion:
    """
    Lee y valida la configuracion. Lanza ErrorConfiguracion si el archivo existe
    pero no es valido. Si no existe (y no se pidio uno explicito), usa DEFECTOS.
    """
    r = ruta_activa(ruta)
    explicito = ruta is not None or bool(os.environ.get(VARIABLE_ENTORNO))
    if not r.is_file():
        if explicito:
            raise ErrorConfiguracion(f"no existe el archivo de configuracion {r}")
        texto = json.dumps(DEFECTOS, sort_keys=True)
        return validar(copy.deepcopy(DEFECTOS), "valores por defecto (config.yaml no encontrado)",
                       hashlib.sha256(texto.encode()).hexdigest())
    try:
        crudo = r.read_bytes()
        texto = crudo.decode("utf-8-sig")
    except (OSError, UnicodeDecodeError) as e:
        raise ErrorConfiguracion(f"no se pudo leer {r.name}: {e}") from None
    datos = leer_yaml(texto, lector)
    try:
        nombre = str(r.resolve().relative_to(RAIZ))
    except ValueError:
        nombre = str(r)
    return validar(datos, nombre.replace("\\", "/"), hashlib.sha256(crudo).hexdigest())


def cargar_o_salir(ruta: str | Path | None = None) -> Configuracion:
    """Para scripts de consola: ante un error imprime un mensaje claro y sale con codigo 2."""
    try:
        cfg = cargar(ruta)
    except ErrorConfiguracion as e:
        print(f"[config] configuracion invalida en {ruta_activa(ruta)}: {e}", file=sys.stderr)
        print("[config] el pipeline no corre con umbrales dudosos: corrige el archivo "
              "(o restauralo con: git checkout config.yaml)", file=sys.stderr)
        sys.exit(2)
    if cfg.origen.startswith("valores por defecto"):
        print("[config] aviso: config.yaml no encontrado; se usan los valores documentados",
              file=sys.stderr)
    return cfg


if __name__ == "__main__":
    c = cargar_o_salir(sys.argv[1] if len(sys.argv) > 1 else None)
    print(json.dumps(c.resumen(), ensure_ascii=False, indent=2))
    print(f"\nOK: configuracion valida (version {c.version}, sha256 {c.sha256[:16]})")
