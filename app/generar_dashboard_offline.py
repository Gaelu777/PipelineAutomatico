"""
Genera `dashboard_offline.html`: un solo archivo con los datos ya incrustados.

Para que sirve: es la red de seguridad de la defensa. Si el dia de la
exposicion falla Python, el puerto esta ocupado, o la laptop no es la propia,
este archivo abre con doble clic en cualquier navegador, sin servidor y sin
internet. Muestra lo mismo que el panel salvo el boton de ejecutar el pipeline,
que por naturaleza necesita un proceso detras.

Uso:  python app/generar_dashboard_offline.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ESTATICO = Path(__file__).resolve().parent / "estatico"
sys.path.insert(0, str(Path(__file__).resolve().parent))

import servidor as S  # noqa: E402  (usa los mismos endpoints que el panel en vivo)


def recolectar() -> dict:
    return {
        "/api/resumen": S.api_resumen(),
        "/api/fases": S.api_fases(),
        "/api/enganosos": S.api_enganosos(),
        "/api/segmentos": S.api_segmentos(),
        "/api/estres": S.api_estres(),
        "/api/qa": S.api_qa(),
        "/api/serie": S.api_serie_temporal(),
        "/api/productos": S.api_top_productos(),
        "/api/comparacion": S.api_comparacion(None),
        "/api/comparacion?canal=FISICA": S.api_comparacion("FISICA"),
        "/api/comparacion?canal=ONLINE": S.api_comparacion("ONLINE"),
        "/api/cuarentena?limite=250": S.api_cuarentena(None, 250),
        "/api/cuarentena?limite=1": S.api_cuarentena(None, 1),
        "/api/estado": {"activo": False, "lineas": [], "fase": "", "paso": 0,
                        "total_pasos": 6, "ok": None, "segundos": 0},
    }


def main() -> None:
    html = (ESTATICO / "index.html").read_text(encoding="utf-8")
    css = (ESTATICO / "estilos.css").read_text(encoding="utf-8")
    js = (ESTATICO / "app.js").read_text(encoding="utf-8")

    datos = recolectar()
    # Cada causa de cuarentena tambien precargada, para que el filtro funcione sin red.
    for fila in datos["/api/cuarentena?limite=1"]["resumen"]:
        causa = fila["causa"]
        clave = f"/api/cuarentena?limite=250&causa={causa}"
        datos[clave] = S.api_cuarentena(causa, 250)

    precarga = (
        "<script>\n"
        "/* Datos incrustados: este archivo no hace ninguna peticion de red. */\n"
        f"const DATOS_OFFLINE = {json.dumps(datos, ensure_ascii=False, default=str)};\n"
        "window.fetch = (url) => {\n"
        "  const u = decodeURIComponent(String(url));\n"
        "  const k = Object.keys(DATOS_OFFLINE).find(x => decodeURIComponent(x) === u)\n"
        "         || Object.keys(DATOS_OFFLINE).find(x => u.startsWith(x.split('?')[0]));\n"
        "  return Promise.resolve({ ok: k !== undefined, status: k ? 200 : 404,\n"
        "    json: () => Promise.resolve(k ? DATOS_OFFLINE[k] : {}) });\n"
        "};\n"
        "</script>\n"
    )

    html = html.replace('<link rel="stylesheet" href="estilos.css">',
                        f"<style>\n{css}\n</style>")
    html = html.replace('<script src="app.js"></script>',
                        f"{precarga}<script>\n{js}\n</script>")
    html = html.replace(
        '<button class="tab" data-vista="ejecutar">Ejecutar pipeline</button>', "")
    html = html.replace('<button class="tab tab-destacada" data-vista="universal">Prueba tu dataset</button>', "")
    html = html.replace(
        "Panel sin dependencias externas: solo librería estándar de Python y JavaScript nativo.\n  Funciona sin internet.",
        "Versión offline: datos incrustados al generar el archivo. No requiere servidor ni internet. "
        "Para ejecutar el pipeline en vivo usa <code>python app/servidor.py</code>.")

    salida = RAIZ / "dashboard_offline.html"
    salida.write_text(html, encoding="utf-8")
    kb = salida.stat().st_size / 1024
    print(f"Generado: {salida}  ({kb:.0f} KB)")
    print("  Abre con doble clic. No necesita servidor, Python ni internet.")


if __name__ == "__main__":
    main()
