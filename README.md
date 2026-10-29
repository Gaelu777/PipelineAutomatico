# PipelineAutomatico
Proyecto para Ingenieria de Datos 

Proyecto integrador ETL de Ingeniería de Datos (ATD-2602, TecNM Colima). Pipeline en Python por fases.

## Requisitos

```
pip install -r requirements.txt
```

`pydantic` se usa en la Fase 6 (validación). `pymupdf` solo hace falta si se quieren
reconstruir los datasets desde los PDF impresos (`utilidades/extraer_pdf.py`).

## Ejecución

Desde la raíz del repositorio, en este orden:

```
python fase1_fase2_pipeline.py datasets salidas_fase2
python fase3_fase4_pipeline.py salidas_fase2 datasets salidas_fase4
python fase5_fase6_pipeline.py salidas_fase4 salidas_fase6
python fase7_carga_bd.py      salidas_fase6 pipeline.db
```

| Script | Fases | Entrada | Salida principal |
|---|---|---|---|
| `fase1_fase2_pipeline.py` | 1 Diagnóstico, 2 Faltantes | `datasets/` | `salidas_fase2/<fuente>_fase2.csv` |
| `fase3_fase4_pipeline.py` | 3 Outliers IQR, 4 Integración y huérfanos | `salidas_fase2/` + catálogos de `datasets/` | `salidas_fase4/<fuente>_fase4.csv`, `unificado_grupo<n>.csv` |
| `fase5_fase6_pipeline.py` | 5 Min-Max y discretización, 6 Pydantic y cuarentena | `salidas_fase4/unificado_grupo1.csv` | `salidas_fase6/transacciones_limpias.csv`, `cuarentena_errores.csv` |
| `fase7_carga_bd.py` | 7 Carga transaccional | `salidas_fase6/` | `pipeline.db` (SQLite) |

Análisis de apoyo, no forma parte del pipeline:

```
python utilidades/forense_datos_enganosos.py datasets salidas_fase4
```

Busca los errores silenciosos: registros que pasan cualquier validación de tipo y formato
y aun así son falsos. Sus hallazgos son el origen de las reglas de la Fase 6.

## Resultado del pipeline

| Etapa | Filas |
|---|---|
| Crudo (620 física + 600 online) | 1,220 |
| Tras limpieza e integración (Fases 1-4) | 1,189 |
| `transacciones_limpias` | 1,049 (88.23%) |
| `cuarentena_errores` | 140 (11.77%) |

Motivos de cuarentena: 57 llaves huérfanas, 48 correos inválidos, 25 totales que no cuadran,
8 bots con total inflado x80 y 2 duplicados de negocio. Monto facturable validado: $4,074,888.52 MXN.

Pruebas de estrés: 25 casos de inyección de datos corruptos, 0 excepciones no controladas.
Rollback verificado contra la base de datos real.

## Inicio rápido (exposición)

1. Doble clic en `INSTALAR.bat` (solo la primera vez).
2. Doble clic en `INICIAR_PANEL.bat`: abre el panel en el navegador.
3. Pestaña **Prueba tu dataset**: arrastra cualquier archivo (CSV, TSV, Excel, JSON, JSON Lines,
   Parquet o ZIP) y pulsa *Procesar dataset*. Para una demo, usa `demo/profe_sucio.xlsx`.
4. Si algo falla, `dashboard_offline.html` abre con doble clic sin servidor ni internet.

Guía completa para la defensa: `GUIA_DEFENSA.pdf`.

## Motor universal

```
python motor/pipeline_universal.py <archivo_o_carpeta> [carpeta_salida]
```

Aplica las 7 fases a cualquier dataset sin conocer sus columnas: detecta codificación,
separador y coma decimal; infiere tipos; recalcula antes de imputar; clasifica MCAR/MAR;
IQR con capping; fechas y epoch a ISO; USD a MXN; detecta solo el catálogo maestro y los
huérfanos; Min-Max y terciles; esquema Pydantic generado en tiempo de ejecución y validado
por lotes; carga a SQLite con rollback verificado. Ninguna entrada lo tumba.

```
python qa/tortura_universal.py --grande
```

21 archivos raros o rotos (Excel multihoja, CP-1252 con `;`, JSON anidado, binario, vacío,
celdas de 2 MB, infinitos…) y un millón de filas en 40 s: 0 excepciones no controladas.

**Archivos enormes (más de 5 GB).** Los CSV, TSV, TXT y JSON Lines que superan
`motor.bloques.umbral_mb` (256 MB) se procesan **por bloques** (`chunksize`) en tres pasadas y
con memoria acotada:

1. Estructura: nulos, duplicados (un hash de 64 bits por fila) y una muestra aleatoria para inferir tipos.
2. Distribuciones: medianas, cuartiles, mínimos, máximos y tablas chi² **globales**.
3. Transformación: se aplican las fases 2 a 7 bloque por bloque **con los parámetros globales**,
   y CSV y SQLite se escriben de forma incremental en una sola transacción.

El modo por bloques produce las mismas salidas, byte a byte, que el modo en memoria; lo verifica
`qa/suite_extensiones.py`. Si algo falla a media carga se hace rollback y no quedan archivos parciales.
Limitación conocida: si una línea con columnas de más cae justo en la primera fila de un bloque, el
lector C de pandas la conserva recortada a las columnas del encabezado en vez de omitirla (no se pierde).

```
python motor/pipeline_universal.py <archivo> <salida> --umbral-mb 0.5   # forzar bloques
python qa/suite_extensiones.py --gigante 5.2                            # prueba con 5.2 GB
```

## Umbrales: `config.yaml`

Tipo de cambio (19.50), factor IQR (1.5), cuantiles de segmentación (terciles BAJO/MEDIO/ALTO),
alfa de MCAR/MAR, reglas de la Fase 6 y parámetros del motor viven en `config.yaml`, versionado
en git. Todas las fases y el motor lo leen con `configuracion.py`, un cargador con validación
estricta: rechaza claves desconocidas o faltantes, tipos incorrectos y valores fuera de rango.
Con la configuración versionada el resultado es idéntico al de siempre.

```
python configuracion.py      # muestra y valida la configuración efectiva
```

Las salidas guardan la huella de la configuración usada (`version` y `sha256`) en
`parametros_escalamiento.json`, `resumen_fase3_fase4.json` y el `reporte.json` del motor.

## Panel: pestaña «Antes / después»

Compara columna por columna el dato crudo contra `transacciones_limpias`: nulos, valores
distintos, mínimo, cuartiles, máximo, formatos de fecha y correos inválidos, con un diagrama de
caja antes y después. Se puede filtrar por canal (tienda física u online) y también funciona
con el último dataset subido en «Prueba tu dataset».

## Pruebas

```
python qa/suite_qa.py --completo     # 200 verificaciones con verdad conocida
python qa/tortura_universal.py       # 21 archivos raros o rotos
python qa/suite_extensiones.py       # config.yaml, bloques == memoria y panel en vivo
```

Las carpetas `salidas_fase*` no se versionan: se regeneran con los comandos de arriba.
Las fuentes que no existan en `datasets/` se omiten.

## Datasets

`datasets/` contiene el Grupo 1 (`catalogo_productos.csv`, `ventas_tienda_fisica.csv`, `ventas_online.json`).

Son los archivos **originales** entregados por el docente. Sustituyen a la versión reconstruida desde los PDF
impresos que se usó al inicio del proyecto, en la que el texto largo de algunas celdas llegaba truncado
(`Artículo Jugueter` en vez de `Artículo Juguetería Ref-001`). El script `utilidades/extraer_pdf.py` se conserva
por si hiciera falta repetir esa reconstrucción.

Las decisiones de limpieza están en `REGLAS_DE_NEGOCIO.md`.
