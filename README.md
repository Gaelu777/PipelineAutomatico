# PipelineAutomatico
Proyecto para Ingenieria de Datos 

Proyecto integrador ETL de Ingeniería de Datos (ATD-2602, TecNM Colima). Pipeline en Python por fases.

## Requisitos

```
pip install pandas numpy scipy pymupdf
```

`pymupdf` solo se usa para reconstruir los datasets desde PDF.

## Ejecución

Desde la raíz del repositorio, en este orden:

```
python fase1_fase2_pipeline.py datasets salidas_fase2
python fase3_fase4_pipeline.py salidas_fase2 datasets salidas_fase4
```

| Script | Fases | Entrada | Salida principal |
|---|---|---|---|
| `fase1_fase2_pipeline.py` | 1 Diagnóstico, 2 Faltantes | `datasets/` | `salidas_fase2/<fuente>_fase2.csv` |
| `fase3_fase4_pipeline.py` | 3 Outliers IQR, 4 Integración y huérfanos | `salidas_fase2/` + catálogos de `datasets/` | `salidas_fase4/<fuente>_fase4.csv`, `unificado_grupo<n>.csv` |

Las carpetas `salidas_fase*` no se versionan: se regeneran con los comandos de arriba.
Las fuentes que no existan en `datasets/` se omiten.

## Datasets

`datasets/` contiene el Grupo 1 (`catalogo_productos.csv`, `ventas_tienda_fisica.csv`, `ventas_online.json`).
Se reconstruyeron a partir de los PDF impresos con:

```
python utilidades/extraer_pdf.py <carpeta_con_los_pdf> datasets
```

Los conteos coinciden con el perfil de la Fase 1 (620 filas en tienda física, 20 duplicados, 193 pedidos en USD, etc.).
Limitación: el PDF trunca el texto largo de algunas celdas (`TRANSFERENC`, `Manzanillo Puert`, `nombre_producto`);
números, fechas y llaves llegan completos. Si se consiguen los archivos originales, basta con reemplazarlos en `datasets/`.

Las decisiones de limpieza están en `REGLAS_DE_NEGOCIO.md`.
