# Reglas de negocio

Proyecto integrador ETL, Ingeniería de Datos (ATD-2602), TecNM Campus Colima.

Cada integrante documenta aquí las decisiones de su fase: qué se decidió, por qué y el efecto antes/después.
Cifras del Grupo 1 (tienda física y online).

## Configuración versionada (`config.yaml`)

Todos los umbrales de este documento se leen de `config.yaml`, que está en git. Cambiar un umbral
es un commit con autor, fecha y motivo, no una edición escondida en el código.

| Parámetro | Valor | Fase |
|---|---|---|
| `moneda.tipo_cambio_usd_mxn` | 19.50 | F4 (se aplica antes del IQR) |
| `outliers.factor_iqr` | 1.5 | F3 |
| `outliers.min_datos_por_grupo` | 8 | F3 |
| `segmentacion.cuantiles` / `etiquetas` | 3 / BAJO, MEDIO, ALTO | F5 (y el CHECK de F7) |
| `faltantes.alfa` | 0.05 | F2 (MCAR / MAR) |
| `validacion.tolerancia_pesos` | 0.01 | F6 R3b |
| `validacion.precio_relleno` | 500.00 | F6 R2 |
| `validacion.factor_bot` | 80 | F6 R3a |
| `validacion.inicio_operacion` / `fin_operacion` | 2026-01-01 / 2026-12-31 | F6 |
| `contrato.umbral_difuso` | 0.70 | F0 |

El cargador (`configuracion.py`) rechaza claves desconocidas o faltantes, tipos incorrectos y
valores fuera de rango. Ante una configuración inválida las fases se detienen con código 2 sin
escribir nada. Si el archivo no existe se usan exactamente estos valores. Con la configuración
versionada, las cifras de este documento no cambian: lo verifica `qa/suite_extensiones.py`.

## Fases 3 y 4: Outliers, integración y huérfanos

Script: `fase3_fase4_pipeline.py`. Orden por fuente: conversión USD a MXN, homologación de mayúsculas,
eliminación de imposibles, IQR con capping, fechas, cruce con catálogo y unificación.
La moneda se convierte antes del IQR para que los cuartiles no mezclen USD con MXN, y las mayúsculas se
homologan antes para que una categoría escrita de dos formas no quede partida en dos grupos.

### F3-1. Valores imposibles
Se eliminan las filas con cantidad o units menor o igual a 0 y, en ATM, las de monto negativo. Se hace antes
de calcular cuartiles.
Por qué: no son valores extremos sino registros que no pueden existir.
Efecto: 11 filas eliminadas en tienda física (6 con cantidad -1 y 5 con 0), guardadas en `eliminados_imposibles.csv`.
En online no hubo.

### F3-2. Outliers por IQR con capping
Para cantidad, monto_total_mxn, units y total_amount (monto_mxn y amount en el Grupo 2) se calcula
IQR = Q3 - Q1 con factor 1.5 y los valores se recortan a los límites (winsorizing). No se elimina ninguna fila
por ser outlier. En cantidades el límite se redondea hacia dentro para no generar fracciones de unidad.
Por qué: la guía pide no perder datos válidos; el capping quita el peso de los extremos sin borrar la transacción.

| Columna | Q1 | Q3 | Límite sup. | Recortados | Media antes / después | Máximo antes / después |
|---|---|---|---|---|---|---|
| tienda.cantidad | 1 | 4 | 8 | 13 | 8.80 / 2.71 | 500 / 8 |
| tienda.monto_total_mxn | 2,012.82 | 7,811.70 | 16,510.02 | 40 | 16,842.98 / 5,457.36 | 2,109,055 / 16,510.02 |
| online.units | 1 | 2 | 3 | 0 | 1.58 / 1.58 | 3 / 3 |
| online.total_amount (MXN) | 1,200.02 | 3,960.31 | 8,100.74 | 47 | 6,942.27 / 3,000.89 | 826,360.80 / 8,100.74 |

Los límites inferiores quedaron negativos o en 0, así que en la práctica solo se recortó por arriba.
Notas:
- En online, 35 de los 47 pedidos recortados no son bots: son pedidos de 2 o 3 unidades de productos caros
  (8,106 a 12,654 MXN). Quedan identificados con `flag_outlier` y su valor en `total_amount_original`.
- En tienda, cantidad y monto se recortan cada uno con su propio IQR; en esas filas el monto ya no es igual a
  cantidad por precio_unitario.

### F3-3. IQR por tipo de operación en ATM
El IQR de monto_mxn se calcula por tipo_operacion. Si un grupo tiene IQR igual a 0 (CONSULTA_SALDO siempre
vale 0), no se recorta.
Por qué: mezclar consultas en 0 con retiros y depósitos baja los cuartiles y recortaría montos normales.

### F3-4. Bots en ventas online
Hay 12 pedidos donde total_amount es 80 veces units por unit_price. Se recorta total_amount (lo reportado por la
tienda); `total_recalculado` (en MXN) y `flag_total_inconsistente` se conservan sin recortar para que la Fase 6
decida. Ejemplo: WEB-00508 pasó de 826,360.80 a 8,100.74; su total recalculado es 10,329.51.

### F3-5. Auditoría
Todo valor recortado conserva su valor previo en `<columna>_original`. Cada fila trae `flag_outlier` y
`columnas_recortadas`, y `log_outliers.csv` tiene una fila por valor recortado. Los umbrales están en
`umbrales_iqr.csv`.

### F4-1. Fechas
Todas las fechas quedan como YYYY-MM-DD en la columna `fecha`, con formatos explícitos por fuente (sin que pandas
adivine). La fecha original se conserva en `<columna>_original`.
- Tienda física: AAAA-MM-DD, DD/MM/AAAA y DD-MM-AAAA (el día va primero).
- ATM: AAAA/MM/DD y DD/MM/AAAA con hora, y AAAA-MM-DD con hora.
- Online y transferencias: texto ISO (con zona horaria en transferencias) y epoch entero. Un epoch se toma en
  milisegundos si es mayor o igual a 100,000,000,000 y en segundos si es menor (en segundos ese valor sería el año 5138).

Zona horaria: todo se expresa en hora local de México (America/Mexico_City). Los epoch están en UTC por definición
y los textos con zona traen su desfase, así que ambos se convierten a hora de México antes de tomar la fecha. Los
textos sin zona y las fechas de tienda y cajero ya están en hora local. De los 208 epoch de online, 49 caerían en
otro día si se tomaran en UTC.
Una fecha no convertible no se borra: se marca con `flag_fecha_invalida` y se agrega "fecha no convertible" a
`motivo_rechazo_cuarentena`.
Efecto: 392 fechas de tienda física (197 ISO, 195 DD/MM/AAAA, 197 DD-MM-AAAA) y 600 de online (392 ISO, 208 epoch
en segundos) quedaron en YYYY-MM-DD. 0 no convertibles.

### F4-2. Moneda
Los importes con currency igual a USD se multiplican por 19.50 (unit_price y total_amount en online; amount en
transferencias). currency queda en MXN, la moneda original en `moneda_original` y el valor sin convertir en
`<columna>_sin_convertir`.
Efecto: 193 pedidos online convertidos; 0 filas en USD después.

### F4-3. Categorías
metodo_pago, sucursal, order_status, payment_gateway, resultado, tipo_operacion, status, concept y platform se pasan
a mayúsculas sin espacios sobrantes.
Efecto: metodo_pago pasó de 6 a 5 valores (Efectivo/efectivo) y order_status de 6 a 5 (completado/COMPLETADO).

### F4-4. Huérfanos
Merge left con `indicator=True` contra el catálogo maestro (id_producto y product_id contra
`catalogo_productos.id_producto`; id_cuenta y account_id contra `catalogo_cuentas.id_cuenta`). Antes del merge se
verifica que el catálogo no tenga llaves duplicadas (0 duplicadas).
Los huérfanos no se borran: se marcan con `flag_huerfano` y `motivo_rechazo_cuarentena = "llave huérfana"`. El
desvío a cuarentena es responsabilidad de la Fase 6.
Efecto: 27 huérfanos en tienda física (4.58%) y 33 en online (5.5%). Todos los de tienda tienen precio_unitario
de 500, lo que confirma que son productos inexistentes.

### F4-5. Unificación
Las fuentes de cada grupo se unen en `unificado_grupo<n>.csv` con un esquema común y la columna `canal`
(FISICA y ONLINE en el Grupo 1; ATM y DIGITAL en el Grupo 2).
Efecto: 1,189 filas (589 física y 600 online).

### Verificación
- Fechas fuera de YYYY-MM-DD: 0.
- Filas en USD sin convertir: 0.
- Cuadre de filas: tienda 600 - 11 = 589; online 600 - 0 = 600.
- Robustez: se probó con texto en columnas numéricas, fecha 31/02, epoch absurdo, moneda EUR y columnas faltantes;
  el script no se detiene y lo registra en `log_fase4.csv`.

## Fases 5, 6 y 7: Escalamiento, validación Pydantic y carga a BD

Scripts: `fase5_fase6_pipeline.py` y `fase7_carga_bd.py`. Entrada: `unificado_grupo1.csv` (1,189 filas).
Las reglas de validación no se inventaron: salen del análisis forense de `utilidades/forense_datos_enganosos.py`,
que busca específicamente los errores que **no** se ven en un `head()` ni los detecta un chequeo de tipos.

### F5-1. Escalamiento Min-Max
`monto_mxn` y `cantidad` se llevan al rango [0, 1] con (x − mín) / (máx − mín).

| Variable | Mínimo | Máximo | Resultado |
|---|---|---|---|
| monto_mxn | 332.67 | 16,510.02 | monto_mxn_norm en [0, 1] |
| cantidad | 1 | 8 | cantidad_norm en [0, 1] |

Por qué después del capping: Min-Max usa únicamente el mínimo y el máximo, así que es la técnica más frágil
ante outliers. Con el monto sin recortar, el máximo era 2,109,055 y el 99% de las ventas quedaba comprimido
por debajo de 0.01. Recortar primero (Fase 3) y escalar después reparte la escala entre los datos reales.
Verificación: 0 valores fuera de [0, 1].

### F5-2. Discretización en 3 segmentos
`monto_mxn` se parte en BAJO, MEDIO y ALTO con `pd.qcut(q=3)` (terciles).

| Segmento | Registros | Rango real (MXN) | Promedio |
|---|---|---|---|
| BAJO | 403 | 332.67 – 2,016.30 | 1,139.72 |
| MEDIO | 391 | 2,048.98 – 4,138.14 | 3,008.55 |
| ALTO | 395 | 4,183.67 – 16,510.02 | 8,555.09 |

Por qué terciles y no cortes fijos en pesos: el catálogo mezcla categorías con precios muy distintos
(Alimentos contra Electrónica). Un corte fijo metería una categoría entera en el mismo segmento y dejaría
grupos vacíos. Los terciles garantizan tres grupos de tamaño comparable (403/391/395) y por lo tanto con
poder estadístico similar. Los cortes quedan registrados en `parametros_escalamiento.json` para que la
segmentación sea reproducible y auditable.

### F6-1. Datos que parecen correctos pero no lo son
Diez tipos de error silencioso detectados. Todos pasan una validación de tipo y formato; todos son falsos.

| Código | Hallazgo | Afectados | Por qué engaña |
|---|---|---|---|
| E-01 | Total distinto de cantidad × precio | 12 | Los tres campos son números válidos y positivos; solo se ve al recalcular |
| E-02 | Bots con total inflado exactamente ×80 | 12 | El factor es constante y exacto, parecen ventas grandes legítimas |
| E-03 | Precio 500.00 exacto (valor de relleno) | 29 | 500 es un importe plausible; lo delata repetirse 29 veces idéntico |
| E-04 | Llaves huérfanas con formato perfecto | 62 | Cumplen `PROD-NNN` y pasan cualquier regex; el producto no existe |
| E-05 | Correos que una validación ingenua aprueba | 11 | `usuario@@gmail..com` tiene arroba, dominio y `.com` |
| E-06 | Duplicados de negocio con folio distinto | 40 | `drop_duplicates()` no los toca porque el id_ticket es único |
| E-07 | Fechas válidas fuera del periodo operativo | 0 | Son fechas reales del calendario, el problema es el rango |
| E-08 | Importes en USD con magnitud de pesos | 0 | El número y la etiqueta son válidos por separado |
| E-09 | Filas que el propio capping dejó incoherentes | 40 | La incoherencia la causamos nosotros al recortar el monto |
| E-10 | Cancelados que conservan importe cobrado | 119 | Estatus válido y monto válido; la contradicción está en leerlos juntos |

Tres hallazgos cambiaron decisiones de diseño:
- **E-09** obligó a que el validador aritmético respete `flag_outlier`. Si la Fase 3 recortó el monto, la fila
  ya no cuadra por culpa nuestra, no del origen; castigarla sería mandar a cuarentena un dato que nosotros rompimos.
- **E-02** (bots) necesitó una regla propia, R3a. La Fase 3 recorta el total inflado ×80 al límite del IQR y marca
  `flag_outlier`, así que R3b lo dejaba pasar: 8 de los 12 bots llegaban a `transacciones_limpias` con un monto falso
  de 8,100.74. La bandera `flag_total_inconsistente` de la Fase 2 sobrevive al capping, se lleva al unificado y R3a
  rechaza todo pedido que la traiga. Efecto: 0 de 12 bots en la tabla limpia (8 etiquetados como bot; los otros 4
  ya caían por llave huérfana o correo).
- **E-10** no se manda a cuarentena. 119 registros (10%) son demasiados para descartar y un pedido cancelado
  sigue siendo un hecho real del negocio. Se resuelve con la columna derivada `monto_facturable_mxn`, que vale 0
  cuando el estatus es CANCELADO o DEVUELTO. El dato se conserva; lo que se corrige es cuánto factura.

### F6-2. Esquema Pydantic y reglas cruzadas
Clase `TransaccionLimpia(BaseModel)` con `@field_validator` por campo y `@model_validator(mode="after")`
para lo que ningún campo puede validar solo.

| Regla | Qué exige | Origen |
|---|---|---|
| R1 | La llave no puede ser huérfana | E-04 |
| R2 | `precio_unitario_mxn` distinto de 500.00 exacto | E-03 |
| R3a | Un pedido con `flag_total_inconsistente` (bot) va a cuarentena aunque ya esté recortado | E-02 |
| R3b | `monto = cantidad × precio` (± 0.01), salvo si `flag_outlier` | E-01, E-09 |
| R4 | Estatus no facturable ⇒ `monto_facturable_mxn = 0` | E-10 |
| R5 | Venta ONLINE obliga correo de contacto | E-05 |
| Campo | `id_producto` cumple `^PROD-\d{3}$` | E-04 |
| Campo | Correo con regex anclado, rechaza `@@` y `..` | E-05 |
| Campo | `fecha` dentro de [2026-01-01, 2026-12-31] | E-07 |
| Campo | `estatus` dentro del catálogo de 6 valores | — |

### F6-3. Bifurcación a cuarentena
Cada fila se valida aislada dentro de `try/except ValidationError`. Una fila corrupta nunca detiene el lote.

| Resultado | Registros | Porcentaje |
|---|---|---|
| transacciones_limpias | 1,049 | 88.23% |
| cuarentena_errores | 140 | 11.77% |

| Causa de rechazo | Registros |
|---|---|
| Llave huérfana (producto inexistente) | 57 |
| Correo con sintaxis inválida | 37 |
| Total distinto de cantidad × precio | 25 |
| Correo con `@@` o `..` | 11 |
| Total inflado ×80 (bot) | 8 |
| Duplicado de negocio | 2 |

Nota de lectura: los 60 huérfanos del unificado están los 60 en cuarentena, pero 3 aparecen etiquetados con su
error de correo. Pydantic evalúa los validadores de campo antes que el `model_validator`, así que la etiqueta
refleja el primer error encontrado, no el único. La columna `motivo_rechazo_cuarentena` conserva la lista completa.
Verificación: 0 huérfanos se filtraron a `transacciones_limpias`.

### F6-4. Pruebas de estrés
25 casos de inyección deliberada: texto en campos numéricos, cantidades negativas y cero, fecha 31/02,
fechas en 1999 y 2099, id mal formado, llave huérfana, precio de relleno, total inflado ×80, correo con doble
arroba, canal inexistente, estatus fuera de catálogo, segmento inválido, normalizado fuera de rango, campo
obligatorio ausente y una fila completamente nula.

Resultado: 24 rechazados con mensaje explícito, 1 aceptado (el caso de control válido), **0 excepciones no
controladas**. El criterio de éxito no es que el pipeline los procese, es que los rechace sin colapsar.

### F7-1. Carga transaccional
SQLite con dos tablas obligatorias. La integridad se declara en el esquema, no solo en Python:
`PRIMARY KEY` en `id_registro`, `CHECK` en cantidad, precios, rango del normalizado, canal y segmento,
`GLOB 'PROD-[0-9][0-9][0-9]'` en la llave de producto, y `NOT NULL` en los campos que el negocio exige.

Cada tabla se carga en una sola transacción: `BEGIN`, `DELETE` previo (idempotencia), `executemany`, `commit`.
Cualquier excepción dispara `rollback` y la tabla queda como estaba.

| Tabla | Filas |
|---|---|
| transacciones_limpias | 1,049 |
| cuarentena_errores | 140 |
| total | 1,189 (cuadra con la entrada) |

### F7-2. Prueba de rollback
Se intenta insertar una fila con `cantidad = -99`, que viola el `CHECK (cantidad > 0)`.
Resultado: `IntegrityError` capturado, `rollback` aplicado, conteo antes y después igual a 1,049.
La atomicidad queda demostrada contra la base real, no asumida.

### F7-3. Resultado de negocio
Monto facturable total: $4,074,888.52 MXN sobre 1,049 ventas válidas.

| Canal | Segmento | Pedidos | Facturado |
|---|---|---|---|
| FÍSICA | BAJO | 134 | $163,169.40 |
| FÍSICA | MEDIO | 161 | $480,872.27 |
| FÍSICA | ALTO | 265 | $2,521,190.44 |
| ONLINE | BAJO | 202 | $138,963.91 |
| ONLINE | MEDIO | 192 | $354,570.17 |
| ONLINE | ALTO | 95 | $416,122.33 |

Lectura: la tienda física concentra el 77.7% del importe facturado con menos pedidos, y su segmento ALTO solo
(265 pedidos) vale más que todo el canal online junto. El online vende volumen en el segmento BAJO.

### Motor universal: archivos enormes por bloques
Un CSV, TSV o JSON Lines mayor a `motor.bloques.umbral_mb` no se carga completo: se procesa en tres
pasadas por bloques. Las decisiones que dependen de todo el archivo (mediana para imputar, cuartiles
del IQR, mínimo y máximo de Min-Max, cortes de terciles, duplicados, coma decimal, epoch) se
calculan **una vez con el archivo completo** y se aplican igual a todos los bloques. Por eso el
resultado es idéntico al del modo en memoria. Diferencia documentada: en el modo por bloques el
duplicado exacto se compara sobre el texto normalizado de la Fase 0.

Corrección hecha al revisar el motor: el lector de CSV de Python descartaba **en silencio** una fila
con una celda de más de 128 KB. Ahora se conserva, de acuerdo con la regla de no perder filas.

### Verificación de las fases 5 a 7
- Valores de `monto_mxn_norm` fuera de [0, 1]: 0.
- Cuadre de filas: 1,049 + 140 = 1,189, igual a la entrada.
- Bots (`flag_total_inconsistente`) en `transacciones_limpias`: 0 de 12.
- Huérfanos en la tabla limpia: 0.
- Excepciones no controladas en las pruebas de estrés: 0 de 25 casos.
- Rollback verificado contra la base: tabla intacta tras violar un CHECK.
