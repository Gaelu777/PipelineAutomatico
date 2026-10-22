# Reglas de negocio

Proyecto integrador ETL, Ingeniería de Datos (ATD-2602), TecNM Campus Colima.

Cada integrante documenta aquí las decisiones de su fase: qué se decidió, por qué y el efecto antes/después.
Cifras del Grupo 1 (tienda física y online).

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
