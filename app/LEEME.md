# Panel de control del pipeline

## Dos formas de usarlo

### 1. Panel en vivo (permite ejecutar el pipeline)

```
python app/servidor.py
```

Abre solo el navegador en `http://127.0.0.1:8000`. Si el puerto está ocupado,
busca el siguiente libre automáticamente.

En Windows también funciona con doble clic en `INICIAR_PANEL.bat`.

**No requiere instalar nada.** Usa únicamente la librería estándar de Python 3.10+.
Sin FastAPI, sin Flask, sin npm, sin CDN. Esa fue una decisión deliberada: el panel
se usa en una defensa en vivo y cada dependencia externa es un punto de falla más.

### 2. Respaldo offline (un solo archivo)

Abre `dashboard_offline.html` con doble clic. No necesita servidor, ni Python, ni
internet: los datos van incrustados en el archivo.

Para regenerarlo después de correr el pipeline:

```
python app/generar_dashboard_offline.py
```

Es la red de seguridad: si el día de la exposición falla Python, el puerto está
ocupado o la laptop no es la propia, este archivo funciona igual.

## Las vistas

| Vista | Qué muestra |
|---|---|
| Resumen | KPIs, bifurcación del lote y motivos de cuarentena |
| Las 7 fases | Embudo de registros a través del pipeline |
| Antes / después | Cada columna en crudo contra la tabla limpia (nulos, distintos, cuartiles, formatos), por canal o del dataset subido |
| Datos engañosos | Los 10 errores silenciosos y por qué engañan |
| Cuarentena | Registros desviados, filtrables por causa |
| Negocio | Facturación por canal y segmento, serie diaria, top productos |
| Pruebas de estrés | Los 24 casos de inyección de datos corruptos |
| QA a escala | Los 9 escenarios y el rendimiento según volumen |
| Ejecutar pipeline | Corre las 7 fases en vivo con consola en tiempo real |

## Estructura

```
app/
├── servidor.py                   servidor HTTP (librería estándar)
├── generar_dashboard_offline.py  construye el HTML autocontenido
├── LEEME.md                      este archivo
└── estatico/
    ├── index.html                estructura y menú
    ├── estilos.css               diseño
    └── app.js                    lógica y gráficas SVG escritas a mano
```

## API

| Endpoint | Devuelve |
|---|---|
| `GET /api/resumen` | KPIs principales |
| `GET /api/fases` | Embudo de las 8 etapas |
| `GET /api/enganosos` | Los 10 hallazgos forenses |
| `GET /api/cuarentena?causa=&limite=` | Registros desviados |
| `GET /api/segmentos` | Rangos y cruce canal × segmento |
| `GET /api/estres` | Resultados de las pruebas de estrés |
| `GET /api/qa` | Resultados de la suite de QA |
| `GET /api/serie` | Facturación por día |
| `GET /api/productos` | Top de productos por facturación |
| `GET /api/estado` | Estado de la ejecución en curso |
| `POST /api/ejecutar` | Lanza el pipeline completo |

Todos degradan a vacío si el archivo o la tabla no existen: el panel nunca revienta
por falta de datos.

## Prueba tu dataset (motor universal)

| Endpoint | Qué hace |
|---|---|
| `POST /api/universal/subir[?id=]` | Recibe un archivo como cuerpo crudo con el encabezado `X-Filename`. Hasta 2 GB, escrito a disco por bloques |
| `POST /api/universal/ejecutar?id=` | Corre las 7 fases sobre los archivos de esa corrida |
| `GET /api/universal/estado?id=` | Bitácora en vivo |
| `GET /api/universal/reporte?id=` | Resultado por fuente y por fase |
| `GET /api/universal/descargar?id=&f=` | CSV limpio, CSV de cuarentena, `resultado.db`, `reporte.json` |

Cada corrida vive en `corridas/<id>/` y no toca los datos del proyecto.
