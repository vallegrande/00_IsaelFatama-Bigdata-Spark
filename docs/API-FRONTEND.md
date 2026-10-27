# Contrato de la API para el frontend

Documento para quien construye la interfaz. Describe cada endpoint, el JSON exacto que devuelve y cómo conviene usarlo. Todos los ejemplos son datos reales del lote de ejemplo (`data/con-errores`).

- [1. Lo básico](#1-lo-básico)
- [2. Flujo de pantallas recomendado](#2-flujo-de-pantallas-recomendado)
- [3. Enumeraciones](#3-enumeraciones)
- [4. Endpoints](#4-endpoints)
- [5. Tablas del dashboard](#5-tablas-del-dashboard)
- [6. Errores](#6-errores)
- [7. Tipos TypeScript](#7-tipos-typescript)

---

## 1. Lo básico

| Tema | Valor |
|------|-------|
| URL base (local) | `http://localhost:8080` |
| Formato | JSON en UTF-8 (también el cuerpo de `POST /api/executions`) |
| CORS | Permitido desde `http://localhost:3000` y `http://localhost:5173`. Otros orígenes se agregan en `bigdata.cors.allowed-origins` |
| Autenticación | Ninguna por ahora |
| Fechas | ISO-8601 en UTC, por ejemplo `2026-11-15T04:18:58.707742700Z` (los decimales de segundo varían) |
| Números | Son números JSON (`119`, `15.26`), no texto |
| Booleanos | `true` / `false` |
| Vacío | `null` |
| Códigos | Los identificadores con ceros a la izquierda o formato mixto (`00018310`, `2026-II`) llegan como **texto**: no los conviertas a número |
| Paginación | `page` empieza en `0`; `size` va de `1` a `500` (por defecto `50`) |
| Identificador de ejecución | Un UUID: `2db64008-20c4-46f6-bed0-261a1b725fa8` |

### Paginación

Las tablas devuelven siempre esta forma:

```json
{
  "content": [ { "...": "..." } ],
  "page": 0,
  "size": 50,
  "totalElements": 1071,
  "totalPages": 22
}
```

---

## 2. Flujo de pantallas recomendado

```
1. Catálogo            GET  /api/jobs                        → elegir el análisis
2. Elegir un dataset  POST /api/executions                  → 202 + id
3. Seguir el avance    GET  /api/executions/{id}             → repetir cada 2 a 3 s hasta un estado final
                       GET  /api/executions/{id}/events      → barra de etapas
4a. PUBLISHED          GET  /api/dashboard/{job}/...         → mostrar gráficos y tablas
4b. BLOCKED            GET  /quality  y  /rejected           → explicar por qué no se publicó
4c. FAILED             mostrar  execution.message
```

**Estados de una ejecución** (`execution.status`):

| Estado | Significa | Qué mostrar |
|--------|-----------|-------------|
| `PENDING` | En la cola, esperando un worker | "En cola" |
| `RUNNING` | Procesando | Progreso por etapa con `/events` |
| `PUBLISHED` | Terminó bien y hay datos | Habilitar el dashboard |
| `BLOCKED` | Terminó, pero la calidad de los datos no alcanzó la tolerancia: **no hay datos Gold** | Reporte de calidad y filas rechazadas |
| `FAILED` | Error técnico | `execution.message` |

Son finales `PUBLISHED`, `BLOCKED` y `FAILED`. Una ejecución dura entre medio minuto y tres minutos; con repetir la consulta cada 2 o 3 segundos basta.

**El dashboard no necesita el id de la ejecución.** `/api/dashboard/{job}/...` siempre devuelve el **último lote cargado en la base de datos** para ese análisis (el backend hace un `SELECT` de solo lectura) y dice de cuál viene (`executionId` es el lote; `publishedAt` es cuándo se cargó), de modo que el frontend puede mostrar "datos al lote X" sin guardar nada.

---

## 3. Enumeraciones

**`ExecutionStatus`**: `PENDING`, `RUNNING`, `PUBLISHED`, `BLOCKED`, `FAILED`.

**`JobType`** (el parámetro `jobType` y `{job}` aceptan cualquier combinación de mayúsculas y guiones: `academic-performance` equivale a `ACADEMIC_PERFORMANCE`):

| `type` | Para qué sirve | `kind` | `input` |
|--------|----------------|--------|---------|
| `ACADEMIC_PERFORMANCE` | Rendimiento académico (promedios, condición, asistencia) | `TRANSFORMATION` | `CSV_FILES` |
| `ATTENDANCE_ALERT` | Alertas de inasistencia | `TRANSFORMATION` | `CSV_FILES` |
| `DATA_INGESTION` | Carga los CSV de un dataset en la base de datos (capa STAGING) | `INGESTION` | `DATASET` |
| `DATA_EXTRACTION` | Lee de la base de datos y deja archivos | `EXTRACTION` | `NONE` |
| `DATA_LOAD` | Carga un resultado publicado a la base de datos | `LOAD` | `PREVIOUS_EXECUTION` |
| `DATA_RECONCILIATION` | Verifica que archivos y base de datos coinciden | `RECONCILIATION` | `PREVIOUS_EXECUTION` |

Solo los jobs `TRANSFORMATION` tienen dashboard.

**`JobInput`** indica qué debe enviar el cuerpo JSON: `CSV_FILES` (un `dataset`, o el `sourceExecutionId` de una extracción), `DATASET` (solo `dataset`), `NONE` (nada) o `PREVIOUS_EXECUTION` (`sourceExecutionId` de una transformación publicada).

**`PipelineStage`** (campo `stage` de los eventos), en el orden en que ocurren:

| Familia | Etapas |
|---------|--------|
| Transformación | `BRONZE`, `VALIDATION`, `QUARANTINE`, `SILVER`, `QUALITY_GATE`, `GOLD`, `EXPORT` |
| Extracción | `EXTRACT` |
| Carga | `LOAD_CORE`, `LOAD_SERVING` |
| Conciliación | `RECONCILE` |

**`StageStatus`** (campo `status` de los eventos): `RUNNING`, `COMPLETED`, `BLOCKED`, `SKIPPED`, `FAILED`.

---

## 4. Endpoints

### 4.1 Salud

`GET /actuator/health` → `{ "status": "UP" }`

### 4.2 Catálogo de jobs

`GET /api/jobs`

```json
[
  {
    "type": "ACADEMIC_PERFORMANCE",
    "label": "Academic performance",
    "kind": "TRANSFORMATION",
    "kindLabel": "Transformation",
    "input": "CSV_FILES",
    "module": "job-academic-performance",
    "defaultThreshold": 0.05,
    "goldTables": [
      "rendimiento", "cursos_resumen", "estudiantes_resumen", "secciones_resumen",
      "evaluaciones_resumen", "distribucion_notas", "asistencia_semanal"
    ]
  },
  {
    "type": "ATTENDANCE_ALERT",
    "label": "Attendance alert",
    "kind": "TRANSFORMATION",
    "kindLabel": "Transformation",
    "input": "CSV_FILES",
    "module": "job-attendance-alert",
    "defaultThreshold": 0.05,
    "goldTables": ["alertas_asistencia", "alertas_por_curso"]
  },
  {
    "type": "DATA_EXTRACTION",
    "label": "Data extraction from the database",
    "kind": "EXTRACTION",
    "kindLabel": "Extraction",
    "input": "NONE",
    "module": "job-data-extraction",
    "defaultThreshold": 0.0,
    "goldTables": []
  },
  {
    "type": "DATA_LOAD",
    "label": "Data load into the database",
    "kind": "LOAD",
    "kindLabel": "Load",
    "input": "PREVIOUS_EXECUTION",
    "module": "job-data-load",
    "defaultThreshold": 0.0,
    "goldTables": []
  },
  {
    "type": "DATA_RECONCILIATION",
    "label": "Reconciliation between files and the database",
    "kind": "RECONCILIATION",
    "kindLabel": "Reconciliation",
    "input": "PREVIOUS_EXECUTION",
    "module": "job-data-reconciliation",
    "defaultThreshold": 0.0,
    "goldTables": []
  }
]
```

`GET /api/jobs/{type}` devuelve un solo elemento con la misma forma. Un tipo inexistente responde `404`.

`defaultThreshold` es la tolerancia de filas rechazadas por tabla (0,05 = 5 %) que se usa si no se envía `threshold`.

### 4.3 Crear una ejecución

`POST /api/executions` con `Content-Type: application/json`. El API **no recibe archivos**: elige qué job lanzar y con qué entrada. Los datos ya viven en el servidor (carpetas de `data/`) o en la base.

| Campo | Tipo | Obligatorio | Descripción |
|-------|------|-------------|-------------|
| `jobType` | texto | Sí | Uno de `GET /api/jobs` |
| `dataset` | texto | Para un análisis, si no hay `sourceExecutionId` | Nombre devuelto por `GET /api/datasets` (por ejemplo `limpio`, `con-errores`) |
| `sourceExecutionId` | texto | Según el job | Id de la ejecución de origen: una extracción publicada (para analizar lo extraído) o una transformación publicada (para cargar o conciliar) |
| `threshold` | número | No | Entre `0` y `1` |

Un análisis (`TRANSFORMATION`) lleva **uno** de `dataset` o `sourceExecutionId`; la extracción no lleva ninguno; carga y conciliación llevan `sourceExecutionId`. Si no cuadra, responde `400` con el motivo.

Ejemplo en JavaScript:

```js
const datasets = await (await fetch('http://localhost:8080/api/datasets')).json();

const response = await fetch('http://localhost:8080/api/executions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ jobType: 'ACADEMIC_PERFORMANCE', dataset: datasets[0] }),
});
const execution = await response.json();
```

`GET /api/datasets` devuelve un arreglo de nombres, ordenado: `["con-errores","limpio"]`. Solo aparecen las carpetas que traen los 5 CSV. Si no hay ninguna, el arreglo es `[]`.

**Respuesta `202 Accepted`**, con el header `Location: /api/executions/{id}`:

```json
{
  "id": "2db64008-20c4-46f6-bed0-261a1b725fa8",
  "jobType": "ACADEMIC_PERFORMANCE",
  "jobLabel": "Academic performance",
  "kind": "TRANSFORMATION",
  "status": "PENDING",
  "threshold": 0.05,
  "sourceExecutionId": null,
  "createdAt": "2026-11-15T04:18:58.707742700Z",
  "startedAt": null,
  "finishedAt": null,
  "durationMs": null,
  "exitCode": null,
  "message": null
}
```

Los valores incorrectos de los datos (una nota de 25, una fecha inválida) no rechazan la petición: el análisis los aparta y los muestra en las filas rechazadas.

### 4.4 Listar ejecuciones

`GET /api/executions?jobType=academic-performance&status=published`

Ambos filtros son opcionales. Devuelve un arreglo de la forma de 4.3, de la más reciente a la más antigua. Un estado desconocido responde `400`.

### 4.5 Detalle y avance

`GET /api/executions/{id}`

```json
{
  "execution": {
    "id": "2db64008-20c4-46f6-bed0-261a1b725fa8",
    "jobType": "ACADEMIC_PERFORMANCE",
    "jobLabel": "Academic performance",
    "kind": "TRANSFORMATION",
    "status": "PUBLISHED",
    "threshold": 0.05,
    "sourceExecutionId": null,
    "createdAt": "2026-11-15T04:18:58.707742700Z",
    "startedAt": "2026-11-15T04:18:58.763216700Z",
    "finishedAt": "2026-11-15T04:20:41.936691300Z",
    "durationMs": 103173,
    "exitCode": 0,
    "message": null
  },
  "result": {
    "published": true,
    "received": 21759,
    "accepted": 21514,
    "rejected": 245,
    "rejectedRatio": 0.01126,
    "maxRejectedRatio": 0.05,
    "goldRows": 1071,
    "durationMs": 60527
  }
}
```

- `result` es `null` mientras la ejecución no termina.
- `message` trae el motivo cuando el estado es `FAILED` o `BLOCKED`.
- `received` es el total de filas leídas, `accepted` las válidas y `rejected` las que se apartaron.

### 4.6 Etapas (eventos)

`GET /api/executions/{id}/events` → arreglo (vacío si aún no empezó):

```json
[
  {
    "stage": "BRONZE",
    "status": "RUNNING",
    "timestamp": "2026-11-15T04:19:40.466508400Z",
    "rows": null,
    "durationMs": null,
    "sparkJobs": null,
    "detail": "Spark lee las cinco tablas CSV del sistema de notas"
  },
  {
    "stage": "BRONZE",
    "status": "COMPLETED",
    "timestamp": "2026-11-15T04:20:03.192691300Z",
    "rows": 21759,
    "durationMs": 22717,
    "sparkJobs": 15,
    "detail": "Filas leídas: semestres 6, cursos 9, estudiantes 123, notas 4331, asistencia 17290"
  }
]
```

Cada etapa genera dos eventos: uno `RUNNING` al empezar y uno `COMPLETED`, `BLOCKED` o `FAILED` al terminar. Las etapas que no llegaron a correr (porque una anterior bloqueó) aparecen como `SKIPPED`. Para una barra de progreso, cuenta las etapas con evento final sobre las 7 de la transformación.

### 4.7 Reporte de calidad

`GET /api/executions/{id}/quality` (`404` si aún no existe)

```json
{
  "rulesVersion": "2.0.0",
  "received": 21759,
  "accepted": 21514,
  "rejected": 245,
  "rejectedRatio": 0.01126,
  "maxRejectedRatio": 0.5,
  "published": true,
  "decision": "Publicado: las filas válidas pasan a Gold y las observadas quedan en cuarentena",
  "tables": [
    { "table": "notas", "received": 4331, "accepted": 4265, "rejected": 66, "rejectedRatio": 0.015239, "withinTolerance": true }
  ],
  "dimensions": { "COMPLETITUD": 1, "VALIDEZ": 35, "UNICIDAD": 13, "INTEGRIDAD": 191, "CONSISTENCIA": 5 },
  "reasons": [
    { "table": "estudiantes", "rule": "REGISTRO_DUPLICADO", "dimension": "UNICIDAD", "description": "La clave del registro ya existe", "rows": 2 }
  ],
  "checks": [
    { "name": "Rechazo en semestres", "passed": true, "detail": "0 de 6 filas rechazadas (0.00 %); máximo 50.00 %" }
  ]
}
```

- `tables`: una fila por tabla de entrada. `withinTolerance: false` es lo que bloquea la publicación.
- `dimensions`: filas rechazadas por dimensión de calidad. Sirve para un gráfico de barras.
- `reasons`: cuántas filas rechazó cada regla, por tabla.
- `checks`: los chequeos de la puerta de calidad con su explicación.

Reglas de calidad posibles (`reasons[].rule`) y su dimensión:

| Dimensión | Reglas |
|-----------|--------|
| `COMPLETITUD` | `CAMPO_REQUERIDO` |
| `VALIDEZ` | `NOTA_NO_NUMERICA`, `NOTA_FUERA_DE_RANGO`, `PESO_INVALIDO`, `ESTADO_INVALIDO`, `FECHA_INVALIDA`, `NUMERO_INVALIDO`, `CORREO_INVALIDO` |
| `UNICIDAD` | `REGISTRO_DUPLICADO` |
| `INTEGRIDAD` | `SEMESTRE_INEXISTENTE`, `ESTUDIANTE_INEXISTENTE`, `CURSO_INEXISTENTE` |
| `CONSISTENCIA` | `CURSO_DE_OTRO_SEMESTRE`, `PERIODO_INCONSISTENTE`, `FECHA_FUERA_DE_PERIODO` |

### 4.8 Filas rechazadas (cuarentena)

`GET /api/executions/{id}/rejected?page=0&size=20`

```json
{
  "content": [
    {
      "tabla": "asistencia",
      "fila": 1528,
      "clave": "VG260011 · EIA · 2026-II · 7",
      "motivos": "ESTADO_INVALIDO",
      "registro": "{\"estudiante_id\":\"VG260011\",\"curso_id\":\"EIA\",\"periodo\":\"2026-II\",\"semana\":\"7\",\"fecha\":\"2026-09-28\",\"estado\":\"X\"}"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 245,
  "totalPages": 13
}
```

- `fila` es el número de fila en el CSV original (el encabezado es la fila 1).
- `registro` es un **texto** que contiene un JSON: hazle `JSON.parse` para mostrarlo.
- `motivos` puede traer varias reglas separadas por coma.

### 4.9 Resultado de Gold de una ejecución

- `GET /api/executions/{id}/gold/summary`: indicadores generales (ver 5.1).
- `GET /api/executions/{id}/gold/{table}?page=0&size=50`: una página de la tabla.

Responden `404` si la ejecución no está `PUBLISHED` o si la tabla no pertenece a ese análisis. Para la interfaz normal usa el dashboard (4.10).

### 4.10 Dashboard (el último lote cargado en la base de datos)

| Endpoint | Devuelve |
|----------|----------|
| `GET /api/dashboard/{job}/tables` | `["rendimiento", "cursos_resumen", ...]` (no depende de que haya datos) |
| `GET /api/dashboard/{job}/summary` | Los indicadores generales |
| `GET /api/dashboard/{job}/tables/{table}?page=0&size=50` | Una página de la tabla |

`GET /api/dashboard/academic-performance/summary`:

```json
{
  "executionId": "2db64008-20c4-46f6-bed0-261a1b725fa8",
  "jobType": "ACADEMIC_PERFORMANCE",
  "publishedAt": "2026-11-15T04:20:41.936691300Z",
  "data": {
    "estudiantes": 119,
    "cursos": 9,
    "matriculas": 1071,
    "aprobados": 645,
    "desaprobados": 154,
    "inhabilitados": 22,
    "pendientes": 250,
    "promedio_general": 14.94,
    "asistencia_promedio": 0.9275,
    "requieren_atencion": 242,
    "correlacion_asistencia_nota": 0.4336,
    "periodo": "2026-II",
    "tasa_aprobacion": 0.7856,
    "nota_aprobatoria": 13,
    "maximo_inasistencia": 0.3
  }
}
```

`GET /api/dashboard/academic-performance/tables/cursos_resumen?size=2`:

```json
{
  "executionId": "2db64008-20c4-46f6-bed0-261a1b725fa8",
  "jobType": "ACADEMIC_PERFORMANCE",
  "publishedAt": "2026-11-15T04:20:41.936691300Z",
  "data": {
    "content": [
      {
        "curso_id": "APS",
        "curso": "Administración de Proyectos de Sistemas",
        "docente": "Mg. Carla Rojas Palomino",
        "creditos": 3,
        "periodo": "2026-II",
        "matriculados": 119,
        "aprobados": 100,
        "desaprobados": 19,
        "inhabilitados": 0,
        "pendientes": 0,
        "promedio": 15.26,
        "mediana": 15.2,
        "promedio_minimo": 9.4,
        "promedio_maximo": 20,
        "desviacion": 2.46,
        "asistencia_promedio": 0.933,
        "correlacion_asistencia_nota": 0.4256,
        "requieren_atencion": 23,
        "tasa_aprobacion": 0.8403
      }
    ],
    "page": 0,
    "size": 2,
    "totalElements": 9,
    "totalPages": 5
  }
}
```

Si todavía no se cargó ningún lote de ese análisis, responde `404` con `detail: "Aún no hay datos cargados en la base de Academic performance: ejecute la carga (DATA_LOAD)"`: muestra un estado vacío invitando a ejecutar el flujo. Si la base de datos no responde, el API devuelve `503` (`Base de datos no disponible`).

### 4.11 Resultado de una conciliación

`GET /api/executions/{id}/reconciliation` (solo para `DATA_RECONCILIATION`)

```json
{
  "batchId": "2db64008-20c4-46f6-bed0-261a1b725fa8",
  "expectedRows": 43321,
  "differenceRows": 0,
  "differenceRatio": 0.0,
  "tolerance": 0.0,
  "reconciled": true,
  "checks": [
    { "layer": "CORE", "table": "notas", "expected": 4265, "found": 4265, "difference": 0, "status": "OK" },
    { "layer": "SERVING", "table": "rendimiento", "expected": 1071, "found": 1071, "difference": 0, "status": "OK" }
  ]
}
```

`status` es `OK` o `DIFERENCIA`. (Ejemplo ilustrativo: los totales dependen del lote.)

---

## 5. Tablas del dashboard

Los nombres de columna están en español porque son los del dominio académico; no se traducen. Todas las columnas numéricas llegan como número o `null`.

### 5.1 `ACADEMIC_PERFORMANCE`

| Tabla | Una fila por | Columnas |
|-------|--------------|----------|
| `rendimiento` | Matrícula (estudiante y curso) | `estudiante_id`, `nombres`, `apellidos`, `seccion`, `semestre_id`, `periodo`, `curso_id`, `curso`, `docente`, `creditos`, `evaluaciones`, `evaluaciones_pendientes`, `promedio`, `nota_final`, `sesiones`, `presentes`, `tardanzas`, `faltas`, `justificadas`, `asistencia_pct`, `inasistencia_pct`, `condicion`, `requiere_atencion`, `alertas` |
| `cursos_resumen` | Curso | `curso_id`, `curso`, `docente`, `creditos`, `periodo`, `matriculados`, `aprobados`, `desaprobados`, `inhabilitados`, `pendientes`, `promedio`, `mediana`, `promedio_minimo`, `promedio_maximo`, `desviacion`, `asistencia_promedio`, `correlacion_asistencia_nota`, `requieren_atencion`, `tasa_aprobacion` |
| `estudiantes_resumen` | Estudiante | `estudiante_id`, `nombres`, `apellidos`, `seccion`, `semestre_id`, `periodo`, `cursos`, `aprobados`, `desaprobados`, `inhabilitados`, `pendientes`, `creditos_matriculados`, `creditos_aprobados`, `promedio_ponderado`, `asistencia_promedio`, `riesgo`, `orden_merito`, `tercio_superior` |
| `secciones_resumen` | Sección y curso | `seccion`, `curso_id`, `curso`, `estudiantes`, `promedio`, `aprobados`, `desaprobados`, `inhabilitados`, `asistencia_promedio`, `tasa_aprobacion` |
| `evaluaciones_resumen` | Evaluación de un curso | `curso_id`, `curso`, `evaluacion`, `peso`, `programadas`, `registradas`, `pendientes`, `promedio`, `aprobados`, `tasa_aprobacion`, `fecha` |
| `distribucion_notas` | Rango de notas de un curso | `curso_id`, `curso`, `rango`, `orden`, `estudiantes` |
| `asistencia_semanal` | Semana de un curso | `curso_id`, `curso`, `semana`, `fecha`, `registros`, `asistieron`, `faltas`, `justificadas`, `asistencia_pct` |

Valores con significado fijo:

| Campo | Valores |
|-------|---------|
| `rendimiento.condicion` | `APROBADO`, `DESAPROBADO`, `INHABILITADO`, `PENDIENTE` |
| `rendimiento.requiere_atencion`, `estudiantes_resumen.tercio_superior` | `true` o `false` |
| `estudiantes_resumen.riesgo` | `ALTO`, `MEDIO`, `BAJO` |
| `distribucion_notas.rango` | Texto como `00-10`; ordena por la columna `orden` |

Los porcentajes (`asistencia_pct`, `tasa_aprobacion`, `asistencia_promedio`) son **fracciones entre 0 y 1**: `0.9275` es 92,75 %. Multiplícalos por 100 al mostrarlos.

Ejemplo de una fila de `rendimiento`:

```json
{
  "estudiante_id": "VG260041", "nombres": "Fabricio", "apellidos": "Aguilar Huertas",
  "seccion": "B", "semestre_id": "V", "periodo": "2026-II",
  "curso_id": "APS", "curso": "Administración de Proyectos de Sistemas",
  "docente": "Mg. Carla Rojas Palomino", "creditos": 3,
  "evaluaciones": 4, "evaluaciones_pendientes": 0, "promedio": 16.1, "nota_final": 16,
  "sesiones": 16, "presentes": 15, "tardanzas": 1, "faltas": 0, "justificadas": 0,
  "asistencia_pct": 1, "inasistencia_pct": 0, "condicion": "APROBADO",
  "requiere_atencion": false, "alertas": null
}
```

### 5.2 `ATTENDANCE_ALERT`

| Tabla | Una fila por | Columnas |
|-------|--------------|----------|
| `alertas_asistencia` | Matrícula | `estudiante_id`, `nombres`, `apellidos`, `seccion`, `curso_id`, `curso`, `periodo`, `sesiones`, `faltas`, `justificadas`, `inasistencia_pct`, `faltas_permitidas`, `faltas_restantes`, `nivel` |
| `alertas_por_curso` | Curso | `curso_id`, `curso`, `matriculas`, `inhabilitados`, `en_riesgo`, `normales`, `inasistencia_promedio` |

`alertas_asistencia.nivel`: `INHABILITADO` (supera el 30 % de faltas), `EN_RIESGO` (desde el 20 %) o `NORMAL`. La tabla viene ordenada por severidad.

`GET /api/dashboard/attendance-alert/summary` → `data`:

```json
{
  "matriculas": 1080, "estudiantes": 120, "inhabilitados": 22, "en_riesgo": 44, "normales": 1014,
  "estudiantes_con_alerta": 37, "inasistencia_promedio": 0.0699, "periodo": "2026-II",
  "maximo_inasistencia": 0.3, "umbral_riesgo": 0.2
}
```

### 5.3 Sugerencia de pantallas

| Pantalla | Fuente |
|----------|--------|
| Tarjetas de indicadores | `/summary` |
| Ranking o tabla de cursos | `/tables/cursos_resumen` |
| Histograma de notas por curso | `/tables/distribucion_notas` (filtra por `curso_id`, ordena por `orden`) |
| Línea de asistencia por semana | `/tables/asistencia_semanal` |
| Tabla de estudiantes con filtros | `/tables/estudiantes_resumen` y `/tables/rendimiento` |
| Alertas de inasistencia | `/api/dashboard/attendance-alert/tables/alertas_asistencia` |

> La API pagina pero **todavía no filtra ni ordena** por columna: para un gráfico que necesite toda la tabla, pide `size=500` y recorre las páginas. Si necesitas filtros del lado del servidor, pídelo al equipo de backend.

---

## 6. Errores

Todos los errores usan el formato estándar `application/problem+json`:

```json
{
  "type": "about:blank",
  "title": "Petición inválida",
  "status": 400,
  "detail": "Faltan los archivos: asistencia, estudiantes, notas, semestres",
  "instance": "/api/executions"
}
```

Muestra `detail` al usuario: está redactado para leerse.

| Código | `title` | Cuándo ocurre |
|--------|---------|---------------|
| `400` | Petición inválida | Faltan archivos, encabezado incorrecto, tolerancia fuera de `0..1`, `page` o `size` fuera de rango, estado desconocido, falta `jobType` |
| `404` | No encontrado | Job inexistente, ejecución inexistente, ejecución sin Gold, tabla que el análisis no produce, aún no hay ejecución publicada |
| `415` | Unsupported Media Type | El cuerpo no es JSON (por ejemplo `multipart/form-data`) |
| `503` | Worker no disponible | El módulo de Spark no está compilado en el servidor (el `detail` dice cómo compilarlo) |
| `500` | Error interno | Error inesperado; sin detalle técnico |

---

## 7. Tipos TypeScript

```ts
export type ExecutionStatus = 'PENDING' | 'RUNNING' | 'PUBLISHED' | 'BLOCKED' | 'FAILED';
export type JobKind = 'TRANSFORMATION' | 'INGESTION' | 'EXTRACTION' | 'LOAD' | 'RECONCILIATION';
export type JobInput = 'CSV_FILES' | 'DATASET' | 'NONE' | 'PREVIOUS_EXECUTION';
export type JobType =
  | 'ACADEMIC_PERFORMANCE' | 'ATTENDANCE_ALERT'
  | 'DATA_INGESTION' | 'DATA_EXTRACTION' | 'DATA_LOAD' | 'DATA_RECONCILIATION';
export type StageStatus = 'RUNNING' | 'COMPLETED' | 'BLOCKED' | 'SKIPPED' | 'FAILED';
export type PipelineStage =
  | 'BRONZE' | 'VALIDATION' | 'QUARANTINE' | 'SILVER' | 'QUALITY_GATE' | 'GOLD' | 'EXPORT'
  | 'EXTRACT' | 'LOAD_CORE' | 'LOAD_SERVING' | 'RECONCILE';

export interface Job {
  type: JobType;
  label: string;
  kind: JobKind;
  kindLabel: string;
  input: JobInput;
  module: string;
  defaultThreshold: number;
  goldTables: string[];
}

export interface Execution {
  id: string;
  jobType: JobType;
  jobLabel: string;
  kind: JobKind;
  status: ExecutionStatus;
  threshold: number;
  sourceExecutionId: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  durationMs: number | null;
  exitCode: number | null;
  message: string | null;
}

export interface ExecutionResult {
  published: boolean;
  received: number;
  accepted: number;
  rejected: number;
  rejectedRatio: number;
  maxRejectedRatio: number;
  goldRows: number;
  durationMs: number;
}

export interface ExecutionDetail {
  execution: Execution;
  result: ExecutionResult | null;
}

export interface StageEvent {
  stage: PipelineStage;
  status: StageStatus;
  timestamp: string;
  rows: number | null;
  durationMs: number | null;
  sparkJobs: number | null;
  detail: string | null;
}

export interface Page<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}

export interface DashboardResponse<T> {
  executionId: string;
  jobType: JobType;
  publishedAt: string;
  data: T;
}

export interface QualityReport {
  rulesVersion: string;
  received: number;
  accepted: number;
  rejected: number;
  rejectedRatio: number;
  maxRejectedRatio: number;
  published: boolean;
  decision: string;
  tables: { table: string; received: number; accepted: number; rejected: number; rejectedRatio: number; withinTolerance: boolean }[];
  dimensions: Record<'COMPLETITUD' | 'VALIDEZ' | 'UNICIDAD' | 'INTEGRIDAD' | 'CONSISTENCIA', number>;
  reasons: { table: string; rule: string; dimension: string; description: string; rows: number }[];
  checks: { name: string; passed: boolean; detail: string }[];
}

export interface RejectedRow {
  tabla: string;
  fila: number;
  clave: string;
  motivos: string;
  registro: string;
}

export interface ProblemDetail {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance: string;
}

export interface CursoResumen {
  curso_id: string; curso: string; docente: string; creditos: number; periodo: string;
  matriculados: number; aprobados: number; desaprobados: number; inhabilitados: number; pendientes: number;
  promedio: number | null; mediana: number | null; promedio_minimo: number | null; promedio_maximo: number | null;
  desviacion: number | null; asistencia_promedio: number | null; correlacion_asistencia_nota: number | null;
  requieren_atencion: number; tasa_aprobacion: number | null;
}

export interface Rendimiento {
  estudiante_id: string; nombres: string; apellidos: string; seccion: string; semestre_id: string; periodo: string;
  curso_id: string; curso: string; docente: string; creditos: number;
  evaluaciones: number; evaluaciones_pendientes: number; promedio: number | null; nota_final: number | null;
  sesiones: number; presentes: number; tardanzas: number; faltas: number; justificadas: number;
  asistencia_pct: number | null; inasistencia_pct: number | null;
  condicion: 'APROBADO' | 'DESAPROBADO' | 'INHABILITADO' | 'PENDIENTE';
  requiere_atencion: boolean; alertas: string | null;
}

export interface AlertaAsistencia {
  estudiante_id: string; nombres: string; apellidos: string; seccion: string;
  curso_id: string; curso: string; periodo: string;
  sesiones: number; faltas: number; justificadas: number; inasistencia_pct: number;
  faltas_permitidas: number; faltas_restantes: number; nivel: 'INHABILITADO' | 'EN_RIESGO' | 'NORMAL';
}
```

> Las interfaces de las demás tablas siguen el mismo patrón: las columnas de la sección 5 son los nombres de los campos.
