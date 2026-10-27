# Guía docente: del CSV al dashboard con Spark y Oracle

Cómo enseñar la capa de base de datos (Oracle) sobre Spark usando este repositorio como **implementación de referencia**, y cómo la implementa cada uno de los 3 proyectos de quinto (AMANKAY, STASYS, IPD), que son repositorios **independientes**.

> **Estado.** La referencia ya está implementada en este repositorio (versión 2.1.0 del servicio): los jobs de extracción, carga y conciliación, el backend completo y los puntos de conexión a la base de datos. **El adaptador de Oracle ya está implementado y configurado para un Oracle local** (`docker-compose.yml`; sección 11.6 del [README](../README.md)). Se validó contra H2 en modo Oracle, **no contra un Oracle real**, y el dashboard del backend todavía lee de archivos.

---

## 1. Enfoque

- Este repo (`00_IsaelFatama`) es la **referencia**: el docente implementa cada pieza aquí y la explica en clase.
- Cada equipo **replica la pieza en su propio repo** (no hay librería compartida: cada proyecto ya tiene su copia de `spark-core`).
- **No se reemplaza nada de lo existente.** Los jobs de transformación (el medallion de cada estudiante) siguen igual; los jobs nuevos se agregan a su alrededor, como una familia aparte (`JobKind`).
- Dentro de cada equipo se separan dos roles:
  - **Responsable de plataforma del equipo** (1 o 2 personas): porta los módulos comunes (`spark-database`, los jobs de extracción, carga y conciliación, y el backend). En la hoja hay dos filas sin job asignado (STASYS e IPD): son las candidatas naturales.
  - **Dueño de job** (cada estudiante): declara su job y sus tablas Gold en `JobType`, y consume los endpoints del dashboard.

### Qué trae el repositorio de referencia

| Pieza | Dónde | Qué enseña |
|-------|-------|------------|
| Familias de job (`JobKind`, `JobInput`) | `contracts` | Que el backend decida qué necesita un job leyendo el contrato, sin un `if` por job |
| Puertos de base de datos y punto de conexión | `spark/spark-database` | Arquitectura de puertos y adaptadores; dónde se conecta Oracle |
| `job-data-extraction`, `job-data-load`, `job-data-reconciliation` | `spark/` | Lectura de una base, `MERGE`, `REPLACE_BATCH` e idempotencia, conciliación |
| Backend en capas | `backend-api` | MVC con puertos sobre WebFlux, cola de ejecuciones, validación, errores |
| Repositorios de base de datos (marcadores) | `backend-api/.../repository/impl` | El segundo punto de conexión: el dashboard leyendo de la base |

El **despacho por parámetro** que se planteó (el backend recibe qué job ejecutar y se lo pasa a Spark) ya se cumple: el backend recibe `jobType`, y el `JobType` indica qué módulo lanzar. Un lanzador único con registro de jobs no hizo falta mientras cada job sea su propio módulo; sería la siguiente mejora si los jobs dejan de ser módulos separados.

### Flujo común a los 3 proyectos

```
CSV subidos por la API ─────────────────────────┐
                                                ▼
BD (STG_*) ─ DATA_EXTRACTION ─ landing/ ─▶ TRANSFORMACION (el job del estudiante)
                                            bronze → validación → cuarentena → silver → gate → gold ─▶ Parquet (+ S3)
                                                │
        DATA_LOAD   silver ─▶ CORE_*     (MERGE por clave natural; maestras primero)
                      gold   ─▶ SERVING_*  (REPLACE_BATCH con batch_id)
        DATA_RECONCILIATION  parquet = CORE = SERVING          ──▶ GET /api/dashboard/* ─▶ Dashboard
```

El backend acepta dos entradas para una transformación: los CSV subidos, o el `landing/` de una extracción publicada.

**Regla de carga (simple para que todos decidan igual)**

| Capa | Estrategia | Por qué |
|---|---|---|
| `CORE_*` (maestras y transaccionales limpias) | `MERGE` por clave natural | Reprocesar no duplica; permite corregir datos |
| `SERVING_*` (gold) | `REPLACE_BATCH`: cada fila lleva `batch_id`; el dashboard lee el último lote `PUBLISHED` | Idempotente, atómico, con historial |
| `ETL_*` (control, cuarentena) | `REPLACE_BATCH` | Auditoría |

Una base Oracle, un usuario (schema) por equipo: `AMANKAY`, `STASYS`, `IPD`.

---

## 2. Lo que enseña el docente (módulos de clase)

Cada módulo tiene demo en este repo y tarea en el repo de cada equipo.

| Módulo | Contenido | Tarea en el repo del equipo |
|---|---|---|
| M1 Diseño | Maestras vs transaccionales; capas STG / CORE / SERVING / ETL | Hoja de clasificación de las tablas de su proyecto |
| M2 Oracle y nombres | Tipos (`NUMBER`, `VARCHAR2`, `DATE`), `''` = NULL, MAYÚSCULAS sin comillas, `DatabaseNaming` | Script DDL del equipo |
| M3 Lectura JDBC | `fetchsize`, lectura por particiones, incremental por watermark | `OracleDatabase.read` y `job-data-extraction` |
| M4 Parquet y contrato entre jobs | landing/silver/gold, `BATCH_ID` (nombre de la carpeta), `RunOutputs` | Su job lee y escribe con el contrato |
| M5 Escritura | `MERGE`, `REPLACE_BATCH`, idempotencia | `OracleDatabase.write` y `job-data-load` |
| M6 Control | Reproceso, conciliación | `job-data-reconciliation` |
| M7 Backend | Subida de CSV, cola de ejecuciones, endpoints del dashboard | Los tres endpoints de su job (los genera el `JobType`) |
| M8 Integración | Flujo completo y demo | Sustentación |
| M9 (opcional) | Streaming con `foreachBatch` hacia Oracle | Solo AMANKAY (`ReportStreamJob`) |

Los módulos M2, M3 y M5 se enseñan sobre `OracleDatabase`, la única clase que habla con Oracle: el docente la explica y cada equipo la porta.

### Endpoints del dashboard

El backend de referencia no necesita un endpoint por job: sirve las tablas que el job declara en `JobType.goldTables()`.

| Endpoint | Devuelve |
|----------|----------|
| `GET /api/dashboard/{job}/tables` | Las tablas Gold del job |
| `GET /api/dashboard/{job}/summary` | Indicadores generales de la última ejecución publicada |
| `GET /api/dashboard/{job}/tables/{table}?page=&size=` | Una página de esa tabla |

Así, el estudiante que declara sus tablas Gold en `JobType` ya tiene su dashboard servido. Los nombres de tabla se validan contra esa lista (lista blanca), nunca se arma SQL con texto del usuario.

---

## 3. Trampas de Oracle que aparecen en los 3 proyectos

| Trampa | Dónde se ve | Qué hacer |
|---|---|---|
| Códigos con ceros a la izquierda (`UBIGEO`, `CO_IPRESS = 00018310`) | AMANKAY, STASYS, IPD | `VARCHAR2`, nunca `NUMBER` |
| `''` es NULL en Oracle | Todos | Normalizar vacíos a NULL antes de cargar |
| Convención de nombres distinta por proyecto | STASYS y AMANKAY usan columnas en MAYÚSCULA; IPD usa minúscula | `DatabaseNaming` normaliza en un único punto |
| Codificación de caracteres | INGEMMET y RENADE traen caracteres rotos (`�`, `¥`) | Leer con el charset correcto; base en `AL32UTF8` |
| Fechas en formatos distintos | STASYS: `dd-MM-yyyy HH:mm`, `d/M/yyyy`, `yyyyMMdd` | Convertir en silver; `DATE`/`TIMESTAMP` en CORE |
| Claves inestables | IGP: `ID` es un contador del archivo; denuncias: sin ID | Clave natural compuesta o `ROW_HASH` determinista |

---

## 4. AMANKAY (equipo 1): riesgos y fuentes oficiales

### 4.1 Fuentes y volumen

| Fuente | Filas aprox. | Particularidad |
|---|---|---|
| IGP (sismos) | 60 (muestra) | Separador `;`, fecha `19600113` y hora `154034` como enteros; `ID` es un contador |
| INDECI (emergencias) | 15 (muestra) | `UBIGEO` de 6 dígitos, `ID` y `CODIGO_SINPAD` estables |
| INGEMMET (peligros geológicos) | 42 650 | Separador `;`, texto largo, codificación rota, clave `COD_INV` |
| SENAMHI (hidrológico) | 37 | Cabecera de varias líneas, valores `S/D` y `T` |
| Open-Meteo (clima, Cañete) | 380 | Líneas de comentario `#`, serie diaria |
| Reportes ciudadanos | llegan por API | Contrato `CitizenReport` (id, instante, coordenadas, ubigeo, tipo) |

### 4.2 Clasificación

| Tipo | Tablas CORE | Clave |
|---|---|---|
| **Maestra** (geografía) | `CORE_UBIGEO` (departamento, provincia, distrito) | `UBIGEO` |
| **Transaccional** | `CORE_SISMO` | `FECHA_UTC + HORA_UTC + LATITUD + LONGITUD` (o hash) |
| | `CORE_EMERGENCIA` | `ID` (estable) |
| | `CORE_PELIGRO_GEOLOGICO` | `COD_INV` |
| | `CORE_CLIMA_DIARIO` | `FECHA + ESTACION` |
| | `CORE_REPORTE_CIUDADANO` | `REPORT_ID` |

Orden de carga: `CORE_UBIGEO` → resto. Dueño de la maestra: quien porte la plataforma del equipo.

### 4.3 Reparto por estudiante

| Job | Lee de CORE | Escribe en SERVING (de `JobType` y del código) | Endpoints sugeridos |
|---|---|---|---|
| `IgpBatchJob` | `CORE_SISMO` | `sismos_resumen`, `sismos_diarios`, `sismos_por_departamento` | resumen, serie diaria, por departamento |
| `IndeciBatchJob` | `CORE_EMERGENCIA`, `CORE_UBIGEO` | `emergencias_consolidadas`, `emergencias_por_tipo`, `emergencias_por_departamento`, `emergencias_diarias` | por tipo, por departamento, diarias |
| `IngemmetBatchJob` | `CORE_PELIGRO_GEOLOGICO`, `CORE_UBIGEO` | A definir (p. ej. peligros por tipo y por departamento) | por tipo, por departamento |
| `OpenMeteoBatchJob` | `CORE_CLIMA_DIARIO` | `open_meteo_resumen`, `open_meteo_diario`, `open_meteo_precipitacion` | serie diaria, precipitación |
| `ReportStreamJob` | `CORE_REPORTE_CIUDADANO` | `reportes_activos`, `senales_zonales` | reportes activos, señales por zona |

Además existe `PriorityJob` (`prioridad_zonas`, `alertas_candidatas`), que combina varias fuentes: es el único job que **consume los gold de otros**.

### 4.4 Particularidades

- **`ReportStreamJob` es el caso transaccional puro.** El reporte entra por `POST /api/reportes` y se inserta directo en `CORE_REPORTE_CIUDADANO` (inserción por evento, sin lote). Spark lo procesa en micro-lotes con `foreachBatch` y `MERGE` (módulo M9). Es el mejor ejemplo para contrastar con los jobs batch.
- **INGEMMET** (42 mil filas, texto largo): columnas largas como `VARCHAR2(4000)` o `CLOB`. Hoy `MAX_CELL_LENGTH` es 300: revisar que no recorte datos al migrar.
- **SENAMHI** no es tabular: parsear en Bronze y cargar solo la serie limpia.
- **Seguridad:** el `.env.example` versionado tiene credenciales AWS completas. Limpiarlo y rotarlas antes de continuar.
- **Coordinación de ramas:** 4 jobs batch viven en el mismo módulo `job-official-batch`; cada estudiante debe tocar solo su clase `*BatchJob` y su `*Analytics`.
- **`JobType` desactualizado:** solo declara `OFFICIAL_BATCH`, `REPORT_STREAM` y `PRIORITY`; los 4 jobs batch individuales no tienen entrada propia.

---

## 5. STASYS (equipo 2): salud, el mayor volumen

### 5.1 Fuentes y volumen

| Fuente | Filas aprox. | Clave en `StasysSchema` | Particularidad |
|---|---|---|---|
| RENIPRESS (establecimientos) | 26 850 | `COD_IPRESS` | Columnas de imágenes y URLs: no sirven para analítica |
| Consultas y denuncias | 50 850 | Sin ID propio | Fecha `dd-MM-yyyy HH:mm`; `CO_INSTITUCION` coincide con el código IPRESS |
| Producción asistencial | 14 170 | `ANHO, MES, UBIGEO, CO_IPRESS, SEXO, EDAD` | Ya viene agregada |
| Liquidaciones EPS | 739 700 | `NUMEXPEDIENTELIQUI_CRYPTO` | Identificadores seudonimizados (`_CRYPTO`) |
| TEDEF farmacia | 1 607 840 | `NUMERO_DOC_PAGO_CRIPTO + CORRELATIVO_ATENCION` | El volumen más grande; separador `;` |

`StasysSchema` ya declara `MAX_ROWS_PER_TABLE = 2 000 000`. Aquí sí importa que Spark lea por particiones.

### 5.2 Clasificación

| Tipo | Tablas CORE | Clave |
|---|---|---|
| **Maestra** (dimensión compartida) | `CORE_IPRESS` (desde RENIPRESS) | `COD_IPRESS` |
| **Transaccional** | `CORE_CONSULTA_DENUNCIA` | `ROW_HASH` (sin ID natural) |
| | `CORE_PRODUCCION_ASISTENCIAL` | `ANHO, MES, UBIGEO, CO_IPRESS, SEXO, EDAD` |
| | `CORE_LIQUIDACION_EPS` | `NUMEXPEDIENTELIQUI_CRYPTO` |
| | `CORE_ATENCION_TEDEF` | `NUMERO_DOC_PAGO_CRIPTO + CORRELATIVO_ATENCION` |

`CORE_IPRESS` es la **dimensión que los otros jobs consumen**: producción por IPRESS, montos EPS por IPRESS y casos por institución se cruzan con ella. Por eso el estudiante de `job-renipress` es dueño de la maestra y su carga debe ir **primero**. Ese orden es el ejemplo ideal para enseñar dependencias entre jobs.

RENIPRESS cambia en el tiempo (`ESTADO`, `CATEGORIA`): buen candidato para historial (columnas `VIGENTE_DESDE`, `VIGENTE_HASTA`, `ES_ACTUAL`) como ejercicio opcional.

### 5.3 Reparto por estudiante

| Job (hoja) | Módulo en el repo | Escribe en SERVING (de `JobType`) | Endpoints sugeridos |
|---|---|---|---|
| `job-renipress` | `job-renipress` | `ipress_por_departamento`, `ipress_por_categoria`, `ipress_por_estado`, `ipress_por_territorio` | cada tabla |
| `monitoring-cases-job` | `case-monitoring-job` | `casos_por_institucion`, `casos_por_tipo`, `casos_por_ubigeo`, `tendencia_mensual` | cada tabla |
| `job-asistencial-production` | `job-production-assistential` | `produccion_por_periodo`, `produccion_por_territorio`, `produccion_por_ipress` | cada tabla |
| `job-prestaciones-eps` | `job-eps-benefits` | `eps_settlement_alerts`, `amounts_by_ipress_and_period` | alertas, montos por IPRESS |
| `job-sis-attentions` | `job-sis-attentions` | A definir: no figura en `JobType` | A definir |

### 5.4 Particularidades

- **Lectura por particiones.** Las claves de EPS y TEDEF son hashes (texto), no sirven como columna de partición numérica. Usar `predicates` con `ORA_HASH(clave, N)` para leer en paralelo. Es el mejor ejemplo de por qué no basta `spark.read.jdbc` a secas.
- **Escritura masiva:** `batchsize` alto, nunca fila por fila; en CORE de TEDEF y EPS, tabla particionada por mes en Oracle.
- **Datos de salud:** los identificadores ya vienen seudonimizados. No cargar ni exponer campos que permitan reidentificar; el dashboard muestra agregados.
- **`JobType` fuera de sincronía:** declara `ALERTA_RIESGO` (no existe el módulo) y no declara `job-sis-attentions` (que sí existe). Alinear antes de generar la orquestación.
- **Confirmar la fuente de `job-sis-attentions`:** en `data/input` no hay un CSV de atenciones SIS; el job tiene su propio extractor y publicador a S3.
- **Credenciales:** `.gitignore` correcto. Es el equipo con mejor práctica en secretos.

---

## 6. IPD (equipo 3): deporte, dos niveles de datos

### 6.1 Fuentes y volumen

| Nivel | Fuente | Filas aprox. |
|---|---|---|
| **Crudo (RENADE)** | Clubes deportivos | 4 800 |
| | Clubes de fútbol | 4 350 |
| | Ligas deportivas | 390 |
| | Deportistas DECAN | 540 |
| | Eventos internacionales | 55 030 |
| | PAD deportistas beneficiados (con `MONTO`) | 68 940 |
| **Canónico** (producido por `job-procesamiento-datos`) | `disciplinas` (51), `competencias` (1 905), `atletas` (3 507), `clubs` (4 193), `ligas` (328), `decanato` (540), `participaciones` (11 808), `rendimientos` (11 808) | |

### 6.2 Clasificación (claves ya declaradas en `DeportivoSchema`)

| Tipo | Tablas CORE | Clave |
|---|---|---|
| **Maestras** | `CORE_DISCIPLINA` | `disciplina_id` |
| | `CORE_COMPETENCIA` | `competencia_id` |
| | `CORE_ATLETA` | `atleta_id` |
| | `CORE_CLUB` | `club_id` |
| | `CORE_LIGA` | `liga_id` |
| | `CORE_DECANATO` | `atleta_id` |
| **Transaccionales** | `CORE_PARTICIPACION` | `atleta_id, competencia_id, periodo` |
| | `CORE_RENDIMIENTO` | `atleta_id, competencia_id, periodo, indicador` |

`job-procesamiento-datos` es el equivalente a un job de **construcción de maestras**: transforma los crudos de RENADE en las tablas canónicas. Como los demás jobs dependen de sus IDs, **debe ejecutarse primero**.

### 6.3 Reparto por estudiante

| Job (hoja) | Lee de CORE | Escribe en SERVING (de `JobType`) |
|---|---|---|
| `job-analisis-disciplina` | participaciones, disciplinas, atletas, competencias | `participacion`, `disciplinas_resumen`, `atletas_resumen`, `competencias_resumen`, `resumen` |
| `job-cobertura-programas` | `CORE_CLUB` filtrado por tipo `FUTBOL`, disciplinas | `cobertura_por_disciplina`, `instituciones_registradas`, `altas_por_anio`, `resumen` |
| `job-rendimiento-atleta` | rendimientos, atletas, disciplinas | `rendimiento`, `atletas_resumen`, `disciplinas_resumen`, `distribucion_puntaje`, `resumen` |
| `job-clasificacion-competencia` | participaciones, competencias, disciplinas | `ranking_competencia`, `competencias_resumen`, `podio_competencia`, `disciplinas_resumen`, `resumen` |
| `job-participacion-deportiva` | participaciones, disciplinas, competencias | `participacion`, `disciplinas_comparadas`, `participacion_por_periodo`, `participacion_por_ubicacion`, `resumen` |
| `job-procesamiento-datos` (no figura en la hoja) | Crudos de RENADE | `disciplinas_preparadas`, `competencias_preparadas`, `atletas_preparados`, `rendimientos_preparados`, `participaciones_preparadas`, `control_calidad`, `resumen` |

### 6.4 Particularidades

- **IDs estables.** `atleta_id` (`ATL00001`) y similares los genera el propio proyecto. Para que el `MERGE` sea idempotente, el ID debe ser determinista (derivado de datos normalizados) o guardarse en una tabla de equivalencias; si se regenera con un contador, cada corrida duplica atletas.
- **Tablas repetidas por job:** `participacion`, `disciplinas_resumen`, `atletas_resumen` y `competencias_resumen` aparecen en varios jobs. En `SERVING` deben llevar el job en el nombre (`SERVING_ANALISIS_DISCIPLINA_PARTICIPACION`) o una columna `JOB`, para no pisarse.
- **Datos personales:** los nombres de deportistas son de registros públicos, pero el dashboard debe priorizar agregados y no exponer fechas de nacimiento ni correos.
- **Codificación:** los crudos traen caracteres rotos (`ZU¥IGA`); resolverlo en Bronze.
- **Montos PAD:** `NUMBER(12,2)`, nunca `FLOAT`.
- **CI:** IPD ya tiene `.gitlab-ci.yml` por módulo; sirve de modelo.
- **`.env`:** versionado y sin entrada en `.gitignore`; hoy solo trae bucket y región, pero corregirlo.

---

## 7. Plan de trabajo sugerido

| Fase | Quién | Resultado |
|---|---|---|
| F0 Higiene | Docente | Credenciales de AMANKAY limpias y rotadas; `.gitignore` de IPD; `.env.example` vacío |
| F1 Referencia | Docente | Hecho: jobs, backend y adaptador de Oracle (validado con H2). Falta correrlo contra un Oracle real y conectar el dashboard a la base |
| F2 Diseño | Cada equipo | Hoja de clasificación (M1) y DDL (M2) del proyecto |
| F3 Plataforma del equipo | Responsable de plataforma | `spark-database`, los tres jobs nuevos y el backend portados al repo del equipo |
| F4 Jobs | Dueños de job | Su job declarado en `JobType` con sus tablas Gold; el dashboard lo sirve el backend |
| F5 Integración | Todos | Flujo completo con conciliación cuadrada; demo |

Las duraciones dependen de la carga del curso.

---

## 8. Rúbrica sugerida

**Por estudiante**

| Criterio | Peso |
|---|---|
| Contrato y tablas declaradas (claves, tipos, capa) | 20 % |
| Job registrado en `JobType` y calidad intacta (`recibidas = aceptadas + rechazadas`) | 30 % |
| Endpoints (leen solo `SERVING`, con filtros y paginación) | 20 % |
| Pruebas | 15 % |
| Git y documentación (`feature/*`, merge request, README del job) | 15 % |

**Por equipo:** integración, conciliación cuadrada, orden de carga respetado (maestras primero), demo del flujo completo.

---

## 9. Mejoras prioritarias

1. **Secretos:** corregir `.env.example` de AMANKAY y `.gitignore` de IPD.
2. **`JobType` sincronizado con los módulos** en AMANKAY y STASYS.
3. **CI uniforme:** replicar el de IPD en AMANKAY y STASYS.
4. **Convención de nombres de jobs:** hoy conviven `job-*`, `*-job` y `*BatchJob`.
5. **Futuro (no necesario para la clase):** extraer `spark-core` a una librería versionada para no mantener 4 copias.
