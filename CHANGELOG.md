# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato se basa en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto sigue [Versionado Semántico](https://semver.org/lang/es/).

## [2.2.0] - 2026-11-15

El backend deja de recibir archivos y de escribir en la base de datos: solo decide qué job ejecutar y sirve la salida. La ingesta queda a cargo de `StagingSeeder` y de los jobs de extracción y carga.

### Cambiado

- **Las tablas ya no se crean desde Spark ni son temporales.** Se crean con los scripts de `db/oracle/` (`SchemaInstaller`). Cada tabla de carga tiene una gemela permanente `_LOAD` que conserva el lote para consultarlo; `OracleDatabase` solo escribe y falla con un mensaje claro si falta una tabla.
- El SQL pasa a ser constantes (`SqlStatements`) en lugar de generarse por código; `OracleSql` se elimina.
- `POST /api/executions` recibe un cuerpo JSON (`jobType`, `dataset` o `sourceExecutionId`, `threshold`) en lugar de `multipart/form-data`.
- Los datasets de entrada se leen de `bigdata.workspace.datasets-directory` (por defecto `data`).

### Agregado

- **`DATA_INGESTION`** (`job-data-ingestion`): carga los CSV de un dataset en las tablas `STG_*`. El flujo completo ya se maneja desde el backend: ingestar, extraer, transformar, cargar y conciliar. Nueva familia `INGESTION` con entrada `DATASET`.
- **El dashboard lee de la base de datos.** `JdbcServingRepository` hace `SELECT` de solo lectura sobre `SERVING_*` del último lote cargado; `SERVING_RESUMEN` guarda el resumen JSON de cada lote. El job de carga publica ahora todas las tablas gold (9) y el resumen; cada fila lleva `ROW_NO` (orden) y `LOADED_AT`. Si la base no responde, el API devuelve `503`.
- `GET /api/datasets`: carpetas de `data/` que traen los 5 CSV.
- `DatasetService` y `DatasetServiceImpl`, con sus pruebas.

### Eliminado

- Carga de CSV: `ExecutionFormReader`, `CsvStructureValidator`, `InputFileRepository`, `UploadedFile` y el error 413.
- Los repositorios de base de datos del backend (`DatabaseExecutionRepository`, `DatabaseGoldTableRepository`) y `bigdata.storage.mode`.
- Los archivos que dependen del esquema al portar el proyecto bajan de 8 a 5.

## [2.1.0] - 2026-11-14

Tres jobs nuevos (extracción, carga y conciliación) con el adaptador de Oracle y su configuración local, el backend completo y los jobs renombrados a inglés. La lógica de los jobs de transformación existentes no cambia.

### Agregado

#### Contratos (`contracts`)

- `JobKind` (`TRANSFORMATION`, `EXTRACTION`, `LOAD`, `RECONCILIATION`) y `JobInput` (`CSV_FILES`, `NONE`, `PREVIOUS_EXECUTION`): permiten al backend saber qué necesita cada job sin conocerlo.
- `DatabaseLayer` (`STAGING`, `CORE`, `SERVING`, `CONTROL`) con su prefijo de tabla y la columna `batch_id`.
- `JobType`: tres entradas nuevas (`DATA_EXTRACTION`, `DATA_LOAD`, `DATA_RECONCILIATION`) y la familia de cada job.
- `PipelineStage`: `EXTRACT`, `LOAD_CORE`, `LOAD_SERVING` y `RECONCILE`.
- `WorkerProtocol`: `LANDING`, `RECONCILIATION_FILE` y `EXECUTION_FILE`.

#### Spark

- Módulo `spark-database`: interfaz `Database` (`read` y `write`), `DatabaseConfig` (variables de entorno `DB_URL`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`; por defecto un Oracle local, solo `DB_PASSWORD` es obligatoria), `DatabaseNaming` (minúsculas del lago a mayúsculas de la base) y `RunOutputs`.
- `OracleDatabase`, la implementación de Oracle sobre JDBC: escribe siempre por una tabla temporal y una única transacción (`MERGE` o reemplazo de lote), elimina la temporal aunque falle, crea las tablas que faltan con tipos explícitos y mapea los `DATE` como fecha. Validado contra H2 en modo Oracle; **no se ha probado contra un Oracle real**.
- `StagingSeeder`: carga un lote de CSV en las tablas `STG_*` para preparar una base local.
- `docker-compose.yml` (Oracle Free) y `.env.example` para un Oracle local; las credenciales viven en `.env`, que Git ignora.
- `job-data-extraction`: lee las cinco tablas de la capa `STAGING` y las publica en `landing/`, con el mismo formato que la entrada CSV.
- `job-data-load`: silver a `CORE` con `MERGE` por clave natural (maestras primero), cuarentena a `CONTROL` y gold a `SERVING` con `batch_id` (`REPLACE_BATCH`, idempotente). No carga nada de una ejecución sin Gold.
- `job-data-reconciliation`: comprueba que silver existe en `CORE` y que cada tabla gold coincide exactamente en `SERVING` para el lote; escribe `reconciliation.json`.
- `PipelineContext.counts(...)`: los jobs sin validación informan sus conteos en `result.json`.
- 57 pruebas nuevas: con una base simulada en memoria y con el adaptador de Oracle contra H2 en modo Oracle.

#### Backend (`backend-api`)

- API WebFlux en capas (controlador, servicio, puertos y adaptadores): `/api/jobs`, `/api/executions` (con subida `multipart`, avance, calidad, conciliación, cuarentena y Gold) y `/api/dashboard/{job}` (siempre la última ejecución publicada).
- Cola de ejecuciones con límite de concurrencia (`bigdata.worker.max-concurrent`) y estados `PENDING`, `RUNNING`, `PUBLISHED`, `BLOCKED` y `FAILED`; las ejecuciones interrumpidas por un reinicio se marcan `FAILED` al arrancar.
- Lanzamiento del worker como proceso independiente (`ProcessWorkerLauncher`) con tiempo máximo y salida en `worker.log`.
- Una transformación puede tomar como entrada el `landing/` de una extracción publicada (`sourceExecutionId`) en lugar de archivos subidos.
- Validación de la estructura de los CSV (encabezado exacto y en orden, tamaño, filas) y subida en *streaming* con límite de tamaño.
- Errores en formato `ProblemDetail`; CORS configurable para el dashboard.
- Repositorios de archivos funcionales y marcadores de base de datos (`DatabaseExecutionRepository`, `DatabaseGoldTableRepository`) activos con `bigdata.storage.mode=database`.
- 110 pruebas, incluida una de la API completa con un worker simulado.

### Cambiado

- **Los jobs y la API pasan a inglés** (cambio incompatible para quien los invocaba por nombre). Los datos del dominio académico (tablas, columnas y archivos CSV) conservan sus nombres en español.

  | Antes | Ahora |
  |-------|-------|
  | `RENDIMIENTO_ACADEMICO`, `job-rendimiento-academico`, `RendimientoAcademicoJob` | `ACADEMIC_PERFORMANCE`, `job-academic-performance`, `AcademicPerformanceJob` |
  | `ALERTA_ASISTENCIA`, `job-alerta-asistencia`, `AlertaAsistenciaJob` | `ATTENDANCE_ALERT`, `job-attendance-alert`, `AttendanceAlertJob` |
  | `spark-academico` (paquete `spark.academico`) | `spark-academic` (paquete `spark.academic`) |
  | `/api/ejecuciones`, `/eventos`, `/calidad`, `/rechazados`, `/conciliacion` | `/api/executions`, `/events`, `/quality`, `/rejected`, `/reconciliation` |
  | `/api/dashboard/{job}/tablas`, `/resumen` | `/api/dashboard/{job}/tables`, `/summary` |

  Los paquetes de los jobs pasan de `job.rendimiento` y `job.asistencia` a `job.performance` y `job.attendance`. Las etiquetas visibles de cada job (`JobType.label()`, `JobKind.label()`) también están en inglés.
- **El backend se reorganiza en una arquitectura MVC estándar**, sin cambiar su comportamiento (las mismas rutas y respuestas, y la misma prueba de flujo completo):

  | Antes | Ahora |
  |-------|-------|
  | `web.controller` (`*Controller`) | `rest` (`JobRest`, `ExecutionRest`, `DashboardRest`) |
  | `web.dto`, `web.request`, `model.PageQuery`, `model.PageResult` | `dto.request` (`CreateExecutionRequest`, `PageQuery`) y `dto.response` (`ExecutionResponse`, `PageResponse`, `DashboardResponse`…) |
  | Clases de servicio concretas | Interfaz en `service` e implementación `*Impl` en `service.impl` |
  | `repository.file`, `repository.database`, `ExecutionOutputReader`, `InputStorage` | `repository.impl` (`File*` y `Database*`), `ExecutionOutputRepository`, `InputFileRepository` |
  | `ExecutionRunner`, `ExecutionRecovery`, `CsvStructureValidator`, `workspace.ExecutionWorkspace` | `component` |
  | `web.error.ApiExceptionHandler` | `exception.GlobalExceptionHandler` |
  | Conversión modelo a DTO dentro de los DTO | `mapper` (`ExecutionMapper`, `JobMapper`) |

  Se adopta Lombok (`@RequiredArgsConstructor`, `@Slf4j`, `@Builder`) y se eliminan los comentarios del código del backend. `POST` sin `jobType` responde `400` (antes respondía `404`). 110 pruebas.
- `BigDataApplication` ahora habilita `@ConfigurationPropertiesScan`.
- La prueba de contexto del backend pasó a un paquete que Spring sí encuentra (`pe.edu.vallegrande.bigdata.api`), con lo que `mvn verify` vuelve a funcionar en todo el proyecto.

## [2.0.0] - 2026-10-02

Reestructuración del servicio Spark en un motor de pipeline reutilizable con dos jobs analíticos, más la documentación completa del proyecto.

### Agregado

#### Contratos (`contracts`)

- Enum `JobType` con el catálogo de jobs disponibles (`RENDIMIENTO_ACADEMICO`, `ALERTA_ASISTENCIA`). Cada entrada define módulo, JAR, clase `main`, etiqueta legible y tablas Gold que produce. `JobType.DEFAULT` apunta a `RENDIMIENTO_ACADEMICO`.
- `GradingPolicy.RISK_ABSENCE_RATIO` (20 %) y las condiciones `EN_RIESGO` y `NORMAL` para las alertas de inasistencia.

#### Motor del pipeline (`spark/spark-core`)

- `Pipeline`, `PipelineStep`, `PipelineContext`, `StepResult` y `PipelineResult`: ejecución ordenada de etapas con estado compartido, bloqueo por calidad y liberación de los `Dataset` cacheados al terminar.
- `PipelineTracker`: escribe cada transición de etapa (`RUNNING`, `COMPLETED`, `BLOCKED`, `SKIPPED`, `FAILED`) en `events.jsonl` con filas, duración y número de jobs de Spark, y agrupa los jobs de cada etapa en la Spark UI mediante *job groups*.
- `JobDefinition` y `JobLauncher`: punto de entrada común para todos los jobs. Valida argumentos (`<input> <output> <threshold> [spark-ui-seconds]`), escribe `result.json` y termina con el código de salida del `WorkerProtocol` (0, 1 o 2).
- Etapas reutilizables `QuarantineStep`, `SilverStep`, `QualityGateStep`, `GoldStep` y `ExportStep`, con el punto de extensión `GoldAnalytics`.
- `QualityGate`, `QualityCheck` y `QualityReport`: tolerancia de rechazo por tabla, chequeos adicionales, conservación de filas (`recibidas = aceptadas + rechazadas`) y conteo por dimensión de calidad en `quality.json`.
- Publicación de tablas con `TablePublisher`, `CsvPublisher` (UTF-8 con BOM y protección contra inyección de fórmulas) y `ParquetPublisher` (un archivo por tabla), y de documentos con `JsonPublisher`.
- `AtomicFiles`: toda escritura pasa por un archivo `.tmp` y un movimiento atómico, con hasta 10 reintentos si Windows bloquea el destino.
- `BronzeTables`, `ValidationResult` y `GoldTables` como representación de cada capa.
- La Spark UI puede quedar abierta N segundos al terminar el job (cuarto argumento) y la sesión escribe `spark.json` con su información.

#### Dominio académico (`spark/spark-academico`)

- `AcademicSteps.medallion(...)`: ensambla las 7 etapas (Bronze, Validation, Quarantine, Silver, Quality Gate, Gold, Export) para cualquier job académico.
- `CsvExtractor`: lectura de las 5 tablas con esquema de texto, recorte de espacios, vacíos como `null` y número de fila original.
- Un validador por tabla (`SemestresValidator`, `CursosValidator`, `EstudiantesValidator`, `NotasValidator`, `AsistenciaValidator`) ejecutados en orden de dependencia con `ReferenceData`.
- `ValidationRules`: campo requerido, detección de duplicados (se conserva la primera fila válida) y formato de la tabla de cuarentena (`tabla`, `fila`, `clave`, `motivos`, `registro`).
- `MasterTablesCheck`: exige al menos una fila válida en `semestres`, `cursos` y `estudiantes`.
- 15 reglas de calidad en 5 dimensiones (completitud, validez, unicidad, integridad y consistencia), versión de reglas `2.0.0`.

#### Job `job-rendimiento-academico`

- Tabla principal `rendimiento` por matrícula: promedio ponderado, nota final con redondeo desde ,5, asistencia, inasistencia, condición (`APROBADO`, `DESAPROBADO`, `INHABILITADO`, `PENDIENTE`), indicador de atención y alertas.
- Tablas `cursos_resumen`, `estudiantes_resumen` (con orden de mérito y tercio superior), `secciones_resumen`, `evaluaciones_resumen`, `distribucion_notas`, `asistencia_semanal` y `gold/resumen.json`.

#### Job `job-alerta-asistencia`

- Tabla `alertas_asistencia` por matrícula con faltas permitidas, faltas restantes y nivel (`INHABILITADO` sobre 30 %, `EN_RIESGO` desde 20 %, `NORMAL`).
- Tabla `alertas_por_curso` y `gold/resumen.json` con estudiantes con alerta e inasistencia promedio.

#### Empaquetado

- Cada job se empaqueta como `target/<job>.jar` con `Main-Class` y `Class-Path: lib/`, más la carpeta `target/lib/` con las dependencias de runtime.

#### Pruebas

- `RendimientoAcademicoJobTests` (4 pruebas) y `AlertaAsistenciaJobTests` (3 pruebas): ejemplo manual con resultados esperados fila por fila, lote limpio (21 735 filas, 0 rechazos, 1 080 matrículas), lote con errores publicado con tolerancia 5 % (245 rechazos) y bloqueado con tolerancia 1 %.

#### Documentación

- `README.md` completo: visión general, arquitectura, módulos, pipeline Medallion, modelo de datos, reglas de calidad, puerta de calidad, tablas Gold y fórmulas de negocio, protocolo del worker, configuración, compilación y ejecución, pruebas, guía de extensión, decisiones de diseño, archivos importantes, solución de problemas y glosario.
- Diagramas animados en `docs/assets/`: `pipeline-medallion.svg` (etapas del pipeline) y `flujo-end-to-end.svg` (frontend, backend-api, worker Spark y resultados).
- Diagramas Mermaid de módulos, secuencia de orquestación, clases del motor, ciclo de vida de etapas, dependencias de validación y modelo entidad-relación.
- Este `CHANGELOG.md`.

### Cambiado

- El módulo único `spark-worker` se reemplaza por el agregador `spark` con cuatro módulos: `spark-core`, `spark-academico`, `job-rendimiento-academico` y `job-alerta-asistencia`.
- `WorkerConfig` y `application.yaml` pasan a `spark-core`. El nombre por defecto de la aplicación cambia de `Vallegrande Academic Pipeline` a `Vallegrande Spark`.
- `WorkerConfig.forJob(...)` ahora recibe el nombre del job además de su identificador, y el nombre de la aplicación Spark queda como `Vallegrande Spark · <job> · <id>`.
- La propiedad de versión de Commons CSV en el POM raíz se renombra de `common-csv.version` a `commons-csv.version`.

### Eliminado

- `WorkerProtocol.MAIN_CLASS` y `WorkerProtocol.GOLD_TABLES`: ahora cada `JobType` define su propia clase `main` y sus tablas Gold.
- Contratos sin uso `DatasetOrigin`, `DatasetRecord`, `JobRecord` y `JobStatus`.
- Módulo `spark-worker` y su clase `WorkerMain`.

### Corregido

- La propiedad de sistema para cambiar la carpeta del event log se leía como `worker.event-log.directoy`; ahora es `worker.event-log.directory`.
- Mensajes de error de `WorkerConfig` con texto mal formado.

### Problemas conocidos

- `BackendApiApplicationTests` está en el paquete `pe.edu.vallegrande.backend.api`, distinto al de `BigDataApplication` (`pe.edu.vallegrande.bigdata.api`), por lo que `./mvnw clean verify` falla en `backend-api`. Los módulos Spark compilan y pasan sus pruebas con `-pl`.
- `backend-api` todavía solo contiene la clase de arranque; la orquestación de workers está documentada como flujo previsto.

### Cambios incompatibles

- Quien lanzaba `pe.edu.vallegrande.bigdata.worker.WorkerMain` debe usar el JAR del job correspondiente (`JobType.jar()`) o su clase `main` (`JobType.mainClass()`).
- Quien usaba `WorkerProtocol.MAIN_CLASS` o `WorkerProtocol.GOLD_TABLES` debe migrar a `JobType`.

## [1.0.0] - 2026-09-25

Primera versión: estructura base del proyecto.

### Agregado

- Proyecto Maven multimódulo `bigdata-platform` con `contracts`, `spark-worker` y `backend-api`, y Maven Wrapper.
- Contratos académicos: `AcademicSchema`, `GradingPolicy`, `QualityRule`, `PipelineStage`, `StageStatus`, `StageEvent` y `WorkerProtocol`.
- Configuración inicial del worker Spark (`application.yaml`, `log4j2.properties`, `WorkerConfig`) y estructura de paquetes del pipeline.
- Aplicación Spring Boot WebFlux `backend-api` con su clase de arranque.
- Lotes de ejemplo en `data/limpio` y `data/con-errores`.

[2.0.0]: https://github.com/vallegrande/00_IsaelFatama-Bigdata-Spark/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/vallegrande/00_IsaelFatama-Bigdata-Spark/releases/tag/v1.0.0
