# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato se basa en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto sigue [Versionado Semántico](https://semver.org/lang/es/).

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
