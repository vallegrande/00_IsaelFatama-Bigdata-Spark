# Guía de clase: por dónde empezar, archivo por archivo

Cómo recorrer este proyecto en clase, en orden, sin perderse. Cada sesión indica **la carpeta**, **los archivos**, **los métodos que se explican**, **un ejercicio** y **cómo comprobar** que quedó funcionando.

- [Idea general](#idea-general)
- [Ruta resumida](#ruta-resumida)
- [Antes de la primera clase](#antes-de-la-primera-clase)
- [Sesión 1. El contrato de datos](#sesión-1-el-contrato-de-datos)
- [Sesión 2. Configuración y nombres en la base de datos](#sesión-2-configuración-y-nombres-en-la-base-de-datos)
- [Sesión 3. Leer de la base de datos](#sesión-3-leer-de-la-base-de-datos)
- [Sesión 4. Insertar y actualizar](#sesión-4-insertar-y-actualizar)
- [Sesión 5. Cargar los resultados](#sesión-5-cargar-los-resultados)
- [Sesión 6. Conciliar](#sesión-6-conciliar)
- [Sesión 7. El API: elegir y ejecutar](#sesión-7-el-api-elegir-y-ejecutar)
- [Sesión 8. El API: consultar para la interfaz](#sesión-8-el-api-consultar-para-la-interfaz)
- [Sesión 9. Integración y portar a tu proyecto](#sesión-9-integración-y-portar-a-tu-proyecto)
- [Qué se puede dejar para el final](#qué-se-puede-dejar-para-el-final)

---

## Idea general

El proyecto mueve datos en un solo sentido, y la clase lo sigue en ese mismo orden:

```
CSV ──▶ [API] ──▶ Spark (transformación: ya conocida) ──▶ Parquet
                                                            │
        Oracle (STG) ──▶ EXTRAER ──▶ archivos (landing/)    │
                                                            ▼
                              CARGAR (insertar / actualizar) ──▶ Oracle (CORE, SERVING)
                                                            │
                                        CONCILIAR ◀─────────┘
                                                            │
                                  [API] ──▶ JSON ──▶ interfaz web
```

Hay tres tipos de trabajo nuevos y se enseñan en este orden: **leer** de la base (extracción), **insertar y actualizar** en la base (carga) y **verificar** que quedó bien (conciliación). El transformador (medallion: Bronze, Silver, Gold) ya se vio en clases anteriores y aquí solo se usa.

**Regla de la clase:** cada sesión termina con una prueba que pasa. Si la prueba no pasa, no se avanza.

---

## Ruta resumida

| # | Sesión | Carpeta | Archivos principales | Lo que se aprende |
|---|--------|---------|----------------------|-------------------|
| 1 | El contrato de datos | `contracts/` | `AcademicSchema`, `DatabaseLayer`, `JobType` | Qué tablas hay, cuáles son maestras y cuáles transaccionales, qué capas tiene la base |
| 2 | Configuración y nombres | `spark/spark-database/` | `DatabaseConfig`, `DatabaseNaming`, `TableRef`, `WriteMode`, `Database` | Cómo se conecta y cómo se nombran las tablas |
| 3 | Leer | `spark/spark-database/` y `spark/job-data-extraction/` | `OracleDatabase.read`, `StagingSeeder`, `ExtractStep` | Lectura JDBC y primer job |
| 4 | Insertar y actualizar | `spark/spark-database/` | `SqlStatements`, `OracleDatabase.write` | `MERGE`, reemplazo de lote, transacción |
| 5 | Cargar | `spark/job-data-load/` | `RunOutputs`, `LoadCoreStep`, `LoadServingStep` | Maestras primero, idempotencia |
| 6 | Conciliar | `spark/job-data-reconciliation/` | `ReconcileStep` | Verificar sin confiar |
| 7 | API: elegir y ejecutar | `backend-api/` | `ExecutionRest`, `ExecutionServiceImpl`, `ExecutionRunner` | Elegir job y dataset, lanzar Spark, estados |
| 8 | API: consultar | `backend-api/` | `DashboardRest`, `ExecutionResultServiceImpl` | JSON para la interfaz |
| 9 | Integración y portar | todo | `README.md`, `docs/API-FRONTEND.md` | El flujo completo y cómo llevarlo a otro proyecto |

Tamaño de cada carpeta, para dimensionar el trabajo:

| Carpeta | Archivos principales (`src/main`) | Pruebas |
|---------|-----------------------------------|---------|
| `contracts` | 11 | 0 |
| `spark/spark-database` | 9 | 6 |
| `spark/job-data-extraction` | 2 | 1 |
| `spark/job-data-load` | 3 | 1 |
| `spark/job-data-reconciliation` | 2 | 2 |
| `backend-api` | 57 | 12 |

Las carpetas `spark/spark-core`, `spark/spark-academic` y los dos jobs de transformación son el medallion que ya se conoce: no se vuelven a explicar.

---

## Antes de la primera clase

1. **Requisitos:** JDK 17 o superior y Docker (para Oracle local).
2. **Compilar todo** (genera los JAR que el API lanza como procesos):

   ```bash
   ./mvnw package -DskipTests
   ```

3. **Oracle local** (valores en `.env.example`, detalle en el [README, sección 11.6](../README.md#116-conexión-a-oracle)):

   ```powershell
   Copy-Item .env.example .env
   docker compose up -d
   docker compose ps          # esperar "healthy"
   . .\script.ps1             # carga .env en la consola
   ```

4. **Comprobar el entorno** con una prueba que no necesita Oracle:

   ```bash
   ./mvnw -pl spark/spark-database -am test -Dtest='DatabaseNamingTest' -Dsurefire.failIfNoSpecifiedTests=false
   ```

> Las pruebas de las sesiones 3 a 6 usan **H2 en modo Oracle** (una base en memoria), así que corren aunque no haya contenedor. Solo los pasos marcados "con Oracle real" lo necesitan.

---

## Sesión 1. El contrato de datos

**Carpeta:** `contracts/src/main/java/pe/edu/vallegrande/bigdata/contracts/`  
**Objetivo:** que todos sepan qué datos se mueven y cómo se llama cada cosa, antes de ver una sola línea de Spark.

| Archivo | Qué mirar | Idea clave |
|---------|-----------|------------|
| `AcademicSchema` | `TABLES`, `COLUMNS`, `KEYS`, `columns(tabla)` | Cinco tablas. `KEYS` define la clave natural de cada una |
| `DatabaseLayer` | `STAGING`, `CORE`, `SERVING`, `CONTROL`, `prefix()`, `BATCH_COLUMN` | Las cuatro capas de la base y su prefijo de tabla (`STG_`, `CORE_`, `SERVING_`, `ETL_`) |
| `JobKind`, `JobInput`, `JobType` | `JobType.kind()`, `goldTables()`, `module()` | Cada job declara qué familia es, qué entrada necesita y qué tablas produce |
| `WorkerProtocol` | `EXIT_SUCCEEDED`, `EXIT_FAILED`, `EXIT_QUALITY_FAILED`, `LANDING`, `RECONCILIATION_FILE` | El acuerdo entre el API y Spark: códigos de salida y nombres de archivos |
| `PipelineStage` | los 11 valores | Las etapas de cada familia |

**Qué mostrar:**

- Abrir `data/limpio/` y `data/con-errores/`: son los mismos datos, el segundo con errores inyectados.
- En `AcademicSchema.KEYS`, preguntar: ¿qué tablas son **maestras** (pocas filas, otras las referencian: `semestres`, `cursos`, `estudiantes`) y cuáles **transaccionales** (crecen cada semana: `notas`, `asistencia`)? La respuesta está en la clave: la de una maestra es un solo código; la de una transaccional es compuesta.
- Por qué `contracts` no tiene dependencias: lo comparten Spark y el API.

**Ejercicio:** clasificar las 5 tablas y justificar con `KEYS`. Después, decidir en qué capa vive cada una (¿`notas` en `STAGING`, `CORE` o `SERVING`?).

**Comprobar:** `./mvnw -pl contracts compile` compila sin errores.

---

## Sesión 2. Configuración y nombres en la base de datos

**Carpeta:** `spark/spark-database/src/main/java/pe/edu/vallegrande/bigdata/spark/database/`  
**Objetivo:** entender los cuatro tipos pequeños que usa todo lo demás.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `DatabaseConfig` | `fromEnvironment()`, `from(...)`, `isConfigured()`, `toString()` | La conexión sale de variables de entorno (`DB_URL`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`). Solo `DB_PASSWORD` es obligatoria. La contraseña nunca se imprime |
| `DatabaseNaming` | `physicalTable()`, `physicalColumn()`, `identifier()`, `toDatabase()`, `toLake()` | El lago usa `estudiante_id`; Oracle usa `ESTUDIANTE_ID`. La traducción vive en un solo lugar |
| `TableRef` | `of(capa, nombre)`, `of(capa, nombre, claves)` | Una tabla es una capa, un nombre y su clave |
| `WriteMode` | `MERGE`, `REPLACE_BATCH` | Las dos formas de escribir |
| `Database` | `read()`, `write()` | La interfaz que usan los jobs. Cambiar de motor es escribir otra implementación |

**Qué mostrar:**

- Por qué las mayúsculas: Spark entrecomilla los nombres al escribir por JDBC; si la columna se crea en minúscula, Oracle la vuelve sensible a mayúsculas y `SELECT nota` deja de funcionar.
- `identifier()` rechaza nombres como `nota final` o `a; DROP TABLE x`: es la defensa contra inyección en los nombres.

**Ejercicio:** sin ejecutar, predecir el nombre físico de `TableRef.of(SERVING, "cursos_resumen")` y de la columna `batch_id`. Después, comprobarlo en `DatabaseNamingTest`.

**Comprobar:**

```bash
./mvnw -pl spark/spark-database -am test -Dtest='DatabaseConfigTest,DatabaseNamingTest' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 3. Leer de la base de datos

**Carpetas:** `spark/spark-database/.../oracle/` y `.../seed/`, y `spark/job-data-extraction/`  
**Objetivo:** el primer job. Se lee de Oracle y se deja como archivos.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `seed/StagingSeeder` | `main()`, `seed()`, `load()`, `requireTable()`, `requireHeader()` | Carga los CSV en las tablas `STG_*` como texto crudo. Prepara la base para la práctica |
| `oracle/OracleDatabase` | `fromEnvironment()`, `read()`, `sparkOptions()` | Lectura JDBC: `format("jdbc")`, `dbtable`, `fetchsize` de 10 000 |
| `job-data-extraction/ExtractStep` | `execute()`, `contractColumns()` | Lee las 5 tablas de `STAGING`, deja solo las columnas del contrato y publica en `landing/` |
| `job-data-extraction/DataExtractionJob` | `main()`, `type()`, `steps()` | Un job es una lista de etapas |

**Qué mostrar:**

- `fetchsize`: el valor por defecto de Oracle es 10 filas por viaje; con 10 000 la lectura es órdenes de magnitud más rápida.
- `landing/` tiene el mismo formato que los CSV de entrada: lo extraído se puede transformar directamente.
- `ExtractStep.contractColumns()` falla con un mensaje claro si a la base le falta una columna del contrato.

**Con Oracle real:**

```powershell
java -cp "spark/job-data-extraction/target/lib/*" pe.edu.vallegrande.bigdata.spark.database.seed.StagingSeeder data/limpio
```

Después, ejecutar el job de extracción como se explica en el README, sección 12.3 (o desde el API, sesión 9).

**Ejercicio:** agregar una columna inexistente al contrato (en una copia del proyecto) y ver el error de `contractColumns()`.

**Comprobar:**

```bash
./mvnw -pl spark/job-data-extraction -am test -Dtest='StagingSeederTest,DataExtractionJobTests' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 4. Insertar y actualizar

**Carpeta:** `spark/spark-database/src/main/java/.../spark/database/oracle/`  
**Objetivo:** el corazón del trabajo: escribir en la base sin duplicar y sin dejar nada a medias.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `resources/db/oracle/*.sql` | | Los scripts que **crean las tablas**: `STG_`, `CORE_`, `SERVING_`, `ETL_` y sus gemelas `_LOAD`. Spark nunca crea tablas |
| `SchemaInstaller` | `install()`, `statements()` | Ejecuta los scripts; se puede repetir sin error |
| `oracle/SqlStatements` | `mergeBatch()`, `deleteBatch()`, `insertBatch()` | El SQL como **constantes** con `?` para el lote. Es puro: se prueba sin base de datos |
| `OracleDatabase` | `write()` | El flujo completo, en el orden abajo |
| `OracleDatabase` | `require()`, `requireTable()` | Valida el modo (claves) y que las tablas existan |
| `OracleDatabase` | `copyToLoad()` | Spark copia las filas a la tabla `_LOAD`, en paralelo |
| `OracleDatabase` | `apply()` | **Una sola transacción**: `MERGE` o borrar-e-insertar el lote; `commit` o `rollback` |

**El flujo de `write()`:**

```
1. validar          require(), requireTable()
2. limpiar el lote  deleteBatch()     → borra de la tabla _LOAD el lote que se va a enviar
3. copiar           copyToLoad()      → Spark escribe en paralelo en la tabla _LOAD (permanente)
4. aplicar          apply()           → MERGE o REPLACE_BATCH desde _LOAD, dentro de una transacción
```

**Las dos estrategias:**

| | `MERGE` | `REPLACE_BATCH` |
|-|---------|-----------------|
| Para | Maestras y transaccionales limpias (`CORE`) | Resultados del dashboard (`SERVING`) |
| Hace | Inserta lo nuevo y **actualiza** lo que ya existe, por la clave | Borra el lote y lo vuelve a insertar |
| Repetir la carga | Mismas filas, sin duplicar | Mismo lote, sin duplicar |
| Necesita | La clave natural | La columna `batch_id` |

**Qué mostrar:**

- Por qué una tabla `_LOAD` y no escribir directo: Spark no sabe hacer `MERGE`; deja el lote en `_LOAD` y el cambio real es una sola sentencia dentro de una transacción. Si falla a la mitad, `rollback` y el destino queda como estaba. Y como `_LOAD` es permanente, se puede consultar qué llegó en cada lote.
- Ejecutar el mismo `write()` dos veces y contar las filas: no cambian. Eso es **idempotencia**.
- Abrir `02_core.sql`: los tipos y las claves están escritos ahí, como en un proyecto de banco; un cambio de esquema es un cambio de script.

**Ejercicio:** en `OracleDatabaseTest`, dado un `MERGE` con una fila existente cambiada y una nueva, predecir el contenido final de la tabla antes de ejecutarlo. Luego romper algo a propósito (por ejemplo, pasar una clave que no está en los datos) y leer el error de `require()`.

**Comprobar:**

```bash
./mvnw -pl spark/spark-database -am test -Dtest='SqlStatementsTest,SchemaInstallerTest,OracleDatabaseTest' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 5. Cargar los resultados

**Carpeta:** `spark/job-data-load/src/main/java/pe/edu/vallegrande/bigdata/job/load/`  
**Objetivo:** tomar lo que produjo una transformación y llevarlo a `CORE` y `SERVING`.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `spark-database/RunOutputs` | `batchId()`, `exists()`, `read()`, `tables()` | La carpeta de una ejecución es el **contrato en Parquet** entre jobs. Su nombre es el identificador del lote |
| `LoadCoreStep` | `execute()`, `loadRejected()` | `silver` a `CORE` con `MERGE`, **maestras primero** (el orden de `AcademicSchema.TABLES`). La cuarentena va a `CONTROL` |
| `LoadServingStep` | `execute()` | `gold` a `SERVING` con `REPLACE_BATCH`, agregando `batch_id` |
| `DataLoadJob` | `steps()` | Dos etapas: `LOAD_CORE` y `LOAD_SERVING` |

**Qué mostrar:**

- Por qué las maestras primero: las transaccionales hacen referencia a ellas.
- `execute()` empieza comprobando `RunOutputs.tables(run, GOLD)`: si el lote no publicó Gold, **no carga nada** y termina con código 2. Un lote bloqueado nunca llega a la base.
- Las tablas de `gold/` se descubren en la carpeta: el mismo job sirve para cualquier transformación.

**Ejercicio:** correr la carga dos veces sobre el mismo lote y comprobar que el número de filas no cambia. Luego apuntar el job a una carpeta vacía y ver que no escribe nada.

**Comprobar:**

```bash
./mvnw -pl spark/job-data-load -am test -Dtest='DataLoadJobTests' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 6. Conciliar

**Carpeta:** `spark/job-data-reconciliation/src/main/java/pe/edu/vallegrande/bigdata/job/reconciliation/`  
**Objetivo:** no confiar en que la carga salió bien: comprobarlo.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `ReconcileStep` | `execute()` | Compara lo publicado con lo que hay en la base y escribe `reconciliation.json` |
| `ReconcileStep` | `reconcileCore()`, `keyEquality()` | Cada fila de `silver` debe existir en `CORE` (un `left_semi` join por la clave). La base puede tener más filas de otros lotes |
| `ReconcileStep` | `reconcileServing()` | Cada tabla `gold` debe tener **exactamente** las mismas filas para ese `batch_id`: detecta faltantes **y** duplicados |
| `ReconcileStep` | `check()` | Un resultado por tabla: `expected`, `found`, `difference`, `status` |
| `DataReconciliationJob` | `steps()` | Una etapa: `RECONCILE` |

**Qué mostrar:**

- La tolerancia es 0 por defecto: cualquier diferencia bloquea (código de salida 2).
- En `OracleRoundTripTests` se hace todo el ciclo: carga, conciliación correcta, **borrar filas a mano**, conciliación que detecta la diferencia, carga de nuevo que la repara.

**Ejercicio:** borrar filas de `CORE_NOTAS` con una sentencia `DELETE` y ver qué dice `reconciliation.json`.

**Comprobar:**

```bash
./mvnw -pl spark/job-data-reconciliation -am test -Dtest='DataReconciliationJobTests,OracleRoundTripTests' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 7. El API: elegir y ejecutar

**Carpeta:** `backend-api/src/main/java/pe/edu/vallegrande/bigdata/api/`  
**Objetivo:** decidir qué job se ejecuta, lanzar Spark y seguir el estado. El API es el **orquestador**: no procesa datos, no recibe archivos y no escribe en la base; lanza procesos y sirve su salida.

Se recorre de afuera hacia adentro, siguiendo una petición `POST /api/executions`:

| Orden | Archivo | Métodos | Idea clave |
|-------|---------|---------|------------|
| 1 | `rest/ExecutionRest` | `create()` | Solo traduce HTTP. Responde `202 Accepted` con el `Location` |
| 2 | `service/impl/DatasetServiceImpl` | `resolve()`, `findAll()` | Convierte el nombre de un dataset en su carpeta; `400` si faltan archivos y `404` si no existe |
| 3 | `service/impl/ExecutionServiceImpl` | `create()` | Las reglas. Ver los tres de abajo |
| 3a | | `validateShape()` | Qué necesita cada familia de job según `JobKind.input()`, sin un `if` por job |
| 3c | | `resolveInput()` | De dónde toma sus datos: carpeta del dataset, `landing/` de una extracción o la salida de una transformación |
| 3d | | `register()` | Guarda la ejecución como `PENDING` y la pone en la cola |
| 4 | `component/ExecutionWorkspace` | `outputDirectory()`, `datasetDirectory()`, `isValidId()` | Rutas seguras: valida el identificador y el nombre del dataset antes de convertirlos en ruta |
| 5 | `component/ExecutionRunner` | `enqueue()`, `run()`, `finish()` | La cola: máximo `max-concurrent` a la vez. `PENDING` → `RUNNING` → estado final |
| 6 | `worker/impl/ProcessWorkerLauncher` | `commandLine()`, `run()` | Lanza `java -jar <modulo>.jar <input> <output> <tolerancia>` como **proceso aparte** |

**Qué mostrar:**

- Por qué un proceso aparte: aísla la memoria y las dependencias de Spark. Un fallo del job nunca tumba el API.
- Por qué `Mono` y `Flux`: el API no se bloquea esperando. El trabajo bloqueante (disco, proceso) se mueve a `boundedElastic`.
- Estados y códigos de salida: `0` es `PUBLISHED`, `2` es `BLOCKED` (la calidad no alcanzó) y cualquier otro es `FAILED`. Si falla, `message` trae el detalle de la etapa que falló, tomado de `events.jsonl`.
- El API no valida el contenido: una nota de 25 llega a cuarentena en Spark. Tampoco toca la base: la ingesta la hacen `StagingSeeder` y los jobs de extracción y carga.

**En vivo:**

```bash
java -jar backend-api/target/backend-api-1.0.0.jar
curl http://localhost:8080/api/datasets
curl -i -X POST http://localhost:8080/api/executions \
  -H 'Content-Type: application/json' \
  -d '{"jobType":"ACADEMIC_PERFORMANCE","dataset":"con-errores"}'
```

**Ejercicio:** pedir un dataset que no existe y leer el `404`. Lanzar `con-errores` con `"threshold":0.01` y ver el estado `BLOCKED`.

**Comprobar:**

```bash
./mvnw -pl backend-api -am test -Dtest='ExecutionServiceImplTest,ExecutionRunnerTest,DatasetServiceImplTest,ExecutionWorkspaceTest' -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Sesión 8. El API: consultar para la interfaz

**Carpeta:** `backend-api/src/main/java/pe/edu/vallegrande/bigdata/api/`  
**Objetivo:** entender qué JSON recibe la interfaz y de dónde sale.

| Archivo | Métodos | Idea clave |
|---------|---------|------------|
| `rest/ExecutionRest` | `findById()`, `findEvents()`, `findQuality()`, `findRejected()`, `findGoldTable()` | Los GET de una ejecución |
| `rest/DashboardRest` | `findTables()`, `findSummary()`, `findTable()` | Siempre el **último lote cargado en la base**: la interfaz no maneja identificadores |
| `service/impl/ExecutionResultServiceImpl` | `findDetail()`, `requirePublished()` | Una ejecución que no se publicó no tiene datos Gold |
| `service/impl/DashboardServiceImpl` | `current()`, `dashboardJob()` | Pide el lote vigente y rechaza jobs sin dashboard |
| `constant/SqlQueries` | | Las consultas `SELECT` del dashboard como constantes. El backend **solo lee** |
| `repository/impl/JdbcServingRepository` | `findCurrent()`, `findPage()` | Lee `SERVING_*` con JDBC en `boundedElastic`. Valida el nombre de la tabla contra la lista de `JobType` y convierte los tipos de Oracle a JSON |
| `repository/impl/FileGoldTableRepository` | `findPage()` | Solo para `/api/executions/{id}/gold/...`: lee los CSV de una ejecución concreta |
| `dto/response/*` | | Lo que sale: `ExecutionResponse`, `PageResponse`, `DashboardResponse`… todos `record` |
| `exception/GlobalExceptionHandler` | | El único lugar donde una excepción se vuelve código HTTP (formato `ProblemDetail`) |

**Qué mostrar:**

- Cómo el lote vigente sale de `SERVING_RESUMEN`: el último `LOADED_AT` de ese `JOB_TYPE`. Por eso el job de carga carga el resumen al final.
- Los tipos: `NUMBER(1)` vuelve como booleano, `NUMBER(p,0)` como entero y `BINARY_DOUBLE` como decimal.
- Qué pasa con un lote `BLOCKED`: nunca llega a la base, así que el dashboard no lo ve. Y si aún no se cargó nada, responde `404` diciendo que ejecute `DATA_LOAD`.
- El contrato completo para quien hace la interfaz está en [API-FRONTEND.md](API-FRONTEND.md). Es el documento que se le entrega.

**Ejercicio:** pedir `/api/dashboard/academic-performance/tables/cursos_resumen?size=2` y comparar cada campo con `docs/API-FRONTEND.md`.

**Comprobar:**

```bash
./mvnw -pl backend-api -am test -Dtest='JdbcServingRepositoryTest,DashboardAndResultServiceImplTest,ApiFlowIntegrationTest' -Dsurefire.failIfNoSpecifiedTests=false
```

`ApiFlowIntegrationTest` recorre la API completa por HTTP (lanzar, esperar, consultar, errores, CORS); es la prueba que más confianza da.

---

## Sesión 9. Integración y portar a tu proyecto

**Objetivo:** ver el flujo completo y llevarlo al proyecto de cada equipo.

**Flujo completo** (con Oracle real, comandos en el [README, sección 12.6](../README.md#126-ejecutar-el-backend)):

1. Crear las tablas con `SchemaInstaller`.
2. `POST /api/executions` con `jobType=DATA_INGESTION` y `dataset=limpio`: carga los CSV en `STG_*`.
2b. `POST` con `jobType=DATA_EXTRACTION`: lee la base y deja `landing/`.
3. `POST` con `jobType=ACADEMIC_PERFORMANCE` y `sourceExecutionId=<id de la extracción>`: transforma lo extraído.
4. `POST` con `jobType=DATA_LOAD` y `sourceExecutionId=<id de la transformación>`: carga a `CORE` y `SERVING`.
5. `POST` con `jobType=DATA_RECONCILIATION` y el mismo `sourceExecutionId`: verifica.
6. `GET /api/dashboard/academic-performance/summary`: lo que verá la interfaz, leído de `SERVING_*`.

**Qué copiar a otro proyecto.** Estas piezas no dependen del dominio y se llevan tal cual (cambiando el paquete):

| Pieza | Carpeta |
|-------|---------|
| Interfaz y Oracle | `spark/spark-database/` (todo menos `StagingSeeder` y los scripts `db/oracle/`, que se reescriben para su esquema) |
| Familias de job | `contracts/`: `JobKind`, `JobInput`, `DatabaseLayer` y los cambios en `JobType`, `PipelineStage`, `WorkerProtocol` |
| Cambio en el motor | `spark-core`: `PipelineContext.counts()` y `PipelineResult.of()` |
| Los 3 jobs | `spark/job-data-*` (cambiando las referencias de abajo) |
| El API | `backend-api/` (cambiando las referencias de abajo) |

**Qué hay que adaptar.** Estos archivos usan `AcademicSchema` (las tablas académicas) y deben apuntar al esquema del proyecto de cada equipo:

| Archivo | Para qué usa el esquema |
|---------|-------------------------|
| `job-data-extraction/ExtractStep` | Tablas y columnas a leer |
| `job-data-load/LoadCoreStep` | Tablas, orden (maestras primero) y claves |
| `job-data-reconciliation/ReconcileStep` | Tablas y claves a comparar |
| `spark-database/seed/StagingSeeder` | Tablas y encabezados |
| `spark-database/resources/db/oracle/*.sql` | Las tablas de cada capa y sus gemelas `_LOAD` |
| `spark-database/DatabaseTables` | Qué tablas gold se publican |
| `service/impl/DatasetServiceImpl` | Las tablas que debe traer un dataset (usa `AcademicSchema.TABLES`) |

Son **7 archivos** por proyecto (los 4 scripts SQL cuentan como uno). Todo lo demás es independiente del dominio.

**Checklist por equipo:**

- [ ] Su esquema declara tablas, columnas y claves (`KEYS`), igual que `AcademicSchema`.
- [ ] Clasificaron sus tablas en maestras y transaccionales, y el orden de carga las pone en ese orden.
- [ ] Sus jobs de transformación están registrados en `JobType` con sus tablas Gold.
- [ ] La extracción, la carga y la conciliación pasan sus pruebas con H2.
- [ ] `ApiFlowIntegrationTest` adaptado pasa.
- [ ] Probaron el flujo completo con Oracle real y la conciliación cuadra.

---

## Qué se puede dejar para el final

No hace falta para entender insertar, leer y actualizar; se ve al final o no se ve:

| Pieza | Para qué sirve |
|-------|----------------|
| `component/ExecutionRecovery` | Marca como fallidas las ejecuciones que quedaron colgadas si el servicio se reinició |
| `mapper/*`, `component/ExecutionFinder` | Orden interno (conversión a DTO y búsqueda con error 404) |
| `repository/impl/Database*Repository` | Marcadores para leer el dashboard desde Oracle en lugar de archivos (`bigdata.storage.mode=database`) |
| `config/CorsConfig` | Permite que la interfaz web llame al API desde otro puerto |
| `constant/*` | Constantes de rutas y del modo de almacenamiento |
