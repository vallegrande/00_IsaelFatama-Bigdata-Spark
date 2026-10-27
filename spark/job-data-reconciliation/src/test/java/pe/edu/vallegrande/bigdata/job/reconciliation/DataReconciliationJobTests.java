package pe.edu.vallegrande.bigdata.job.reconciliation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;
import pe.edu.vallegrande.bigdata.spark.database.WriteMode;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;
import pe.edu.vallegrande.bigdata.spark.database.RunOutputs;

import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataReconciliationJobTests {

    private static final Path RUN = Path.of("..", "..", "runtime", "jobs", "rendimiento-local-01");

    private static final String BATCH = "rendimiento-local-01";

    private static final ObjectMapper JSON = new ObjectMapper();

    private static SparkSession spark;

    @TempDir
    Path directory;

    private Path lastOutput;

    private int runs;

    @BeforeAll
    static void startSpark() {
        spark = SparkSessionFactory.create(WorkerConfig.load().withoutUi());
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void isAReconciliationJobWithASingleStage() {
        DataReconciliationJob job = new DataReconciliationJob();

        assertEquals(JobType.DATA_RECONCILIATION, job.type());
        assertEquals(List.of(PipelineStage.RECONCILE), job.steps().stream().map(PipelineStep::stage).toList());
    }

    @Test
    void reconcilesWhenTheDatabaseHoldsExactlyWhatWasPublished() throws Exception {
        PipelineResult result = reconcile(faithfulDatabase(), 0);

        assertTrue(result.published());
        assertEquals(0, result.rejected());
        JsonNode report = report();
        assertTrue(report.get("reconciled").asBoolean());
        report.get("checks").forEach(check -> assertEquals("OK", check.get("status").asText()));
        assertEquals(AcademicSchema.TABLES.size() + JobType.ACADEMIC_PERFORMANCE.goldTables().size(), report.get("checks").size());
    }

    @Test
    void blocksWhenRowsAreMissingInCore() throws Exception {
        PipelineResult result = reconcile(database((layer, table) -> {
            Dataset<Row> data = faithful(layer, table);
            return layer == DatabaseLayer.CORE && table.equals(AcademicSchema.NOTAS) ? data.limit(10) : data;
        }), 0);

        assertFalse(result.published());
        assertTrue(result.rejected() > 0);
        JsonNode notas = check(DatabaseLayer.CORE, AcademicSchema.NOTAS);
        assertEquals("DIFERENCIA", notas.get("status").asText());
        assertEquals(10, notas.get("found").asLong());
        assertFalse(report().get("reconciled").asBoolean());
    }

    @Test
    void blocksWhenServingHasDuplicatedRows() throws Exception {
        PipelineResult result = reconcile(database((layer, table) -> {
            Dataset<Row> data = faithful(layer, table);
            return layer == DatabaseLayer.SERVING ? data.union(data) : data;
        }), 0);

        assertFalse(result.published());
        assertEquals("DIFERENCIA", check(DatabaseLayer.SERVING, "rendimiento").get("status").asText());
    }

    @Test
    void ignoresRowsOfOtherBatchesInServing() throws Exception {
        PipelineResult result = reconcile(database((layer, table) -> {
            Dataset<Row> data = faithful(layer, table);
            if (layer != DatabaseLayer.SERVING) {
                return data;
            }
            Dataset<Row> otherBatch = data.withColumn(DatabaseNaming.physicalColumn(DatabaseLayer.BATCH_COLUMN), lit("otro-lote"));
            return data.union(otherBatch);
        }), 0);

        assertTrue(result.published());
    }

    @Test
    void acceptsSmallDifferencesWhenTheToleranceAllowsIt() throws Exception {
        Database missingOneNote = database((layer, table) -> {
            Dataset<Row> data = faithful(layer, table);
            return layer == DatabaseLayer.CORE && table.equals(AcademicSchema.NOTAS) ? data.limit((int) data.count() - 1) : data;
        });

        assertFalse(reconcile(missingOneNote, 0).published());
        assertTrue(reconcile(missingOneNote, 0.01).published());
    }

    private Database faithfulDatabase() {
        return database(this::faithful);
    }

    private Dataset<Row> faithful(DatabaseLayer layer, String table) {
        Dataset<Row> data = layer == DatabaseLayer.CORE
                ? RunOutputs.read(spark, RUN, "silver", table)
                : RunOutputs.read(spark, RUN, "gold", table).withColumn(DatabaseLayer.BATCH_COLUMN, lit(BATCH));
        return DatabaseNaming.toDatabase(data);
    }

    private Database database(BiFunction<DatabaseLayer, String, Dataset<Row>> tables) {
        return new Database() {
            @Override
            public Dataset<Row> read(SparkSession session, TableRef table) {
                return tables.apply(table.layer(), table.name());
            }

            @Override
            public long write(Dataset<Row> data, TableRef table, WriteMode mode, String batchId) {
                throw new UnsupportedOperationException("La conciliación no escribe");
            }
        };
    }

    private PipelineResult reconcile(Database database, double tolerance) throws Exception {
        lastOutput = directory.resolve("run-" + (++runs));
        return new DataReconciliationJob(() -> database).pipeline().run(spark, RUN, lastOutput, tolerance);
    }

    private JsonNode report() throws Exception {
        return JSON.readTree(lastOutput.resolve("reconciliation.json").toFile());
    }

    private JsonNode check(DatabaseLayer layer, String table) throws Exception {
        for (JsonNode check : report().get("checks")) {
            if (check.get("layer").asText().equals(layer.name()) && check.get("table").asText().equals(table)) {
                return check;
            }
        }
        throw new AssertionError("No hay chequeo para " + layer + "." + table);
    }

}
