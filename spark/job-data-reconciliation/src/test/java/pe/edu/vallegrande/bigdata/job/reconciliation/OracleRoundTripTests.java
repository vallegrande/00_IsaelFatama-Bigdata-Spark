package pe.edu.vallegrande.bigdata.job.reconciliation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.job.load.DataLoadJob;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.SchemaInstaller;
import pe.edu.vallegrande.bigdata.spark.database.oracle.OracleDatabase;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleRoundTripTests {

    private static final Path RUN = Path.of("..", "..", "runtime", "jobs", "rendimiento-local-01");

    private static final ObjectMapper JSON = new ObjectMapper();

    private static SparkSession spark;

    @TempDir
    Path directory;

    private final DatabaseConfig config = new DatabaseConfig("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x", null);

    private final Database database = new OracleDatabase(config);

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

    @BeforeEach
    void createTables() throws Exception {
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            SchemaInstaller.install(connection);
        }
    }

    @Test
    void whatTheLoadJobWritesIsWhatTheReconciliationVerifies() throws Exception {
        PipelineResult load = load("carga-1");
        assertTrue(load.published());

        PipelineResult reconciliation = reconcile("conciliacion-1");

        assertTrue(reconciliation.published());
        JsonNode report = report("conciliacion-1");
        assertTrue(report.get("reconciled").asBoolean());
        report.get("checks").forEach(check -> assertEquals("OK", check.get("status").asText(), check.toString()));
    }

    @Test
    void loadingTheSameBatchTwiceDoesNotDuplicateAnything() throws Exception {
        load("carga-1");
        long coreBefore = count("CORE_NOTAS");
        long servingBefore = count("SERVING_RENDIMIENTO");

        load("carga-2");

        assertEquals(coreBefore, count("CORE_NOTAS"), "MERGE: las mismas claves se actualizan, no se duplican");
        assertEquals(servingBefore, count("SERVING_RENDIMIENTO"), "REPLACE_BATCH: el lote se reemplaza completo");
        assertTrue(reconcile("conciliacion-1").published());
    }

    @Test
    void theReconciliationDetectsRowsMissingInTheDatabaseAndALoadRepairsThem() throws Exception {
        load("carga-1");
        long before = count("CORE_NOTAS");
        execute("DELETE FROM CORE_NOTAS WHERE EVALUACION = 'EC1'");
        long missing = before - count("CORE_NOTAS");
        assertTrue(missing > 0);

        PipelineResult broken = reconcile("conciliacion-1");

        assertFalse(broken.published());
        JsonNode notes = check("conciliacion-1", "CORE", AcademicSchema.NOTAS);
        assertEquals("DIFERENCIA", notes.get("status").asText());
        assertEquals(missing, notes.get("difference").asLong());

        load("carga-2");
        assertTrue(reconcile("conciliacion-2").published(), "volver a cargar repara la base");
    }

    @Test
    void rejectedRowsAreStoredInTheControlLayerTaggedWithTheirBatch() throws Exception {
        load("carga-1");

        assertTrue(count("ETL_RECHAZADOS") > 0);
        assertEquals(count("ETL_RECHAZADOS"), Long.parseLong(query("SELECT COUNT(*) FROM ETL_RECHAZADOS WHERE BATCH_ID = 'rendimiento-local-01'")));
    }

    private PipelineResult load(String name) throws Exception {
        return new DataLoadJob(() -> database).pipeline().run(spark, RUN, directory.resolve(name), 0);
    }

    private PipelineResult reconcile(String name) throws Exception {
        return new DataReconciliationJob(() -> database).pipeline().run(spark, RUN, directory.resolve(name), 0);
    }

    private JsonNode report(String name) throws Exception {
        return JSON.readTree(directory.resolve(name).resolve("reconciliation.json").toFile());
    }

    private JsonNode check(String name, String layer, String table) throws Exception {
        for (JsonNode check : report(name).get("checks")) {
            if (check.get("layer").asText().equals(layer) && check.get("table").asText().equals(table)) {
                return check;
            }
        }
        throw new AssertionError("No hay chequeo para " + layer + "." + table);
    }

    private long count(String table) throws Exception {
        return Long.parseLong(query("SELECT COUNT(*) FROM " + table));
    }

    private String query(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

}
