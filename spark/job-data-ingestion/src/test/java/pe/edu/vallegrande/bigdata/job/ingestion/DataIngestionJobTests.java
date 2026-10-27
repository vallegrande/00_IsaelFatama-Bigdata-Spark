package pe.edu.vallegrande.bigdata.job.ingestion;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.SchemaInstaller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataIngestionJobTests {

    private static final Path CLEAN = Path.of("..", "..", "data", "limpio");

    private static SparkSession spark;

    @TempDir
    Path directory;

    private final DatabaseConfig h2 = new DatabaseConfig("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x", null);

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
    void isAnIngestionJobWithASingleStage() {
        DataIngestionJob job = new DataIngestionJob();

        assertEquals(JobType.DATA_INGESTION, job.type());
        assertEquals(List.of(PipelineStage.INGEST), job.steps().stream().map(PipelineStep::stage).toList());
    }

    @Test
    void loadsTheFiveCsvIntoTheStagingTables() throws Exception {
        createTables();

        PipelineResult result = new DataIngestionJob(() -> h2).pipeline().run(spark, CLEAN, directory.resolve("run"), 0);

        assertTrue(result.published());
        for (String table : AcademicSchema.TABLES) {
            assertEquals(csvRows(table), count("STG_" + table.toUpperCase()), table);
        }
        assertEquals(csvRows("notas") + csvRows("estudiantes") + csvRows("cursos") + csvRows("semestres") + csvRows("asistencia"),
                result.received());
    }

    @Test
    void ingestingTheSameDatasetAgainDoesNotDuplicateRows() throws Exception {
        createTables();
        DataIngestionJob job = new DataIngestionJob(() -> h2);

        job.pipeline().run(spark, CLEAN, directory.resolve("run-1"), 0);
        job.pipeline().run(spark, CLEAN, directory.resolve("run-2"), 0);

        assertEquals(csvRows("notas"), count("STG_NOTAS"));
    }

    @Test
    void failsWhenTheTablesWereNotCreated() {
        DataIngestionJob job = new DataIngestionJob(() -> h2);

        Exception error = assertThrows(Exception.class, () -> job.pipeline().run(spark, CLEAN, directory.resolve("run"), 0));

        assertTrue(String.valueOf(error.getMessage()).contains("SchemaInstaller"), error.getMessage());
    }

    @Test
    void failsClearlyWhenNoDatabaseIsConnected() {
        DataIngestionJob job = new DataIngestionJob(() -> {
            throw new IllegalStateException("Defina la variable de entorno DB_PASSWORD");
        });

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> job.pipeline().run(spark, CLEAN, directory.resolve("run"), 0));

        assertTrue(error.getMessage().contains("DB_PASSWORD"));
    }

    private void createTables() throws Exception {
        try (Connection connection = DriverManager.getConnection(h2.url(), h2.user(), h2.password())) {
            SchemaInstaller.install(connection);
        }
    }

    private long csvRows(String table) throws Exception {
        try (var lines = Files.lines(CLEAN.resolve(table + ".csv"))) {
            return lines.filter(line -> !line.isBlank()).count() - 1;
        }
    }

    private long count(String table) throws Exception {
        try (Connection connection = DriverManager.getConnection(h2.url(), h2.user(), h2.password());
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            result.next();
            return result.getLong(1);
        }
    }

}
