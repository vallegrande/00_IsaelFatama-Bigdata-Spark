package pe.edu.vallegrande.bigdata.job.extraction;

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
import pe.edu.vallegrande.bigdata.spark.academic.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.SchemaInstaller;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;
import pe.edu.vallegrande.bigdata.spark.database.WriteMode;
import pe.edu.vallegrande.bigdata.spark.database.oracle.OracleDatabase;
import pe.edu.vallegrande.bigdata.spark.database.seed.StagingSeeder;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataExtractionJobTests {

    private static final Path CLEAN = Path.of("..", "..", "data", "limpio");

    private static SparkSession spark;

    @TempDir
    Path directory;

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
    void isAnExtractionJobWithASingleStage() {
        DataExtractionJob job = new DataExtractionJob();

        assertEquals(JobType.DATA_EXTRACTION, job.type());
        assertEquals(List.of(PipelineStage.EXTRACT), job.steps().stream().map(PipelineStep::stage).toList());
    }

    @Test
    void publishesTheFiveTablesAsFilesTheTransformationCanRead() throws Exception {
        DataExtractionJob job = new DataExtractionJob(() -> databaseFromCsv(null));
        Path output = directory.resolve("run");

        PipelineResult result = job.pipeline().run(spark, Files.createDirectories(directory.resolve("input")), output, 0);

        assertTrue(result.published());
        assertEquals(expectedRows(), result.received());
        assertEquals(0, result.rejected());
        for (String table : AcademicSchema.TABLES) {
            assertTrue(Files.isRegularFile(output.resolve("landing").resolve(table + ".csv")), table + ".csv");
            assertTrue(Files.isRegularFile(output.resolve("landing").resolve(table + ".parquet")), table + ".parquet");
        }
        assertTrue(Files.readString(output.resolve("events.jsonl")).contains("\"stage\":\"EXTRACT\",\"status\":\"COMPLETED\""));
    }

    @Test
    void theLandingFolderIsValidInputForTheTransformationExtractor() throws Exception {
        Path output = directory.resolve("run");
        new DataExtractionJob(() -> databaseFromCsv(null)).pipeline().run(spark, Files.createDirectories(directory.resolve("input")), output, 0);

        BronzeTables bronze = new CsvExtractor().read(spark, output.resolve("landing"));

        assertEquals(expectedRows(), bronze.totalRows(), "la transformación lee exactamente lo que extrajo el job");
        assertEquals(AcademicSchema.TABLES, List.copyOf(bronze.tables().keySet()));
    }

    @Test
    void readsTheStagingTablesThroughTheOracleAdapterAfterSeeding() throws Exception {
        DatabaseConfig h2 = new DatabaseConfig("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x", null);
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(h2.url(), h2.user(), h2.password())) {
            SchemaInstaller.install(connection);
        }
        StagingSeeder.seed(h2, CLEAN);
        Path output = directory.resolve("run");

        PipelineResult result = new DataExtractionJob(() -> new OracleDatabase(h2)).pipeline()
                .run(spark, Files.createDirectories(directory.resolve("input")), output, 0);

        assertTrue(result.published());
        assertEquals(expectedRows(), result.received(), "se extrajo todo lo que se sembró");
        assertEquals(expectedRows(), new CsvExtractor().read(spark, output.resolve("landing")).totalRows(),
                "y lo extraído es entrada válida de la transformación");
    }

    @Test
    void failsWhenTheDatabaseTableLacksAContractColumn() throws Exception {
        DataExtractionJob job = new DataExtractionJob(() -> databaseFromCsv(AcademicSchema.NOTAS));
        Path input = Files.createDirectories(directory.resolve("input"));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> job.pipeline().run(spark, input, directory.resolve("run"), 0));

        assertTrue(error.getMessage().contains("notas"));
    }

    @Test
    void failsClearlyWhenNoDatabaseIsConnected() throws Exception {
        DataExtractionJob job = new DataExtractionJob(() -> {
            throw new IllegalStateException("Defina la variable de entorno DB_PASSWORD");
        });
        Path input = Files.createDirectories(directory.resolve("input"));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> job.pipeline().run(spark, input, directory.resolve("run"), 0));

        assertTrue(error.getMessage().contains("DB_PASSWORD"));
    }

    private Database databaseFromCsv(String brokenTable) {
        return new Database() {
            @Override
            public Dataset<Row> read(SparkSession session, TableRef table) {
                Dataset<Row> csv = session.read().option("header", "true").option("encoding", "UTF-8")
                        .csv(CLEAN.resolve(table.name() + ".csv").toString());
                if (table.name().equals(brokenTable)) {
                    csv = csv.drop(AcademicSchema.columns(brokenTable).get(0));
                }
                return DatabaseNaming.toDatabase(csv);
            }

            @Override
            public long write(Dataset<Row> data, TableRef table, WriteMode mode, String batchId) {
                throw new UnsupportedOperationException("La extracción no escribe");
            }
        };
    }

    private long expectedRows() throws Exception {
        long rows = 0;
        for (String table : AcademicSchema.TABLES) {
            try (var lines = Files.lines(CLEAN.resolve(table + ".csv"))) {
                rows += lines.filter(line -> !line.isBlank()).count() - 1;
            }
        }
        return rows;
    }

}
