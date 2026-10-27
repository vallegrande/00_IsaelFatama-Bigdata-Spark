package pe.edu.vallegrande.bigdata.job.load;

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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataLoadJobTests {

    private static final Path PUBLISHED_RUN = Path.of("..", "..", "runtime", "jobs", "rendimiento-local-01");

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
    void isALoadJobWithCoreAndServingStages() {
        DataLoadJob job = new DataLoadJob();

        assertEquals(JobType.DATA_LOAD, job.type());
        assertEquals(List.of(PipelineStage.LOAD_CORE, PipelineStage.LOAD_SERVING),
                job.steps().stream().map(PipelineStep::stage).toList());
    }

    @Test
    void loadsSilverByKeyMastersFirstAndGoldByBatch() throws Exception {
        RecordingDatabase database = new RecordingDatabase();

        PipelineResult result = new DataLoadJob(() -> database).pipeline()
                .run(spark, PUBLISHED_RUN, directory.resolve("run"), 0);

        assertTrue(result.published());
        List<Write> core = database.writes.stream().filter(write -> write.table.layer() == DatabaseLayer.CORE).toList();
        assertEquals(AcademicSchema.TABLES, core.stream().map(write -> write.table.name()).toList());
        core.forEach(write -> {
            assertEquals(WriteMode.MERGE, write.mode);
            assertEquals(AcademicSchema.KEYS.get(write.table.name()), write.table.keys());
            assertEquals("rendimiento-local-01", write.batchId);
        });

        List<Write> serving = database.writes.stream().filter(write -> write.table.layer() == DatabaseLayer.SERVING).toList();
        List<String> expected = new java.util.ArrayList<>(JobType.ACADEMIC_PERFORMANCE.goldTables().stream().sorted().toList());
        expected.add(DatabaseLayer.SUMMARY_TABLE);
        assertEquals(expected, serving.stream().map(write -> write.table.name()).toList());
        serving.forEach(write -> {
            assertEquals(WriteMode.REPLACE_BATCH, write.mode);
            assertTrue(write.columns.contains(DatabaseLayer.BATCH_COLUMN), write.table + " debe llevar batch_id");
            assertTrue(write.columns.contains(DatabaseLayer.ROW_COLUMN), write.table + " debe llevar row_no");
        });
    }

    @Test
    void sendsTheQuarantineToTheControlLayer() throws Exception {
        RecordingDatabase database = new RecordingDatabase();

        new DataLoadJob(() -> database).pipeline().run(spark, PUBLISHED_RUN, directory.resolve("run"), 0);

        Write rejected = database.writes.stream()
                .filter(write -> write.table.layer() == DatabaseLayer.CONTROL)
                .findFirst().orElseThrow();
        assertEquals("rechazados", rejected.table.name());
        assertEquals(WriteMode.REPLACE_BATCH, rejected.mode);
        assertTrue(rejected.rows > 0);
    }

    @Test
    void doesNotLoadAnythingFromARunThatWasNotPublished() throws Exception {
        RecordingDatabase database = new RecordingDatabase();
        Path blockedRun = Files.createDirectories(directory.resolve("bloqueada"));

        PipelineResult result = new DataLoadJob(() -> database).pipeline()
                .run(spark, blockedRun, directory.resolve("run"), 0);

        assertFalse(result.published());
        assertTrue(database.writes.isEmpty());
        assertTrue(Files.readString(directory.resolve("run").resolve("events.jsonl"))
                .contains("\"stage\":\"LOAD_SERVING\",\"status\":\"SKIPPED\""));
    }

    private record Write(TableRef table, WriteMode mode, String batchId, List<String> columns, long rows) {
    }

    private static final class RecordingDatabase implements Database {

        private final List<Write> writes = new ArrayList<>();

        @Override
        public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> read(org.apache.spark.sql.SparkSession spark, TableRef table) {
            throw new UnsupportedOperationException("La carga no lee");
        }

        @Override
        public long write(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> data, TableRef table, WriteMode mode, String batchId) {
            long rows = data.count();
            writes.add(new Write(table, mode, batchId, Arrays.asList(data.columns()), rows));
            return rows;
        }
    }

}
