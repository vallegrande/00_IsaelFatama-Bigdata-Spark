package pe.edu.vallegrande.bigdata.job.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.RunOutputs;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;
import pe.edu.vallegrande.bigdata.spark.database.WriteMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;

final class LoadServingStep implements PipelineStep {

    private final Supplier<Database> database;

    LoadServingStep(Supplier<Database> database) {
        this.database = database;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.LOAD_SERVING;
    }

    @Override
    public String description() {
        return "Las tablas gold se cargan por lote en la capa que consume el dashboard";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        Path run = context.input();
        String batchId = RunOutputs.batchId(run);
        List<String> tables = RunOutputs.tables(run, WorkerProtocol.GOLD);
        Optional<JobType> dashboard = JobType.dashboardOf(tables);
        if (dashboard.isEmpty()) {
            return StepResult.blocked(0, "La ejecución " + batchId + " no tiene tablas gold del dashboard");
        }
        Database db = database.get();
        Map<String, Long> rowsByTable = new LinkedHashMap<>();
        long total = 0;
        for (String table : tables) {
            Dataset<Row> gold = RunOutputs.read(context.spark(), run, WorkerProtocol.GOLD, table)
                    .withColumn(DatabaseLayer.BATCH_COLUMN, lit(batchId))
                    .withColumn(DatabaseLayer.ROW_COLUMN, row_number().over(Window.orderBy(monotonically_increasing_id())));
            long written = db.write(gold, TableRef.of(DatabaseLayer.SERVING, table), WriteMode.REPLACE_BATCH, batchId);
            rowsByTable.put(table, written);
            total += written;
        }
        loadSummary(context, run, dashboard.get(), batchId, db);
        context.counts(context.processedRows() + total, 0);
        return StepResult.completed(total, "Filas cargadas en SERVING: " + total + " en " + tables.size() + " tablas", Map.of(
                "layer", DatabaseLayer.SERVING.name(),
                "batchId", batchId,
                "dashboard", dashboard.get().name(),
                "rowsByTable", rowsByTable));
    }

    private void loadSummary(PipelineContext context, Path run, JobType dashboard, String batchId, Database db) throws IOException {
        Path file = run.resolve(WorkerProtocol.SUMMARY_FILE);
        if (!Files.isRegularFile(file)) {
            return;
        }
        StructType schema = new StructType()
                .add(DatabaseLayer.JOB_TYPE_COLUMN, DataTypes.StringType)
                .add(DatabaseLayer.CONTENT_COLUMN, DataTypes.StringType)
                .add(DatabaseLayer.ROW_COLUMN, DataTypes.IntegerType)
                .add(DatabaseLayer.BATCH_COLUMN, DataTypes.StringType);
        Row row = RowFactory.create(dashboard.name(), Files.readString(file, StandardCharsets.UTF_8), 1, batchId);
        Dataset<Row> summary = context.spark().createDataFrame(List.of(row), schema);
        db.write(summary, TableRef.of(DatabaseLayer.SERVING, DatabaseLayer.SUMMARY_TABLE), WriteMode.REPLACE_BATCH, batchId);
    }

}
