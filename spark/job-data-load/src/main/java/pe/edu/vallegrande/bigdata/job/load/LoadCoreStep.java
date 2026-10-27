package pe.edu.vallegrande.bigdata.job.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.lit;

final class LoadCoreStep implements PipelineStep {

    private final Supplier<Database> database;

    LoadCoreStep(Supplier<Database> database) {
        this.database = database;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.LOAD_CORE;
    }

    @Override
    public String description() {
        return "Las tablas silver se cargan en la base de datos por clave natural (maestras primero)";
    }

    @Override
    public StepResult execute(PipelineContext context) {
        Path run = context.input();
        String batchId = RunOutputs.batchId(run);
        if (RunOutputs.tables(run, WorkerProtocol.GOLD).isEmpty()) {
            return StepResult.blocked(0, "La ejecución " + batchId + " no publicó Gold: no se carga nada");
        }
        Database db = database.get();
        Map<String, Long> rowsByTable = new LinkedHashMap<>();
        long total = 0;
        for (String table : AcademicSchema.TABLES) {
            Dataset<Row> silver = RunOutputs.read(context.spark(), run, WorkerProtocol.SILVER, table);
            TableRef target = TableRef.of(DatabaseLayer.CORE, table, AcademicSchema.KEYS.get(table));
            long written = db.write(silver, target, WriteMode.MERGE, batchId);
            rowsByTable.put(table, written);
            total += written;
        }
        long rejected = loadRejected(context, run, batchId, db);
        context.counts(total, 0);
        return StepResult.completed(total, "Filas cargadas en CORE: " + total + "; cuarentena: " + rejected, Map.of(
                "layer", DatabaseLayer.CORE.name(),
                "batchId", batchId,
                "rowsByTable", rowsByTable,
                "rejectedRows", rejected));
    }

    private long loadRejected(PipelineContext context, Path run, String batchId, Database db) {
        if (!RunOutputs.exists(run, WorkerProtocol.QUARANTINE, WorkerProtocol.REJECTED_TABLE)) {
            return 0;
        }
        Dataset<Row> rejected = RunOutputs.read(context.spark(), run, WorkerProtocol.QUARANTINE, WorkerProtocol.REJECTED_TABLE)
                .withColumn(DatabaseLayer.BATCH_COLUMN, lit(batchId));
        TableRef target = TableRef.of(DatabaseLayer.CONTROL, WorkerProtocol.REJECTED_TABLE);
        return db.write(rejected, target, WriteMode.REPLACE_BATCH, batchId);
    }

}
