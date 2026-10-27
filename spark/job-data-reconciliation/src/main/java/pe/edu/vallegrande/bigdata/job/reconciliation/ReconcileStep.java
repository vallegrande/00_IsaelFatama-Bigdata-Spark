package pe.edu.vallegrande.bigdata.job.reconciliation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;
import pe.edu.vallegrande.bigdata.spark.database.RunOutputs;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.col;

final class ReconcileStep implements PipelineStep {

    private final Supplier<Database> database;

    ReconcileStep(Supplier<Database> database) {
        this.database = database;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.RECONCILE;
    }

    @Override
    public String description() {
        return "Conciliación: lo publicado en archivos contra lo cargado en la base de datos";
    }

    @Override
    public StepResult execute(PipelineContext context) throws Exception {
        Path run = context.input();
        String batchId = RunOutputs.batchId(run);
        Database db = database.get();
        List<Map<String, Object>> checks = new ArrayList<>();
        checks.addAll(reconcileCore(context, run, db));
        checks.addAll(reconcileServing(context, run, batchId, db));

        long expected = checks.stream().mapToLong(check -> (long) check.get("expected")).sum();
        long differences = checks.stream().mapToLong(check -> (long) check.get("difference")).sum();
        double ratio = expected == 0 ? 0 : (double) differences / expected;
        boolean withinTolerance = ratio <= context.maxRejectedRatio();

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("batchId", batchId);
        report.put("expectedRows", expected);
        report.put("differenceRows", differences);
        report.put("differenceRatio", ratio);
        report.put("tolerance", context.maxRejectedRatio());
        report.put("reconciled", withinTolerance);
        report.put("checks", checks);
        JsonPublisher.write(context.output().resolve(WorkerProtocol.RECONCILIATION_FILE), report);

        context.counts(expected, differences);
        Map<String, Object> metrics = Map.of("checks", checks.size(), "differenceRows", differences, "differenceRatio", ratio);
        return withinTolerance
                ? StepResult.completed(expected, "Conciliado: " + checks.size() + " tablas sin diferencias fuera de tolerancia", metrics)
                : StepResult.blocked(expected, differences + " filas con diferencias en " + checks.size() + " tablas revisadas", metrics);
    }

    private List<Map<String, Object>> reconcileCore(PipelineContext context, Path run, Database db) {
        List<Map<String, Object>> checks = new ArrayList<>();
        for (String table : AcademicSchema.TABLES) {
            List<String> keys = AcademicSchema.KEYS.get(table);
            Dataset<Row> silver = RunOutputs.read(context.spark(), run, WorkerProtocol.SILVER, table);
            Dataset<Row> core = DatabaseNaming.toLake(db.read(context.spark(), TableRef.of(DatabaseLayer.CORE, table, keys)));
            long expected = silver.count();
            long found = silver.alias("s").join(core.alias("c"), keyEquality(keys), "left_semi").count();
            checks.add(check(DatabaseLayer.CORE, table, expected, found, Math.max(0, expected - found)));
        }
        return checks;
    }

    private List<Map<String, Object>> reconcileServing(PipelineContext context, Path run, String batchId, Database db) {
        List<Map<String, Object>> checks = new ArrayList<>();
        for (String table : RunOutputs.tables(run, WorkerProtocol.GOLD)) {
            long expected = RunOutputs.read(context.spark(), run, WorkerProtocol.GOLD, table).count();
            long found = DatabaseNaming.toLake(db.read(context.spark(), TableRef.of(DatabaseLayer.SERVING, table)))
                    .filter(col(DatabaseLayer.BATCH_COLUMN).equalTo(batchId))
                    .count();
            checks.add(check(DatabaseLayer.SERVING, table, expected, found, Math.abs(expected - found)));
        }
        return checks;
    }

    private Column keyEquality(List<String> keys) {
        Column condition = null;
        for (String key : keys) {
            Column equal = col("s." + key).eqNullSafe(col("c." + key));
            condition = condition == null ? equal : condition.and(equal);
        }
        return condition;
    }

    private Map<String, Object> check(DatabaseLayer layer, String table, long expected, long found, long difference) {
        Map<String, Object> check = new LinkedHashMap<>();
        check.put("layer", layer.name());
        check.put("table", table);
        check.put("expected", expected);
        check.put("found", found);
        check.put("difference", difference);
        check.put("status", difference == 0 ? "OK" : "DIFERENCIA");
        return check;
    }

}
