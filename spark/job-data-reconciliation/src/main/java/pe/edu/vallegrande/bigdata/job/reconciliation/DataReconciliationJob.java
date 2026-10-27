package pe.edu.vallegrande.bigdata.job.reconciliation;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.oracle.OracleDatabase;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class DataReconciliationJob implements JobDefinition {

    private final Supplier<Database> database;

    public DataReconciliationJob() {
        this(OracleDatabase::fromEnvironment);
    }

    public DataReconciliationJob(Supplier<Database> database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new DataReconciliationJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.DATA_RECONCILIATION;
    }

    @Override
    public List<PipelineStep> steps() {
        return List.of(new ReconcileStep(database));
    }

}
