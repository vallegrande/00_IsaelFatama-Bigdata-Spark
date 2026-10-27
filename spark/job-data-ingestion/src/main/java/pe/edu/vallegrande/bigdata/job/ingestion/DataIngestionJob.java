package pe.edu.vallegrande.bigdata.job.ingestion;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class DataIngestionJob implements JobDefinition {

    private final Supplier<DatabaseConfig> config;

    public DataIngestionJob() {
        this(DatabaseConfig::requiredFromEnvironment);
    }

    public DataIngestionJob(Supplier<DatabaseConfig> config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new DataIngestionJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.DATA_INGESTION;
    }

    @Override
    public List<PipelineStep> steps() {
        return List.of(new IngestStep(config));
    }

}
