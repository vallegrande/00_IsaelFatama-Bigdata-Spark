package pe.edu.vallegrande.bigdata.job.ingestion;

import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.seed.StagingSeeder;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class IngestStep implements PipelineStep {

    private final Supplier<DatabaseConfig> config;

    IngestStep(Supplier<DatabaseConfig> config) {
        this.config = config;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.INGEST;
    }

    @Override
    public String description() {
        return "Los cinco CSV del dataset se cargan como texto crudo en la capa " + DatabaseLayer.STAGING;
    }

    @Override
    public StepResult execute(PipelineContext context) throws Exception {
        Map<String, Long> rowsByTable = StagingSeeder.seed(config.get(), context.input());
        long total = rowsByTable.values().stream().mapToLong(Long::longValue).sum();
        context.counts(total, 0);
        String detail = rowsByTable.entrySet().stream()
                .map(entry -> entry.getKey() + " " + entry.getValue())
                .collect(Collectors.joining(", "));
        return StepResult.completed(total, "Filas ingeridas: " + detail, Map.of(
                "layer", DatabaseLayer.STAGING.name(),
                "rowsByTable", rowsByTable,
                "totalRows", total));
    }

}
