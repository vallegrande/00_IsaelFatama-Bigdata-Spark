package pe.edu.vallegrande.bigdata.spark.academico.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.academico.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

public final class BronzeStep implements PipelineStep {
    private final CsvExtractor extractor;

    public BronzeStep(CsvExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.BRONZE;
    }

    @Override
    public String description() {
        return "Spark lee las cinco tablas CSV del sistema de notas";
    }

    @Override
    public StepResult execute(PipelineContext context) {
        BronzeTables bronze = extractor.read(context.spark(), context.input());
        context.bronze(bronze);
        return StepResult.completed(bronze.totalRows(), "Filas leídas: " + bronze.describe());
    }
}
