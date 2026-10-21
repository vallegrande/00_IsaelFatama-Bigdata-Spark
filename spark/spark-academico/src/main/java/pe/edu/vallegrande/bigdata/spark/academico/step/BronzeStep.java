package pe.edu.vallegrande.bigdata.spark.academico.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.academico.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

import java.util.LinkedHashMap;
import java.util.Map;

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
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("inputFiles", bronze.tables().keySet().stream().map(table -> table + ".csv").toList());
        metrics.put("tableCount", bronze.tables().size());
        metrics.put("rowsByTable", bronze.rows());
        metrics.put("totalRows", bronze.totalRows());
        return StepResult.completed(bronze.totalRows(), "Filas leídas: " + bronze.describe(), metrics);
    }
}
