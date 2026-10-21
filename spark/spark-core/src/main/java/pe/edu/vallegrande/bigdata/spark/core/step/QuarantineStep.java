package pe.edu.vallegrande.bigdata.spark.core.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class QuarantineStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.QUARANTINE;
    }

    @Override
    public String description() {
        return "Las filas observadas se guardan con su tabla, fila y motivo";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long rejected = context.publisher().publish(WorkerProtocol.QUARANTINE, WorkerProtocol.REJECTED_TABLE,
                context.validation().rejected());
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("rejectedRows", rejected);
        metrics.put("rowsByTable", context.validation().counts().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().rejected(), (left, right) -> left, LinkedHashMap::new)));
        metrics.put("outputTable", WorkerProtocol.REJECTED_TABLE);
        metrics.put("formats", java.util.List.of("csv", "parquet"));
        return StepResult.completed(rejected, rejected == 0 ? "Sin filas observadas" : rejected + " filas en cuarentena", metrics);
    }
}
