package pe.edu.vallegrande.bigdata.spark.academic.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.academic.validation.AcademicValidator;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ValidationStep implements PipelineStep {
    private final AcademicValidator validator;

    public ValidationStep(AcademicValidator validator) {
        this.validator = validator;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.VALIDATION;
    }

    @Override
    public String description() {
        return "Tipos de datos, reglas de calidad, duplicados e integridad entre tablas";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        ValidationResult validation = validator.validate(context.bronze());
        context.validation(validation);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("received", validation.received());
        summary.put("accepted", validation.accepted());
        summary.put("rejected", validation.rejectedCount());
        summary.put("tables", validation.counts());
        summary.put("reasons", validation.reasons());
        JsonPublisher.write(context.output().resolve(WorkerProtocol.VALIDATION_FILE), summary);
        return StepResult.completed(validation.received(),
                validation.accepted() + " filas válidas y " + validation.rejectedCount() + " observadas", summary);
    }
}
