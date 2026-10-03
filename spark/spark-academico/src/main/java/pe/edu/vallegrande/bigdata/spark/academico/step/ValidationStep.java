package pe.edu.vallegrande.bigdata.spark.academico.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.academico.validation.AcademicValidator;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

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
    public StepResult execute(PipelineContext context) {
        ValidationResult validation = validator.validate(context.bronze());
        context.validation(validation);
        return StepResult.completed(validation.received(),
                validation.accepted() + " filas válidas y " + validation.rejectedCount() + " observadas");
    }
}
