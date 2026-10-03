package pe.edu.vallegrande.bigdata.spark.core.pipeline;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;

public interface PipelineStep {

    PipelineStage stage();

    String description();

    StepResult execute(PipelineContext context) throws Exception;
}
