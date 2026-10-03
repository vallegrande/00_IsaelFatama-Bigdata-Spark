package pe.edu.vallegrande.bigdata.spark.core.launcher;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.Pipeline;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;

import java.util.List;

public interface JobDefinition {

    JobType type();

    List<PipelineStep> steps();

    default Pipeline pipeline() {
        return new Pipeline(steps());
    }
}
