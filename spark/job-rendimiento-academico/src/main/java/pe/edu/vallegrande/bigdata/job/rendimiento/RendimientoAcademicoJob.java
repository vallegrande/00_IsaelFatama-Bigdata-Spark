package pe.edu.vallegrande.bigdata.job.rendimiento;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.rendimiento.analytics.AcademicAnalytics;
import pe.edu.vallegrande.bigdata.spark.academico.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;

import java.util.List;

public final class RendimientoAcademicoJob implements JobDefinition {

    public static void main(String[] args) {
        JobLauncher.launch(new RendimientoAcademicoJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.RENDIMIENTO_ACADEMICO;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(new AcademicAnalytics());
    }
}
