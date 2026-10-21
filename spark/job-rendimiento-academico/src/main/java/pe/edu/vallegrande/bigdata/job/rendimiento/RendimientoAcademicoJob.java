package pe.edu.vallegrande.bigdata.job.rendimiento;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.rendimiento.analytics.AcademicAnalytics;
import pe.edu.vallegrande.bigdata.spark.academico.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.List;
import java.util.Objects;

public final class RendimientoAcademicoJob implements JobDefinition {
    private final GoldAnalytics analytics;

    public RendimientoAcademicoJob() {
        this(new AcademicAnalytics());
    }

    RendimientoAcademicoJob(GoldAnalytics analytics) {
        this.analytics = Objects.requireNonNull(analytics, "analytics");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new RendimientoAcademicoJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.RENDIMIENTO_ACADEMICO;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(analytics);
    }
}
