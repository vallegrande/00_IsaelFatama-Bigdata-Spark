package pe.edu.vallegrande.bigdata.job.asistencia;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.asistencia.analytics.AttendanceAlertAnalytics;
import pe.edu.vallegrande.bigdata.spark.academico.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.List;
import java.util.Objects;

public final class AlertaAsistenciaJob implements JobDefinition {
    private final GoldAnalytics analytics;

    public AlertaAsistenciaJob() {
        this(new AttendanceAlertAnalytics());
    }

    AlertaAsistenciaJob(GoldAnalytics analytics) {
        this.analytics = Objects.requireNonNull(analytics, "analytics");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new AlertaAsistenciaJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.ALERTA_ASISTENCIA;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(analytics);
    }
}
