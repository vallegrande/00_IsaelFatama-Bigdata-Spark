package pe.edu.vallegrande.bigdata.job.asistencia;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.asistencia.analytics.AttendanceAlertAnalytics;
import pe.edu.vallegrande.bigdata.spark.academico.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;

import java.util.List;

public final class AlertaAsistenciaJob implements JobDefinition {

    public static void main(String[] args) {
        JobLauncher.launch(new AlertaAsistenciaJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.ALERTA_ASISTENCIA;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(new AttendanceAlertAnalytics());
    }
}
