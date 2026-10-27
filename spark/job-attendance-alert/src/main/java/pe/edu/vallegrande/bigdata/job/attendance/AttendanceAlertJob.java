package pe.edu.vallegrande.bigdata.job.attendance;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.attendance.analytics.AttendanceAlertAnalytics;
import pe.edu.vallegrande.bigdata.spark.academic.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.List;
import java.util.Objects;

public final class AttendanceAlertJob implements JobDefinition {
    private final GoldAnalytics analytics;

    public AttendanceAlertJob() {
        this(new AttendanceAlertAnalytics());
    }

    AttendanceAlertJob(GoldAnalytics analytics) {
        this.analytics = Objects.requireNonNull(analytics, "analytics");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new AttendanceAlertJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.ATTENDANCE_ALERT;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(analytics);
    }
}
