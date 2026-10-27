package pe.edu.vallegrande.bigdata.job.performance;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.job.performance.analytics.AcademicAnalytics;
import pe.edu.vallegrande.bigdata.spark.academic.AcademicSteps;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.bigdata.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.List;
import java.util.Objects;

public final class AcademicPerformanceJob implements JobDefinition {
    private final GoldAnalytics analytics;

    public AcademicPerformanceJob() {
        this(new AcademicAnalytics());
    }

    AcademicPerformanceJob(GoldAnalytics analytics) {
        this.analytics = Objects.requireNonNull(analytics, "analytics");
    }

    public static void main(String[] args) {
        JobLauncher.launch(new AcademicPerformanceJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.ACADEMIC_PERFORMANCE;
    }

    @Override
    public List<PipelineStep> steps() {
        return AcademicSteps.medallion(analytics);
    }
}
