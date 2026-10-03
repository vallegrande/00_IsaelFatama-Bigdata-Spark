package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AcademicAnalytics implements GoldAnalytics {
    private final GoldTableBuilder performance;
    private final List<GoldTableBuilder> builders;
    private final GoldTableBuilder summary;

    public AcademicAnalytics() {
        this(new PerformanceBuilder(), List.of(new CourseSummaryBuilder(), new StudentSummaryBuilder(),
                new SectionSummaryBuilder(), new AssessmentSummaryBuilder(), new GradeDistributionBuilder(),
                new WeeklyAttendanceBuilder()), new SummaryBuilder());
    }

    public AcademicAnalytics(GoldTableBuilder performance, List<GoldTableBuilder> builders, GoldTableBuilder summary) {
        this.performance = performance;
        this.builders = List.copyOf(builders);
        this.summary = summary;
    }

    @Override
    public String description() {
        return "Joins entre tablas y agregaciones: promedios, condición, asistencia y orden de mérito";
    }

    @Override
    public GoldTables analyze(ValidationResult silver) {
        GoldInputs base = GoldInputs.from(silver);
        Dataset<Row> rendimiento = performance.build(base).cache();
        GoldInputs inputs = base.withRendimiento(rendimiento);
        Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
        tables.put(performance.name(), rendimiento);
        builders.forEach(builder -> tables.put(builder.name(), builder.build(inputs)));
        return new GoldTables(performance.name(), tables, summary.build(inputs));
    }

    @Override
    public String detail(long rows) {
        return rows + " matrículas estudiante-curso con indicadores";
    }
}
