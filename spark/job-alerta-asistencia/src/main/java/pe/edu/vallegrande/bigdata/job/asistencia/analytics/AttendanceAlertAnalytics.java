package pe.edu.vallegrande.bigdata.job.asistencia.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;
import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public final class AttendanceAlertAnalytics implements GoldAnalytics {
    public static final String ALERTS = "alertas_asistencia";
    public static final String BY_COURSE = "alertas_por_curso";

    private static Column level() {
        Column ratio = try_divide(col("faltas"), col("sesiones"));
        return when(ratio.gt(GradingPolicy.MAX_ABSENCE_RATIO), lit(GradingPolicy.INHABILITADO))
                .when(ratio.geq(GradingPolicy.RISK_ABSENCE_RATIO), lit(GradingPolicy.EN_RIESGO))
                .otherwise(lit(GradingPolicy.NORMAL));
    }

    private static Column severity() {
        return when(col("nivel").equalTo(GradingPolicy.INHABILITADO), 0)
                .when(col("nivel").equalTo(GradingPolicy.EN_RIESGO), 1)
                .otherwise(2);
    }

    private static Column countState(String state, String alias) {
        return sum(when(col("estado").equalTo(state), 1).otherwise(0)).alias(alias);
    }

    private static Column countLevel(String level, String alias) {
        return sum(when(col("nivel").equalTo(level), 1).otherwise(0)).alias(alias);
    }

    @Override
    public String description() {
        return "Faltas por estudiante y curso comparadas con el 30 % de inasistencia permitido";
    }

    @Override
    public GoldTables analyze(ValidationResult silver) {
        Dataset<Row> alerts = alerts(silver).cache();
        Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
        tables.put(ALERTS, alerts);
        tables.put(BY_COURSE, byCourse(alerts));
        return new GoldTables(ALERTS, tables, summary(alerts));
    }

    @Override
    public String detail(long rows) {
        return rows + " matrículas evaluadas por inasistencia";
    }

    private Dataset<Row> alerts(ValidationResult silver) {
        Dataset<Row> students = silver.silver(AcademicSchema.ESTUDIANTES)
                .select(col("estudiante_id"), col("nombres"), col("apellidos"), col("seccion"));
        Dataset<Row> courses = silver.silver(AcademicSchema.CURSOS).select(col("curso_id"), col("nombre").alias("curso"));
        return silver.silver(AcademicSchema.ASISTENCIA)
                .groupBy("estudiante_id", "curso_id", "periodo")
                .agg(count("*").alias("sesiones"),
                        countState("F", "faltas"),
                        countState("J", "justificadas"))
                .withColumn("inasistencia_pct", round(try_divide(col("faltas"), col("sesiones")), 4))
                .withColumn("faltas_permitidas", floor(col("sesiones").multiply(GradingPolicy.MAX_ABSENCE_RATIO)))
                .withColumn("faltas_restantes", greatest(col("faltas_permitidas").minus(col("faltas")), lit(0)))
                .withColumn("nivel", level())
                .join(students, "estudiante_id")
                .join(courses, "curso_id")
                .select("estudiante_id", "nombres", "apellidos", "seccion", "curso_id", "curso", "periodo", "sesiones",
                        "faltas", "justificadas", "inasistencia_pct", "faltas_permitidas", "faltas_restantes", "nivel")
                .orderBy(severity(), col("inasistencia_pct").desc(), col("estudiante_id"), col("curso_id"));
    }

    private Dataset<Row> byCourse(Dataset<Row> alerts) {
        return alerts.groupBy("curso_id", "curso")
                .agg(count("*").alias("matriculas"),
                        countLevel(GradingPolicy.INHABILITADO, "inhabilitados"),
                        countLevel(GradingPolicy.EN_RIESGO, "en_riesgo"),
                        countLevel(GradingPolicy.NORMAL, "normales"),
                        round(avg("inasistencia_pct"), 4).alias("inasistencia_promedio"))
                .orderBy(col("inhabilitados").desc(), col("en_riesgo").desc(), col("curso_id"));
    }

    private Dataset<Row> summary(Dataset<Row> alerts) {
        return alerts.agg(
                        count("*").alias("matriculas"),
                        countDistinct("estudiante_id").alias("estudiantes"),
                        countLevel(GradingPolicy.INHABILITADO, "inhabilitados"),
                        countLevel(GradingPolicy.EN_RIESGO, "en_riesgo"),
                        countLevel(GradingPolicy.NORMAL, "normales"),
                        countDistinct(when(col("nivel").notEqual(GradingPolicy.NORMAL), col("estudiante_id")))
                                .alias("estudiantes_con_alerta"),
                        round(avg("inasistencia_pct"), 4).alias("inasistencia_promedio"),
                        first("periodo").alias("periodo"))
                .withColumn("maximo_inasistencia", lit(GradingPolicy.MAX_ABSENCE_RATIO))
                .withColumn("umbral_riesgo", lit(GradingPolicy.RISK_ABSENCE_RATIO));
    }
}
