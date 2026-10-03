package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class SummaryBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "resumen";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.rendimiento().agg(
                        countDistinct("estudiante_id").alias("estudiantes"),
                        countDistinct("curso_id").alias("cursos"),
                        count("*").alias("matriculas"),
                        Conditions.count(GradingPolicy.APROBADO, "aprobados"),
                        Conditions.count(GradingPolicy.DESAPROBADO, "desaprobados"),
                        Conditions.count(GradingPolicy.INHABILITADO, "inhabilitados"),
                        Conditions.count(GradingPolicy.PENDIENTE, "pendientes"),
                        round(avg("promedio"), 2).alias("promedio_general"),
                        round(avg("asistencia_pct"), 4).alias("asistencia_promedio"),
                        sum(when(col("requiere_atencion"), 1).otherwise(0)).alias("requieren_atencion"),
                        round(corr("asistencia_pct", "promedio"), 4).alias("correlacion_asistencia_nota"),
                        first("periodo").alias("periodo"))
                .withColumn("tasa_aprobacion", Conditions.passRate())
                .withColumn("nota_aprobatoria", lit(GradingPolicy.PASSING_SCORE))
                .withColumn("maximo_inasistencia", lit(GradingPolicy.MAX_ABSENCE_RATIO));
    }
}
