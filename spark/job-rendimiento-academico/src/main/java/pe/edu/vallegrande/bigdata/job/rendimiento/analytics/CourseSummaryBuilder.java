package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class CourseSummaryBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "cursos_resumen";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.rendimiento().groupBy("curso_id", "curso", "docente", "creditos", "periodo").agg(
                        count("*").alias("matriculados"),
                        Conditions.count(GradingPolicy.APROBADO, "aprobados"),
                        Conditions.count(GradingPolicy.DESAPROBADO, "desaprobados"),
                        Conditions.count(GradingPolicy.INHABILITADO, "inhabilitados"),
                        Conditions.count(GradingPolicy.PENDIENTE, "pendientes"),
                        round(avg("promedio"), 2).alias("promedio"),
                        round(expr("percentile_approx(promedio, 0.5)"), 2).alias("mediana"),
                        min("promedio").alias("promedio_minimo"),
                        max("promedio").alias("promedio_maximo"),
                        round(stddev("promedio"), 2).alias("desviacion"),
                        round(avg("asistencia_pct"), 4).alias("asistencia_promedio"),
                        round(corr("asistencia_pct", "promedio"), 4).alias("correlacion_asistencia_nota"),
                        sum(when(col("requiere_atencion"), 1).otherwise(0)).alias("requieren_atencion"))
                .withColumn("tasa_aprobacion", Conditions.passRate())
                .orderBy("curso_id");
    }
}
