package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class SectionSummaryBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "secciones_resumen";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.rendimiento().groupBy("seccion", "curso_id", "curso").agg(
                        countDistinct("estudiante_id").alias("estudiantes"),
                        round(avg("promedio"), 2).alias("promedio"),
                        Conditions.count(GradingPolicy.APROBADO, "aprobados"),
                        Conditions.count(GradingPolicy.DESAPROBADO, "desaprobados"),
                        Conditions.count(GradingPolicy.INHABILITADO, "inhabilitados"),
                        round(avg("asistencia_pct"), 4).alias("asistencia_promedio"))
                .withColumn("tasa_aprobacion", Conditions.passRate())
                .orderBy("seccion", "curso_id");
    }
}
