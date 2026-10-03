package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class AssessmentSummaryBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "evaluaciones_resumen";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.notas().groupBy("curso_id", "evaluacion").agg(
                        first("peso").alias("peso"),
                        count("*").alias("programadas"),
                        count("nota").alias("registradas"),
                        round(avg("nota"), 2).alias("promedio"),
                        sum(when(col("nota").geq(GradingPolicy.PASSING_SCORE), 1).otherwise(0)).alias("aprobados"),
                        min("fecha_registro").alias("fecha"))
                .withColumn("pendientes", col("programadas").minus(col("registradas")))
                .withColumn("tasa_aprobacion", round(try_divide(col("aprobados"), col("registradas")), 4))
                .join(inputs.courseNames(), "curso_id")
                .select("curso_id", "curso", "evaluacion", "peso", "programadas", "registradas", "pendientes", "promedio",
                        "aprobados", "tasa_aprobacion", "fecha")
                .orderBy(col("curso_id"), col("fecha").asc_nulls_last(), col("evaluacion"));
    }
}
