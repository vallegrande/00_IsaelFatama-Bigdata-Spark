package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public final class WeeklyAttendanceBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "asistencia_semanal";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.asistencia().groupBy("curso_id", "semana").agg(
                        min("fecha").alias("fecha"),
                        count("*").alias("registros"),
                        sum(when(col("estado").isin("P", "T"), 1).otherwise(0)).alias("asistieron"),
                        sum(when(col("estado").equalTo("F"), 1).otherwise(0)).alias("faltas"),
                        sum(when(col("estado").equalTo("J"), 1).otherwise(0)).alias("justificadas"))
                .withColumn("asistencia_pct", round(try_divide(col("asistieron"), col("registros").minus(col("justificadas"))), 4))
                .join(inputs.courseNames(), "curso_id")
                .select("curso_id", "curso", "semana", "fecha", "registros", "asistieron", "faltas", "justificadas", "asistencia_pct")
                .orderBy("curso_id", "semana");
    }
}
