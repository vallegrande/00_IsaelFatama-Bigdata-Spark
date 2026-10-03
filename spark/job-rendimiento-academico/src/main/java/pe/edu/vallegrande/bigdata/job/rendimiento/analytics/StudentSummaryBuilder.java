package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class StudentSummaryBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "estudiantes_resumen";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        WindowSpec semester = Window.partitionBy("semestre_id", "periodo");
        return totalsByStudent(inputs.rendimiento())
                .withColumn("promedio_ponderado", round(try_divide(col("_puntaje"), col("_creditos_con_nota")), 2))
                .withColumn("riesgo", risk())
                .withColumn("orden_merito", rank().over(semester.orderBy(col("promedio_ponderado").desc_nulls_last())))
                .withColumn("tercio_superior", col("orden_merito").leq(ceil(count("*").over(semester).divide(3))))
                .select("estudiante_id", "nombres", "apellidos", "seccion", "semestre_id", "periodo", "cursos",
                        "aprobados", "desaprobados", "inhabilitados", "pendientes", "creditos_matriculados",
                        "creditos_aprobados", "promedio_ponderado", "asistencia_promedio", "riesgo", "orden_merito",
                        "tercio_superior")
                .orderBy("orden_merito", "apellidos");
    }

    private Dataset<Row> totalsByStudent(Dataset<Row> rendimiento) {
        return rendimiento.groupBy("estudiante_id", "nombres", "apellidos", "seccion", "semestre_id", "periodo").agg(
                count("*").alias("cursos"),
                Conditions.count(GradingPolicy.APROBADO, "aprobados"),
                Conditions.count(GradingPolicy.DESAPROBADO, "desaprobados"),
                Conditions.count(GradingPolicy.INHABILITADO, "inhabilitados"),
                Conditions.count(GradingPolicy.PENDIENTE, "pendientes"),
                sum("creditos").alias("creditos_matriculados"),
                sum(when(col("condicion").equalTo(GradingPolicy.APROBADO), col("creditos")).otherwise(0)).alias("creditos_aprobados"),
                sum(when(col("promedio").isNotNull(), col("promedio").multiply(col("creditos")))).alias("_puntaje"),
                sum(when(col("promedio").isNotNull(), col("creditos"))).alias("_creditos_con_nota"),
                round(avg("asistencia_pct"), 4).alias("asistencia_promedio"));
    }

    private Column risk() {
        Column failures = col("desaprobados").plus(col("inhabilitados"));
        return when(failures.geq(2), "ALTO").when(failures.equalTo(1), "MEDIO").otherwise("BAJO");
    }
}
