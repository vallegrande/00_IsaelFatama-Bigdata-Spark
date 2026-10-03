package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class GradeDistributionBuilder implements GoldTableBuilder {

    @Override
    public String name() {
        return "distribucion_notas";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return inputs.rendimiento()
                .withColumn("rango", range())
                .withColumn("orden", order())
                .groupBy("curso_id", "curso", "rango", "orden").agg(count("*").alias("estudiantes"))
                .orderBy("curso_id", "orden");
    }

    private Column range() {
        return when(col("promedio").isNull(), "Sin notas")
                .when(col("promedio").lt(11), "00-10")
                .when(col("promedio").lt(GradingPolicy.PASSING_SCORE), "11-12")
                .when(col("promedio").lt(17), "13-16")
                .otherwise("17-20");
    }

    private Column order() {
        return when(col("rango").equalTo("00-10"), 1)
                .when(col("rango").equalTo("11-12"), 2)
                .when(col("rango").equalTo("13-16"), 3)
                .when(col("rango").equalTo("17-20"), 4)
                .otherwise(5);
    }
}
