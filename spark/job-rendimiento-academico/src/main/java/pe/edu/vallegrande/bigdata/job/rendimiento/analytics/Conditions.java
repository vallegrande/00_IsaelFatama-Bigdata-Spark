package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

final class Conditions {

    private Conditions() {
    }

    static Column count(String condition, String alias) {
        return sum(when(col("condicion").equalTo(condition), 1).otherwise(0)).alias(alias);
    }

    static Column passRate() {
        Column graded = col("aprobados").plus(col("desaprobados"));
        return when(graded.gt(0), round(try_divide(col("aprobados"), graded.plus(col("inhabilitados"))), 4));
    }
}
