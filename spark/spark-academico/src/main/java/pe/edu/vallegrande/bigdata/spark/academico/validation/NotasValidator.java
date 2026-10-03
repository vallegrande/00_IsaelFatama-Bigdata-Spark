package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.rule;

public final class NotasValidator implements TableValidator {

    @Override
    public String table() {
        return AcademicSchema.NOTAS;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> bronze, ReferenceData references) {
        Dataset<Row> typed = references.joinEnrollment(bronze
                .withColumn("_nota", expr("try_cast(nota as double)"))
                .withColumn("_peso", expr("try_cast(peso as double)"))
                .withColumn("_fecha", expr("try_cast(fecha_registro as date)")));
        List<Column> rules = new ArrayList<>(List.of(
                rule(col("nota").isNotNull().and(col("_nota").isNull()), QualityRule.NOTA_NO_NUMERICA),
                rule(col("_nota").isNotNull().and(not(col("_nota").between(GradingPolicy.MIN_SCORE, GradingPolicy.MAX_SCORE))),
                        QualityRule.NOTA_FUERA_DE_RANGO),
                rule(col("peso").isNotNull().and(not(coalesce(col("_peso").gt(0).and(col("_peso").leq(1)), lit(false)))),
                        QualityRule.PESO_INVALIDO),
                rule(col("fecha_registro").isNotNull().and(col("_fecha").isNull()), QualityRule.FECHA_INVALIDA)));
        rules.addAll(ReferenceData.enrollmentRules());
        return ValidationRules.check(table(), typed, rules);
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked).select(col("estudiante_id"), col("curso_id"), col("periodo"), col("evaluacion"),
                col("_peso").alias("peso"), col("_nota").alias("nota"), col("_fecha").alias("fecha_registro"));
    }
}
