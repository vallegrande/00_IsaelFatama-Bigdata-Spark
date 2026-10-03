package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.invalidPositive;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.rule;

public final class SemestresValidator implements TableValidator {

    @Override
    public String table() {
        return AcademicSchema.SEMESTRES;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> bronze, ReferenceData references) {
        Dataset<Row> typed = bronze
                .withColumn("_numero", expr("try_cast(numero as int)"))
                .withColumn("_inicio", expr("try_cast(fecha_inicio as date)"))
                .withColumn("_fin", expr("try_cast(fecha_fin as date)"));
        return ValidationRules.check(table(), typed, List.of(
                rule(col("numero").isNotNull().and(invalidPositive(col("_numero"))), QualityRule.NUMERO_INVALIDO),
                rule(col("fecha_inicio").isNotNull().and(col("_inicio").isNull())
                        .or(col("fecha_fin").isNotNull().and(col("_fin").isNull())), QualityRule.FECHA_INVALIDA)));
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked).select(col("semestre_id"), col("_numero").alias("numero"), col("nombre"),
                col("periodo"), col("_inicio").alias("fecha_inicio"), col("_fin").alias("fecha_fin"));
    }

    @Override
    public ReferenceData publish(Dataset<Row> checked, ReferenceData references) {
        return references.withSemestres(ValidationRules.valid(checked).select(col("semestre_id").alias("_sem_id"),
                col("periodo").alias("_sem_periodo"), col("_inicio").alias("_sem_inicio"), col("_fin").alias("_sem_fin")));
    }
}
