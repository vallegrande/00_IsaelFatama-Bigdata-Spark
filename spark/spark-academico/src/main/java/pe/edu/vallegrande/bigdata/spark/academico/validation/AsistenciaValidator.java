package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.invalidPositive;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.rule;

public final class AsistenciaValidator implements TableValidator {

    @Override
    public String table() {
        return AcademicSchema.ASISTENCIA;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> bronze, ReferenceData references) {
        Dataset<Row> typed = references.joinEnrollment(bronze
                .withColumn("_semana", expr("try_cast(semana as int)"))
                .withColumn("_fecha", expr("try_cast(fecha as date)"))
                .withColumn("_estado", upper(col("estado"))));
        Column outsidePeriod = col("_fecha").lt(col("_cur_inicio")).or(col("_fecha").gt(col("_cur_fin")));
        List<Column> rules = new ArrayList<>(List.of(
                rule(col("semana").isNotNull().and(invalidPositive(col("_semana"))), QualityRule.NUMERO_INVALIDO),
                rule(col("fecha").isNotNull().and(col("_fecha").isNull()), QualityRule.FECHA_INVALIDA),
                rule(col("_fecha").isNotNull().and(col("_cur_inicio").isNotNull()).and(outsidePeriod), QualityRule.FECHA_FUERA_DE_PERIODO),
                rule(col("estado").isNotNull().and(not(col("_estado").isin(AcademicSchema.ATTENDANCE_STATES.toArray()))),
                        QualityRule.ESTADO_INVALIDO)));
        rules.addAll(ReferenceData.enrollmentRules());
        return ValidationRules.check(table(), typed, rules);
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked).select(col("estudiante_id"), col("curso_id"), col("periodo"),
                col("_semana").alias("semana"), col("_fecha").alias("fecha"), col("_estado").alias("estado"));
    }
}
