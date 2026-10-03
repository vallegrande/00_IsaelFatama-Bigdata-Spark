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

public final class CursosValidator implements TableValidator {

    @Override
    public String table() {
        return AcademicSchema.CURSOS;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> bronze, ReferenceData references) {
        Dataset<Row> typed = bronze
                .withColumn("_creditos", expr("try_cast(creditos as int)"))
                .withColumn("_horas", expr("try_cast(horas_semanales as int)"))
                .join(references.semestres(), col("semestre_id").equalTo(col("_sem_id")), "left");
        return ValidationRules.check(table(), typed, List.of(
                rule(col("creditos").isNotNull().and(invalidPositive(col("_creditos")))
                        .or(col("horas_semanales").isNotNull().and(invalidPositive(col("_horas")))), QualityRule.NUMERO_INVALIDO),
                rule(col("semestre_id").isNotNull().and(col("_sem_id").isNull()), QualityRule.SEMESTRE_INEXISTENTE)));
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked).select(col("curso_id"), col("nombre"), col("semestre_id"),
                col("_creditos").alias("creditos"), col("_horas").alias("horas_semanales"), col("docente"));
    }

    @Override
    public ReferenceData publish(Dataset<Row> checked, ReferenceData references) {
        return references.withCursos(ValidationRules.valid(checked).select(col("curso_id").alias("_cur_id"),
                col("semestre_id").alias("_cur_semestre"), col("_sem_periodo").alias("_cur_periodo"),
                col("_sem_inicio").alias("_cur_inicio"), col("_sem_fin").alias("_cur_fin")));
    }
}
