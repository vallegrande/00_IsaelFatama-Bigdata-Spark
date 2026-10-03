package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static pe.edu.vallegrande.bigdata.spark.academico.validation.ValidationRules.rule;

public final class EstudiantesValidator implements TableValidator {
    private static final String EMAIL_PATTERN = "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$";

    @Override
    public String table() {
        return AcademicSchema.ESTUDIANTES;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> bronze, ReferenceData references) {
        Dataset<Row> joined = bronze.join(references.semestres().select(col("_sem_id")),
                col("semestre_id").equalTo(col("_sem_id")), "left");
        return ValidationRules.check(table(), joined, List.of(
                rule(col("correo").isNotNull().and(not(col("correo").rlike(EMAIL_PATTERN))), QualityRule.CORREO_INVALIDO),
                rule(col("semestre_id").isNotNull().and(col("_sem_id").isNull()), QualityRule.SEMESTRE_INEXISTENTE)));
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked).select(ValidationRules.columns(AcademicSchema.columns(table())));
    }

    @Override
    public ReferenceData publish(Dataset<Row> checked, ReferenceData references) {
        return references.withEstudiantes(ValidationRules.valid(checked)
                .select(col("estudiante_id").alias("_est_id"), col("semestre_id").alias("_est_semestre")));
    }
}
