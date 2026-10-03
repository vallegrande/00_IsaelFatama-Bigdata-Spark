package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public record ReferenceData(Dataset<Row> semestres, Dataset<Row> cursos, Dataset<Row> estudiantes) {

    public static ReferenceData empty() {
        return new ReferenceData(null, null, null);
    }

    public static List<Column> enrollmentRules() {
        return List.of(
                ValidationRules.rule(col("estudiante_id").isNotNull().and(col("_est_id").isNull()), QualityRule.ESTUDIANTE_INEXISTENTE),
                ValidationRules.rule(col("curso_id").isNotNull().and(col("_cur_id").isNull()), QualityRule.CURSO_INEXISTENTE),
                ValidationRules.rule(col("_est_semestre").isNotNull().and(col("_cur_semestre").isNotNull())
                        .and(col("_est_semestre").notEqual(col("_cur_semestre"))), QualityRule.CURSO_DE_OTRO_SEMESTRE),
                ValidationRules.rule(col("periodo").isNotNull().and(col("_cur_periodo").isNotNull())
                        .and(col("periodo").notEqual(col("_cur_periodo"))), QualityRule.PERIODO_INCONSISTENTE));
    }

    public ReferenceData withSemestres(Dataset<Row> value) {
        return new ReferenceData(value, cursos, estudiantes);
    }

    public ReferenceData withCursos(Dataset<Row> value) {
        return new ReferenceData(semestres, value, estudiantes);
    }

    public ReferenceData withEstudiantes(Dataset<Row> value) {
        return new ReferenceData(semestres, cursos, value);
    }

    public Dataset<Row> joinEnrollment(Dataset<Row> facts) {
        return facts
                .join(estudiantes, col("estudiante_id").equalTo(col("_est_id")), "left")
                .join(cursos, col("curso_id").equalTo(col("_cur_id")), "left");
    }
}
