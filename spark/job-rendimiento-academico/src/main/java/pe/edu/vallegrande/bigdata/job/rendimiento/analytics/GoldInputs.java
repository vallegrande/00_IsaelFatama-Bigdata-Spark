package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;

import static org.apache.spark.sql.functions.col;

public record GoldInputs(Dataset<Row> semestres, Dataset<Row> cursos, Dataset<Row> estudiantes, Dataset<Row> notas,
                         Dataset<Row> asistencia, Dataset<Row> rendimiento) {

    public static GoldInputs from(ValidationResult silver) {
        return new GoldInputs(silver.silver(AcademicSchema.SEMESTRES), silver.silver(AcademicSchema.CURSOS),
                silver.silver(AcademicSchema.ESTUDIANTES), silver.silver(AcademicSchema.NOTAS),
                silver.silver(AcademicSchema.ASISTENCIA), null);
    }

    public GoldInputs withRendimiento(Dataset<Row> value) {
        return new GoldInputs(semestres, cursos, estudiantes, notas, asistencia, value);
    }

    public Dataset<Row> courseNames() {
        return cursos.select(col("curso_id"), col("nombre").alias("curso"));
    }
}
