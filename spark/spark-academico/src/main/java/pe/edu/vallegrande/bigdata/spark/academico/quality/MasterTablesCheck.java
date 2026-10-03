package pe.edu.vallegrande.bigdata.spark.academico.quality;

import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityCheck;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityReport;

import java.util.List;

public final class MasterTablesCheck implements QualityCheck {
    private static final List<String> MASTERS = List.of(AcademicSchema.SEMESTRES, AcademicSchema.CURSOS, AcademicSchema.ESTUDIANTES);

    @Override
    public List<QualityReport.Check> evaluate(ValidationResult validation) {
        return MASTERS.stream().map(master -> {
            long accepted = validation.counts().get(master).accepted();
            return new QualityReport.Check("Tabla maestra " + master + " con datos válidos", accepted > 0, accepted + " filas válidas");
        }).toList();
    }
}
