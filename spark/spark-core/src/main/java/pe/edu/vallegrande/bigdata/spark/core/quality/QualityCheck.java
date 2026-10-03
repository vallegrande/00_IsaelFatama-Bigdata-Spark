package pe.edu.vallegrande.bigdata.spark.core.quality;

import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;

import java.util.List;

public interface QualityCheck {

    List<QualityReport.Check> evaluate(ValidationResult validation);
}
