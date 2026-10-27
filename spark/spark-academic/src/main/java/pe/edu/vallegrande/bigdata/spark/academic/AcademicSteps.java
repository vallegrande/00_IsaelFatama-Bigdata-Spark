package pe.edu.vallegrande.bigdata.spark.academic;

import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.spark.academic.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.academic.quality.MasterTablesCheck;
import pe.edu.vallegrande.bigdata.spark.academic.step.BronzeStep;
import pe.edu.vallegrande.bigdata.spark.academic.step.ValidationStep;
import pe.edu.vallegrande.bigdata.spark.academic.validation.AcademicValidator;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityGate;
import pe.edu.vallegrande.bigdata.spark.core.step.*;

import java.util.List;

public final class AcademicSteps {

    private AcademicSteps() {
    }

    public static List<PipelineStep> medallion(GoldAnalytics analytics) {
        return List.of(
                new BronzeStep(new CsvExtractor()),
                new ValidationStep(new AcademicValidator()),
                new QuarantineStep(),
                new SilverStep(),
                new QualityGateStep(qualityGate()),
                new GoldStep(analytics),
                new ExportStep());
    }

    public static QualityGate qualityGate() {
        return new QualityGate(AcademicSchema.RULES_VERSION, List.of(new MasterTablesCheck()));
    }
}
