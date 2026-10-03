package pe.edu.vallegrande.bigdata.spark.core.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityGate;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityReport;

import java.io.IOException;

public final class QualityGateStep implements PipelineStep {
    private final QualityGate gate;

    public QualityGateStep(QualityGate gate) {
        this.gate = gate;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.QUALITY_GATE;
    }

    @Override
    public String description() {
        return "Se comparan los rechazos de cada tabla con la tolerancia del job";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        QualityReport report = gate.evaluate(context.validation(), context.maxRejectedRatio());
        context.quality(report);
        JsonPublisher.write(context.output().resolve(WorkerProtocol.QUALITY_FILE), report);
        long rows = context.validation().received();
        return report.published() ? StepResult.completed(rows, report.decision()) : StepResult.blocked(rows, report.decision());
    }
}
