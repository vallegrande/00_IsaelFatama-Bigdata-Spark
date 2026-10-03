package pe.edu.vallegrande.bigdata.spark.core.step;

import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

public final class GoldStep implements PipelineStep {
    private final GoldAnalytics analytics;

    public GoldStep(GoldAnalytics analytics) {
        this.analytics = analytics;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.GOLD;
    }

    @Override
    public String description() {
        return analytics.description();
    }

    @Override
    public StepResult execute(PipelineContext context) {
        GoldTables gold = analytics.analyze(context.validation());
        context.gold(gold);
        long rows = gold.primaryTable().count();
        return StepResult.completed(rows, analytics.detail(rows));
    }
}
