package pe.edu.vallegrande.bigdata.spark.core.pipeline;

import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.spark.core.publication.TablePublisher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class Pipeline {
    private static final String SKIPPED = "No se ejecuta porque la puerta de calidad bloqueó el lote";

    private final List<PipelineStep> steps;

    public Pipeline(List<PipelineStep> steps) {
        this.steps = List.copyOf(steps);
    }

    public PipelineResult run(SparkSession spark, Path input, Path output, double maxRejectedRatio) throws Exception {
        requireTolerance(maxRejectedRatio);
        long started = System.currentTimeMillis();
        Files.createDirectories(output);
        PipelineTracker tracker = new PipelineTracker(output, spark);
        PipelineContext context = new PipelineContext(spark, input, output, maxRejectedRatio, TablePublisher.csvAndParquet(output));
        try {
            boolean published = runSteps(context, tracker);
            return PipelineResult.of(published, context, System.currentTimeMillis() - started);
        } catch (Exception error) {
            tracker.failCurrent(error.getMessage());
            throw error;
        } finally {
            context.release();
        }
    }

    private boolean runSteps(PipelineContext context, PipelineTracker tracker) throws Exception {
        for (int index = 0; index < steps.size(); index++) {
            PipelineStep step = steps.get(index);
            tracker.start(step.stage(), step.description());
            StepResult result = step.execute(context);
            if (!result.proceed()) {
                tracker.block(step.stage(), result.rows(), result.detail());
                skipRemaining(tracker, index + 1);
                return false;
            }
            tracker.complete(step.stage(), result.rows(), result.detail());
        }
        return true;
    }

    private void skipRemaining(PipelineTracker tracker, int from) throws Exception {
        for (PipelineStep step : steps.subList(from, steps.size())) {
            tracker.skip(step.stage(), SKIPPED);
        }
    }

    private void requireTolerance(double value) {
        if (!Double.isFinite(value) || value < 0 || value > 1) {
            throw new IllegalArgumentException("La tolerancia de rechazo debe estar entre 0 y 1");
        }
    }
}
