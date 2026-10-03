package pe.edu.vallegrande.bigdata.spark.core.pipeline;

public record PipelineResult(boolean published, long received, long accepted, long rejected, double rejectedRatio,
                             double maxRejectedRatio, long goldRows, long durationMs) {

    public static PipelineResult of(boolean published, PipelineContext context, long durationMs) {
        var validation = context.validation();
        long received = validation == null ? 0 : validation.received();
        long rejected = validation == null ? 0 : validation.rejectedCount();
        double ratio = received == 0 ? 0 : Math.round((double) rejected / received * 1_000_000d) / 1_000_000d;
        long goldRows = published && context.gold() != null ? context.gold().primaryTable().count() : 0;
        return new PipelineResult(published, received, received - rejected, rejected, ratio, context.maxRejectedRatio(),
                goldRows, durationMs);
    }
}
