package pe.edu.vallegrande.bigdata.spark.core.pipeline;

import java.util.Map;

public record StepResult(long rows, String detail, boolean proceed, Map<String, Object> metrics) {

    public StepResult(long rows, String detail, boolean proceed) {
        this(rows, detail, proceed, Map.of());
    }

    public StepResult {
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
    }

    public static StepResult completed(long rows, String detail) {
        return completed(rows, detail, Map.of());
    }

    public static StepResult completed(long rows, String detail, Map<String, Object> metrics) {
        return new StepResult(rows, detail, true, metrics);
    }

    public static StepResult blocked(long rows, String detail) {
        return blocked(rows, detail, Map.of());
    }

    public static StepResult blocked(long rows, String detail, Map<String, Object> metrics) {
        return new StepResult(rows, detail, false, metrics);
    }
}
