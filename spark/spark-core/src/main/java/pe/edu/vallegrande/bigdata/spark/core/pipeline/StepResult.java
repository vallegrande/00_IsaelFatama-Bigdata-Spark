package pe.edu.vallegrande.bigdata.spark.core.pipeline;

public record StepResult(long rows, String detail, boolean proceed) {

    public static StepResult completed(long rows, String detail) {
        return new StepResult(rows, detail, true);
    }

    public static StepResult blocked(long rows, String detail) {
        return new StepResult(rows, detail, false);
    }
}
