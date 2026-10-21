package pe.edu.vallegrande.bigdata.spark.core.pipeline;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import pe.edu.vallegrande.bigdata.contracts.StageStatus;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PipelineTracker {
    private final Path events;
    private final SparkContext context;
    private final Map<PipelineStage, Long> startedAt = new EnumMap<>(PipelineStage.class);
    private final Map<PipelineStage, String> startedIso = new EnumMap<>(PipelineStage.class);
    private final Path reports;
    private final String runId;
    private PipelineStage current;

    public PipelineTracker(Path output, SparkSession spark) {
        this.events = output.resolve(WorkerProtocol.EVENTS_FILE);
        this.reports = output.resolve(WorkerProtocol.STAGE_REPORTS);
        this.runId = output.getFileName() == null ? output.toString() : output.getFileName().toString();
        this.context = spark.sparkContext();
    }

    public void start(PipelineStage stage, String detail) throws IOException {
        current = stage;
        startedAt.put(stage, System.currentTimeMillis());
        startedIso.put(stage, Instant.now().toString());
        context.setJobGroup(stage.name(), stage.name() + " - " + detail, false);
        emit(new StageEvent(stage, StageStatus.RUNNING, Instant.now().toString(), null, null, null, detail));
    }

    public void complete(PipelineStage stage, long rows, String detail) throws IOException {
        finish(stage, StageStatus.COMPLETED, rows, detail, Map.of());
    }

    public void complete(PipelineStage stage, StepResult result) throws IOException {
        finish(stage, StageStatus.COMPLETED, result.rows(), result.detail(), result.metrics());
    }

    public void block(PipelineStage stage, long rows, String detail) throws IOException {
        finish(stage, StageStatus.BLOCKED, rows, detail, Map.of());
    }

    public void block(PipelineStage stage, StepResult result) throws IOException {
        finish(stage, StageStatus.BLOCKED, result.rows(), result.detail(), result.metrics());
    }

    public void skip(PipelineStage stage, String detail) throws IOException {
        StageEvent event = new StageEvent(stage, StageStatus.SKIPPED, Instant.now().toString(), null, null, 0, detail);
        emit(event);
        writeReport(event, Map.of());
    }

    public void failCurrent(String detail) {
        if (current == null) {
            return;
        }
        try {
            finish(current, StageStatus.FAILED, null, detail == null ? "La etapa terminó con error" : detail, Map.of());
        } catch (IOException error) {
            System.err.println("No se pudo registrar el fallo de " + current + ": " + error.getMessage());
        }
    }

    private void finish(PipelineStage stage, StageStatus status, Long rows, String detail,
                        Map<String, Object> metrics) throws IOException {
        Long started = startedAt.get(stage);
        Long duration = started == null ? null : System.currentTimeMillis() - started;
        int sparkJobs = context.statusTracker().getJobIdsForGroup(stage.name()).length;
        context.clearJobGroup();
        current = null;
        StageEvent event = new StageEvent(stage, status, Instant.now().toString(), rows, duration, sparkJobs, detail);
        emit(event);
        writeReport(event, metrics);
    }

    private void writeReport(StageEvent event, Map<String, Object> metrics) throws IOException {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("schemaVersion", 1);
        report.put("runId", runId);
        report.put("stage", event.stage().name());
        report.put("status", event.status().name());
        report.put("startedAt", startedIso.get(event.stage()));
        report.put("completedAt", event.timestamp());
        report.put("durationMs", event.durationMs());
        report.put("sparkJobs", event.sparkJobs());
        report.put("rows", event.rows());
        report.put("detail", event.detail());
        report.put("metrics", metrics);
        JsonPublisher.write(reports.resolve(event.stage().name().toLowerCase(java.util.Locale.ROOT) + ".json"), report);
    }

    private void emit(StageEvent event) throws IOException {
        JsonPublisher.appendLine(events, event);
        System.out.printf("[%s] %s - %s%n", event.stage(), event.status(), event.detail());
    }
}
