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
import java.util.Map;

public final class PipelineTracker {
    private final Path events;
    private final SparkContext context;
    private final Map<PipelineStage, Long> startedAt = new EnumMap<>(PipelineStage.class);
    private PipelineStage current;

    public PipelineTracker(Path output, SparkSession spark) {
        this.events = output.resolve(WorkerProtocol.EVENTS_FILE);
        this.context = spark.sparkContext();
    }

    public void start(PipelineStage stage, String detail) throws IOException {
        current = stage;
        startedAt.put(stage, System.currentTimeMillis());
        context.setJobGroup(stage.name(), stage.name() + " - " + detail, false);
        emit(new StageEvent(stage, StageStatus.RUNNING, Instant.now().toString(), null, null, null, detail));
    }

    public void complete(PipelineStage stage, long rows, String detail) throws IOException {
        finish(stage, StageStatus.COMPLETED, rows, detail);
    }

    public void block(PipelineStage stage, long rows, String detail) throws IOException {
        finish(stage, StageStatus.BLOCKED, rows, detail);
    }

    public void skip(PipelineStage stage, String detail) throws IOException {
        emit(new StageEvent(stage, StageStatus.SKIPPED, Instant.now().toString(), null, null, 0, detail));
    }

    public void failCurrent(String detail) {
        if (current == null) {
            return;
        }
        try {
            finish(current, StageStatus.FAILED, null, detail == null ? "La etapa terminó con error" : detail);
        } catch (IOException error) {
            System.err.println("No se pudo registrar el fallo de " + current + ": " + error.getMessage());
        }
    }

    private void finish(PipelineStage stage, StageStatus status, Long rows, String detail) throws IOException {
        Long started = startedAt.get(stage);
        Long duration = started == null ? null : System.currentTimeMillis() - started;
        int sparkJobs = context.statusTracker().getJobIdsForGroup(stage.name()).length;
        context.clearJobGroup();
        current = null;
        emit(new StageEvent(stage, status, Instant.now().toString(), rows, duration, sparkJobs, detail));
    }

    private void emit(StageEvent event) throws IOException {
        JsonPublisher.appendLine(events, event);
        System.out.printf("[%s] %s - %s%n", event.stage(), event.status(), event.detail());
    }
}
