package pe.edu.vallegrande.bigdata.api.component;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.api.config.WorkerProperties;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.model.ExecutionStatus;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionOutputRepository;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import pe.edu.vallegrande.bigdata.contracts.StageStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionRunnerTest {

    private final TestSupport.InMemoryExecutions executions = new TestSupport.InMemoryExecutions();

    private final Map<String, Sinks.One<Integer>> exits = new HashMap<>();

    private Flux<StageEvent> workerEvents = Flux.empty();

    private final ExecutionOutputRepository outputs = new ExecutionOutputRepository() {
        @Override
        public Mono<JsonNode> findResult(String executionId) {
            return Mono.empty();
        }

        @Override
        public Flux<StageEvent> findEvents(String executionId) {
            return workerEvents;
        }

        @Override
        public Mono<JsonNode> findQuality(String executionId) {
            return Mono.empty();
        }

        @Override
        public Mono<JsonNode> findReconciliation(String executionId) {
            return Mono.empty();
        }

        @Override
        public Mono<PageResponse<Map<String, Object>>> findRejected(String executionId, PageQuery query) {
            return Mono.empty();
        }
    };

    private ExecutionRunner runner;

    @AfterEach
    void stop() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    void anExitCodeOfZeroPublishes() {
        start(1);

        submit("a", 0);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.PUBLISHED, status("a")));
        assertEquals(0, executions.get("a").exitCode());
        assertTrue(executions.get("a").startedAt() != null && executions.get("a").finishedAt() != null);
    }

    @Test
    void anExitCodeOfTwoBlocksByQuality() {
        start(1);

        submit("a", 2);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.BLOCKED, status("a")));
    }

    @Test
    void anyOtherExitCodeFails() {
        start(1);

        submit("a", 1);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.FAILED, status("a")));
        assertTrue(executions.get("a").message().contains("código 1"));
    }

    @Test
    void aFailedRunExplainsItselfWithTheDetailOfTheStageThatFailed() {
        workerEvents = Flux.just(
                event(PipelineStage.EXTRACT, StageStatus.RUNNING, "lee"),
                event(PipelineStage.EXTRACT, StageStatus.FAILED, "No se puede leer STAGING.semestres: defina DB_PASSWORD"));
        start(1);

        submit("a", 1);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.FAILED, status("a")));
        assertEquals("No se puede leer STAGING.semestres: defina DB_PASSWORD", executions.get("a").message());
    }

    @Test
    void aBlockedRunExplainsWhichStageBlockedIt() {
        workerEvents = Flux.just(event(PipelineStage.QUALITY_GATE, StageStatus.BLOCKED, "Bloqueado: asistencia supera la tolerancia"));
        start(1);

        submit("a", 2);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.BLOCKED, status("a")));
        assertEquals("Bloqueado: asistencia supera la tolerancia", executions.get("a").message());
    }

    @Test
    void ifTheEventsCannotBeReadTheDefaultMessageIsKept() {
        workerEvents = Flux.error(new IllegalStateException("events.jsonl dañado"));
        start(1);

        submit("a", 1);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.FAILED, status("a")));
        assertTrue(executions.get("a").message().contains("código 1"));
    }

    @Test
    void aPublishedRunHasNoMessage() {
        workerEvents = Flux.just(event(PipelineStage.EXPORT, StageStatus.COMPLETED, "listo"));
        start(1);

        submit("a", 0);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.PUBLISHED, status("a")));
        assertNull(executions.get("a").message());
    }

    @Test
    void aLauncherErrorFailsTheExecutionWithItsMessage() {
        runner = runner(launcher(command -> Mono.error(new IllegalStateException("no arrancó la JVM"))), 1);

        runner.enqueue(register("a"), command("a"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.FAILED, status("a")));
        assertEquals("no arrancó la JVM", executions.get("a").message());
    }

    @Test
    void aFailureDoesNotStopTheQueue() {
        runner = runner(launcher(command -> command.output().getFileName().toString().equals("a")
                ? Mono.error(new IllegalStateException("falla"))
                : Mono.just(0)), 1);

        runner.enqueue(register("a"), command("a"));
        runner.enqueue(register("b"), command("b"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.PUBLISHED, status("b")));
        assertEquals(ExecutionStatus.FAILED, status("a"));
    }

    @Test
    void executionsWaitInThePendingStateWhenAllWorkersAreBusy() {
        start(1);
        exits.put("a", Sinks.one());
        exits.put("b", Sinks.one());

        runner.enqueue(register("a"), command("a"));
        runner.enqueue(register("b"), command("b"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.RUNNING, status("a")));
        assertEquals(ExecutionStatus.PENDING, status("b"));

        exits.get("a").tryEmitValue(0);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.RUNNING, status("b")));
        assertEquals(ExecutionStatus.PUBLISHED, status("a"));
        exits.get("b").tryEmitValue(0);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(ExecutionStatus.PUBLISHED, status("b")));
    }

    @Test
    void severalWorkersRunAtTheSameTimeWhenTheLimitAllowsIt() {
        start(2);
        exits.put("a", Sinks.one());
        exits.put("b", Sinks.one());

        runner.enqueue(register("a"), command("a"));
        runner.enqueue(register("b"), command("b"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(ExecutionStatus.RUNNING, status("a"));
            assertEquals(ExecutionStatus.RUNNING, status("b"));
        });
        exits.get("a").tryEmitValue(0);
        exits.get("b").tryEmitValue(0);
    }

    private void start(int maxConcurrent) {
        runner = runner(launcher(command -> {
            String id = command.output().getFileName().toString();
            Sinks.One<Integer> controlled = exits.get(id);
            return controlled != null ? controlled.asMono() : Mono.just(Integer.parseInt(command.input().getFileName().toString()));
        }), maxConcurrent);
    }

    private ExecutionRunner runner(WorkerLauncher launcher, int maxConcurrent) {
        ExecutionRunner created = new ExecutionRunner(executions, outputs, launcher, properties(maxConcurrent),
                Clock.fixed(TestSupport.NOW, ZoneOffset.UTC));
        created.start();
        return created;
    }

    private void submit(String id, int exitCode) {
        runner.enqueue(register(id), new WorkerCommand(JobType.ACADEMIC_PERFORMANCE, Path.of(Integer.toString(exitCode)), Path.of(id), 0.05));
    }

    private Execution register(String id) {
        Execution pending = TestSupport.pending(id, JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW);
        executions.save(pending).block();
        return pending;
    }

    private WorkerCommand command(String id) {
        return new WorkerCommand(JobType.ACADEMIC_PERFORMANCE, Path.of("0"), Path.of(id), 0.05);
    }

    private StageEvent event(PipelineStage stage, StageStatus status, String detail) {
        return new StageEvent(stage, status, TestSupport.NOW.toString(), null, null, null, detail);
    }

    private ExecutionStatus status(String id) {
        return executions.get(id).status();
    }

    private WorkerProperties properties(int maxConcurrent) {
        return new WorkerProperties(Path.of("spark"), "java", maxConcurrent, Duration.ofMinutes(1), List.of());
    }

    private WorkerLauncher launcher(Function<WorkerCommand, Mono<Integer>> behaviour) {
        return new WorkerLauncher() {
            @Override
            public boolean isAvailable(JobType jobType) {
                return true;
            }

            @Override
            public String unavailableReason(JobType jobType) {
                return "";
            }

            @Override
            public Mono<Integer> launch(WorkerCommand command) {
                return Mono.defer(() -> behaviour.apply(command));
            }
        };
    }

}
