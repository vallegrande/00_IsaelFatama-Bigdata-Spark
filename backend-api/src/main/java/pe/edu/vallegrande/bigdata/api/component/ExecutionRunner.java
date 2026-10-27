package pe.edu.vallegrande.bigdata.api.component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.config.WorkerProperties;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionOutputRepository;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import pe.edu.vallegrande.bigdata.contracts.StageStatus;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Clock;
import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionRunner {

    private record Task(Execution execution, WorkerCommand command) {
    }

    private final ExecutionRepository executionRepository;

    private final ExecutionOutputRepository outputRepository;

    private final WorkerLauncher workerLauncher;

    private final WorkerProperties properties;

    private final Clock clock;

    private final Sinks.Many<Task> queue = Sinks.many().unicast().onBackpressureBuffer();

    private Disposable subscription;

    @PostConstruct
    void start() {
        subscription = queue.asFlux()
                .flatMap(this::run, properties.maxConcurrent())
                .subscribe();
    }

    @PreDestroy
    void stop() {
        queue.tryEmitComplete();
        if (subscription != null) {
            subscription.dispose();
        }
    }

    public void enqueue(Execution execution, WorkerCommand command) {
        queue.emitNext(new Task(execution, command), Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
    }

    private Mono<Void> run(Task task) {
        return executionRepository.save(task.execution().running(clock.instant()))
                .flatMap(running -> workerLauncher.launch(task.command())
                        .flatMap(exitCode -> finish(running, exitCode).flatMap(executionRepository::save))
                        .onErrorResume(error -> executionRepository.save(running.failed(describe(error), clock.instant()))))
                .doOnNext(finished -> log.info("Ejecución {} ({}) terminó en estado {}",
                        finished.id(), finished.jobType(), finished.status()))
                .onErrorResume(error -> {
                    log.error("No se pudo registrar el estado de la ejecución {}", task.execution().id(), error);
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Execution> finish(Execution running, int exitCode) {
        Execution finished = running.finished(exitCode, clock.instant());
        if (finished.isPublished()) {
            return Mono.just(finished);
        }
        return outputRepository.findEvents(finished.id())
                .filter(event -> event.status() == StageStatus.FAILED || event.status() == StageStatus.BLOCKED)
                .filter(event -> event.detail() != null)
                .map(StageEvent::detail)
                .collectList()
                .map(details -> details.isEmpty() ? finished : finished.withMessage(details.get(details.size() - 1)))
                .onErrorReturn(finished);
    }

    private String describe(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

}
