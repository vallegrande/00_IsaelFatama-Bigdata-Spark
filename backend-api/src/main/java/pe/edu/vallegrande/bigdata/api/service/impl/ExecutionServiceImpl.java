package pe.edu.vallegrande.bigdata.api.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pe.edu.vallegrande.bigdata.api.component.ExecutionFinder;
import pe.edu.vallegrande.bigdata.api.component.ExecutionRunner;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.dto.request.CreateExecutionRequest;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionResponse;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.WorkerUnavailableException;
import pe.edu.vallegrande.bigdata.api.mapper.ExecutionMapper;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.model.ExecutionStatus;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import pe.edu.vallegrande.bigdata.api.service.DatasetService;
import pe.edu.vallegrande.bigdata.api.service.ExecutionService;
import pe.edu.vallegrande.bigdata.api.service.JobService;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.JobKind;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class ExecutionServiceImpl implements ExecutionService {

    private final ExecutionRepository executionRepository;

    private final WorkerLauncher workerLauncher;

    private final ExecutionRunner executionRunner;

    private final ExecutionWorkspace executionWorkspace;

    private final ExecutionFinder executionFinder;

    private final JobService jobService;

    private final DatasetService datasetService;

    private final ExecutionMapper executionMapper;

    private final Clock clock;

    @Override
    public Mono<ExecutionResponse> create(CreateExecutionRequest request) {
        return Mono.defer(() -> {
            if (request.jobType() == null || request.jobType().isBlank()) {
                throw new InvalidRequestException("Falta el campo 'jobType'");
            }
            JobType type = jobService.resolve(request.jobType());
            double threshold = resolveThreshold(type, request.threshold());
            validateShape(type, request);
            requireWorker(type);
            String id = executionWorkspace.newId();
            return resolveInput(id, type, request)
                    .flatMap(input -> register(id, type, request, threshold, input));
        }).map(executionMapper::toResponse);
    }

    @Override
    public Mono<ExecutionResponse> findById(String id) {
        return executionFinder.findOrFail(id).map(executionMapper::toResponse);
    }

    @Override
    public Flux<ExecutionResponse> findAll(String jobType, String status) {
        return Flux.defer(() -> {
            JobType type = isBlank(jobType) ? null : jobService.resolve(jobType);
            ExecutionStatus wanted = parseStatus(status);
            return executionRepository.findAll()
                    .filter(execution -> type == null || execution.jobType() == type)
                    .filter(execution -> wanted == null || execution.status() == wanted);
        }).map(executionMapper::toResponse);
    }

    private ExecutionStatus parseStatus(String status) {
        if (isBlank(status)) {
            return null;
        }
        try {
            return ExecutionStatus.valueOf(status.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException error) {
            throw new InvalidRequestException("Estado desconocido: " + status);
        }
    }

    private double resolveThreshold(JobType type, Double value) {
        if (value == null) {
            return type.kind().defaultThreshold();
        }
        if (!Double.isFinite(value) || value < 0 || value > 1) {
            throw new InvalidRequestException("La tolerancia debe estar entre 0 y 1");
        }
        return value;
    }

    private void validateShape(JobType type, CreateExecutionRequest request) {
        boolean hasSource = !isBlank(request.sourceExecutionId());
        boolean hasDataset = !isBlank(request.dataset());
        switch (type.kind().input()) {
            case CSV_FILES -> {
                if (hasSource == hasDataset) {
                    throw new InvalidRequestException("El job " + type + " necesita 'dataset' (carpeta con los 5 CSV) o 'sourceExecutionId' (una extracción), uno de los dos");
                }
            }
            case DATASET -> {
                if (!hasDataset || hasSource) {
                    throw new InvalidRequestException("El job " + type + " necesita solo 'dataset', la carpeta con los 5 CSV");
                }
            }
            case NONE -> {
                if (hasSource || hasDataset) {
                    throw new InvalidRequestException("El job " + type + " no recibe 'dataset' ni 'sourceExecutionId'");
                }
            }
            case PREVIOUS_EXECUTION -> {
                if (!hasSource || hasDataset) {
                    throw new InvalidRequestException("El job " + type + " necesita solo 'sourceExecutionId', la ejecución de la que toma sus datos");
                }
            }
        }
    }

    private void requireWorker(JobType type) {
        if (!workerLauncher.isAvailable(type)) {
            throw new WorkerUnavailableException(workerLauncher.unavailableReason(type));
        }
    }

    private Mono<Path> resolveInput(String id, JobType type, CreateExecutionRequest request) {
        return switch (type.kind().input()) {
            case CSV_FILES -> isBlank(request.sourceExecutionId())
                    ? Mono.fromSupplier(() -> datasetService.resolve(request.dataset()))
                    : landingOf(request.sourceExecutionId());
            case DATASET -> Mono.fromSupplier(() -> datasetService.resolve(request.dataset()));
            case NONE -> Mono.just(executionWorkspace.outputDirectory(id));
            case PREVIOUS_EXECUTION -> published(JobKind.TRANSFORMATION, "una transformación publicada", request.sourceExecutionId())
                    .map(source -> executionWorkspace.outputDirectory(source.id()));
        };
    }

    private Mono<Path> landingOf(String sourceId) {
        return published(JobKind.EXTRACTION, "una extracción publicada", sourceId)
                .map(source -> executionWorkspace.outputDirectory(source.id()).resolve(WorkerProtocol.LANDING));
    }

    private Mono<Execution> published(JobKind kind, String description, String sourceId) {
        return executionFinder.findOrFail(sourceId)
                .filter(source -> source.jobType().kind() == kind && source.isPublished())
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException(
                        "La ejecución " + sourceId + " debe ser " + description)));
    }

    private Mono<Execution> register(String id, JobType type, CreateExecutionRequest request, double threshold, Path input) {
        String sourceId = isBlank(request.sourceExecutionId()) ? null : request.sourceExecutionId();
        Execution pending = Execution.pending(id, type, threshold, sourceId, clock.instant());
        WorkerCommand command = new WorkerCommand(type, input, executionWorkspace.outputDirectory(id), threshold);
        return executionRepository.save(pending).doOnSuccess(saved -> executionRunner.enqueue(saved, command));
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

}
