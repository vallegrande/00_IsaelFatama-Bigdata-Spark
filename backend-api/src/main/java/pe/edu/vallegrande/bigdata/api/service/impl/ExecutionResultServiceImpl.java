package pe.edu.vallegrande.bigdata.api.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pe.edu.vallegrande.bigdata.api.component.ExecutionFinder;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionDetailResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.mapper.ExecutionMapper;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionOutputRepository;
import pe.edu.vallegrande.bigdata.api.repository.GoldTableRepository;
import pe.edu.vallegrande.bigdata.api.service.ExecutionResultService;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ExecutionResultServiceImpl implements ExecutionResultService {

    private final ExecutionFinder executionFinder;

    private final ExecutionOutputRepository outputRepository;

    private final GoldTableRepository goldTableRepository;

    private final ExecutionMapper executionMapper;

    @Override
    public Mono<ExecutionDetailResponse> findDetail(String id) {
        return executionFinder.findOrFail(id)
                .flatMap(execution -> outputRepository.findResult(id)
                        .map(Optional::of)
                        .defaultIfEmpty(Optional.empty())
                        .map(result -> executionMapper.toDetail(execution, result.orElse(null))));
    }

    @Override
    public Flux<StageEvent> findEvents(String id) {
        return executionFinder.findOrFail(id).flatMapMany(execution -> outputRepository.findEvents(id));
    }

    @Override
    public Mono<JsonNode> findQuality(String id) {
        return executionFinder.findOrFail(id)
                .then(Mono.defer(() -> outputRepository.findQuality(id)))
                .switchIfEmpty(notYet("el reporte de calidad", id));
    }

    @Override
    public Mono<JsonNode> findReconciliation(String id) {
        return executionFinder.findOrFail(id)
                .then(Mono.defer(() -> outputRepository.findReconciliation(id)))
                .switchIfEmpty(notYet("el reporte de conciliación", id));
    }

    @Override
    public Mono<PageResponse<Map<String, Object>>> findRejected(String id, PageQuery query) {
        return executionFinder.findOrFail(id)
                .then(Mono.defer(() -> outputRepository.findRejected(id, query)))
                .switchIfEmpty(notYet("las filas en cuarentena", id));
    }

    @Override
    public Mono<JsonNode> findGoldSummary(String id) {
        return executionFinder.findOrFail(id)
                .flatMap(this::requirePublished)
                .flatMap(execution -> goldTableRepository.findSummary(id))
                .switchIfEmpty(notYet("el resumen Gold", id));
    }

    @Override
    public Mono<PageResponse<Map<String, Object>>> findGoldTable(String id, String table, PageQuery query) {
        return executionFinder.findOrFail(id)
                .flatMap(this::requirePublished)
                .flatMap(execution -> {
                    if (!execution.jobType().goldTables().contains(table)) {
                        return Mono.error(new ResourceNotFoundException(
                                "El job " + execution.jobType() + " no produce la tabla '" + table + "'"));
                    }
                    return goldTableRepository.findPage(id, table, query);
                })
                .switchIfEmpty(notYet("la tabla " + table, id));
    }

    private Mono<Execution> requirePublished(Execution execution) {
        return execution.isPublished()
                ? Mono.just(execution)
                : Mono.error(new ResourceNotFoundException(
                        "La ejecución " + execution.id() + " está " + execution.status() + " y no tiene datos Gold"));
    }

    private <T> Mono<T> notYet(String what, String id) {
        return Mono.error(() -> new ResourceNotFoundException("La ejecución " + id + " aún no tiene " + what));
    }

}
