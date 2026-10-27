package pe.edu.vallegrande.bigdata.api.rest;

import pe.edu.vallegrande.bigdata.api.constant.ApiPaths;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import pe.edu.vallegrande.bigdata.api.dto.request.CreateExecutionRequest;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionDetailResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.service.ExecutionResultService;
import pe.edu.vallegrande.bigdata.api.service.ExecutionService;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping(ApiPaths.EXECUTIONS)
@RequiredArgsConstructor
public class ExecutionRest {

    private final ExecutionService executionService;

    private final ExecutionResultService executionResultService;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<ExecutionResponse>> create(@RequestBody CreateExecutionRequest request) {
        return executionService.create(request)
                .map(response -> ResponseEntity.accepted()
                        .location(URI.create(ApiPaths.EXECUTIONS + "/" + response.id()))
                        .body(response));
    }

    @GetMapping
    public Flux<ExecutionResponse> findAll(@RequestParam(required = false) String jobType,
                                           @RequestParam(required = false) String status) {
        return executionService.findAll(jobType, status);
    }

    @GetMapping(ApiPaths.BY_ID)
    public Mono<ExecutionDetailResponse> findById(@PathVariable String id) {
        return executionResultService.findDetail(id);
    }

    @GetMapping(ApiPaths.EVENTS)
    public Flux<StageEvent> findEvents(@PathVariable String id) {
        return executionResultService.findEvents(id);
    }

    @GetMapping(ApiPaths.QUALITY)
    public Mono<JsonNode> findQuality(@PathVariable String id) {
        return executionResultService.findQuality(id);
    }

    @GetMapping(ApiPaths.RECONCILIATION)
    public Mono<JsonNode> findReconciliation(@PathVariable String id) {
        return executionResultService.findReconciliation(id);
    }

    @GetMapping(ApiPaths.REJECTED)
    public Mono<PageResponse<Map<String, Object>>> findRejected(@PathVariable String id,
                                                                @RequestParam(required = false) Integer page,
                                                                @RequestParam(required = false) Integer size) {
        return Mono.fromSupplier(() -> PageQuery.of(page, size))
                .flatMap(query -> executionResultService.findRejected(id, query));
    }

    @GetMapping(ApiPaths.GOLD_SUMMARY)
    public Mono<JsonNode> findGoldSummary(@PathVariable String id) {
        return executionResultService.findGoldSummary(id);
    }

    @GetMapping(ApiPaths.GOLD_TABLE)
    public Mono<PageResponse<Map<String, Object>>> findGoldTable(@PathVariable String id, @PathVariable String table,
                                                                 @RequestParam(required = false) Integer page,
                                                                 @RequestParam(required = false) Integer size) {
        return Mono.fromSupplier(() -> PageQuery.of(page, size))
                .flatMap(query -> executionResultService.findGoldTable(id, table, query));
    }

}
