package pe.edu.vallegrande.bigdata.api.service;

import com.fasterxml.jackson.databind.JsonNode;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionDetailResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface ExecutionResultService {

    Mono<ExecutionDetailResponse> findDetail(String id);

    Flux<StageEvent> findEvents(String id);

    Mono<JsonNode> findQuality(String id);

    Mono<JsonNode> findReconciliation(String id);

    Mono<PageResponse<Map<String, Object>>> findRejected(String id, PageQuery query);

    Mono<JsonNode> findGoldSummary(String id);

    Mono<PageResponse<Map<String, Object>>> findGoldTable(String id, String table, PageQuery query);

}
