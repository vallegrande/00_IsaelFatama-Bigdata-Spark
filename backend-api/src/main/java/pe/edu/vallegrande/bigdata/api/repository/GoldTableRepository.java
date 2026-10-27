package pe.edu.vallegrande.bigdata.api.repository;

import com.fasterxml.jackson.databind.JsonNode;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface GoldTableRepository {

    Mono<JsonNode> findSummary(String executionId);

    Mono<PageResponse<Map<String, Object>>> findPage(String executionId, String table, PageQuery query);

}
