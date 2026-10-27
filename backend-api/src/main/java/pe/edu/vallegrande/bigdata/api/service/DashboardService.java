package pe.edu.vallegrande.bigdata.api.service;

import com.fasterxml.jackson.databind.JsonNode;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.DashboardResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

public interface DashboardService {

    List<String> findTables(String jobType);

    Mono<DashboardResponse<JsonNode>> findSummary(String jobType);

    Mono<DashboardResponse<PageResponse<Map<String, Object>>>> findTable(String jobType, String table, PageQuery query);

}
