package pe.edu.vallegrande.bigdata.api.rest;

import pe.edu.vallegrande.bigdata.api.constant.ApiPaths;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.DashboardResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.service.DashboardService;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(ApiPaths.DASHBOARD)
@RequiredArgsConstructor
public class DashboardRest {

    private final DashboardService dashboardService;

    @GetMapping(ApiPaths.TABLES)
    public Mono<List<String>> findTables(@PathVariable String jobType) {
        return Mono.fromSupplier(() -> dashboardService.findTables(jobType));
    }

    @GetMapping(ApiPaths.SUMMARY)
    public Mono<DashboardResponse<JsonNode>> findSummary(@PathVariable String jobType) {
        return dashboardService.findSummary(jobType);
    }

    @GetMapping(ApiPaths.TABLE)
    public Mono<DashboardResponse<PageResponse<Map<String, Object>>>> findTable(@PathVariable String jobType,
                                                                               @PathVariable String table,
                                                                               @RequestParam(required = false) Integer page,
                                                                               @RequestParam(required = false) Integer size) {
        return Mono.fromSupplier(() -> PageQuery.of(page, size))
                .flatMap(query -> dashboardService.findTable(jobType, table, query));
    }

}
