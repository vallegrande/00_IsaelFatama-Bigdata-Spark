package pe.edu.vallegrande.bigdata.api.repository;

import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.model.ServingSnapshot;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface ServingRepository {

    Mono<ServingSnapshot> findCurrent(JobType type);

    Mono<PageResponse<Map<String, Object>>> findPage(String batchId, String table, PageQuery query);

}
