package pe.edu.vallegrande.bigdata.api.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.DashboardResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.model.ServingSnapshot;
import pe.edu.vallegrande.bigdata.api.repository.ServingRepository;
import pe.edu.vallegrande.bigdata.api.service.DashboardService;
import pe.edu.vallegrande.bigdata.api.service.JobService;
import pe.edu.vallegrande.bigdata.contracts.JobKind;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class DashboardServiceImpl implements DashboardService {

    private final JobService jobService;

    private final ServingRepository servingRepository;

    @Override
    public List<String> findTables(String jobType) {
        return dashboardJob(jobType).goldTables();
    }

    @Override
    public Mono<DashboardResponse<JsonNode>> findSummary(String jobType) {
        return Mono.defer(() -> {
            JobType type = dashboardJob(jobType);
            return current(type).map(snapshot -> toResponse(snapshot, type, snapshot.summary()));
        });
    }

    @Override
    public Mono<DashboardResponse<PageResponse<Map<String, Object>>>> findTable(String jobType, String table, PageQuery query) {
        return Mono.defer(() -> {
            JobType type = dashboardJob(jobType);
            if (!type.goldTables().contains(table)) {
                throw new ResourceNotFoundException("El job " + type + " no produce la tabla '" + table + "'");
            }
            return current(type).flatMap(snapshot -> servingRepository.findPage(snapshot.batchId(), table, query)
                    .map(page -> toResponse(snapshot, type, page)));
        });
    }

    private JobType dashboardJob(String name) {
        JobType type = jobService.resolve(name);
        if (type.kind() != JobKind.TRANSFORMATION) {
            throw new InvalidRequestException("El job " + type + " no produce tablas para el dashboard");
        }
        return type;
    }

    private Mono<ServingSnapshot> current(JobType type) {
        return servingRepository.findCurrent(type)
                .switchIfEmpty(Mono.error(() -> new ResourceNotFoundException(
                        "Aún no hay datos cargados en la base de " + type.label() + ": ejecute la carga (" + JobType.DATA_LOAD + ")")));
    }

    private <T> DashboardResponse<T> toResponse(ServingSnapshot snapshot, JobType type, T data) {
        return new DashboardResponse<>(snapshot.batchId(), type, snapshot.loadedAt(), data);
    }

}
