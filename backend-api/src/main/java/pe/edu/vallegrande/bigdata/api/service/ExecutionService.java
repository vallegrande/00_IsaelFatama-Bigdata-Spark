package pe.edu.vallegrande.bigdata.api.service;

import pe.edu.vallegrande.bigdata.api.dto.request.CreateExecutionRequest;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ExecutionService {

    Mono<ExecutionResponse> create(CreateExecutionRequest request);

    Mono<ExecutionResponse> findById(String id);

    Flux<ExecutionResponse> findAll(String jobType, String status);

}
