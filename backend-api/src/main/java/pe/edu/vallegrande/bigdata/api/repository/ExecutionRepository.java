package pe.edu.vallegrande.bigdata.api.repository;

import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ExecutionRepository {

    Mono<Execution> save(Execution execution);

    Mono<Execution> findById(String id);

    Flux<Execution> findAll();

    default Mono<Execution> findLatestPublished(JobType jobType) {
        return findAll().filter(execution -> execution.jobType() == jobType && execution.isPublished()).next();
    }

}
