package pe.edu.vallegrande.bigdata.api.worker;

import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;

public interface WorkerLauncher {

    boolean isAvailable(JobType jobType);

    String unavailableReason(JobType jobType);

    Mono<Integer> launch(WorkerCommand command);

}
