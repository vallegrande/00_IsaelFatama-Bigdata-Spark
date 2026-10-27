package pe.edu.vallegrande.bigdata.api.component;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class ExecutionFinder {

    private final ExecutionRepository executionRepository;

    public Mono<Execution> findOrFail(String id) {
        return executionRepository.findById(id)
                .switchIfEmpty(Mono.error(() -> new ResourceNotFoundException("No existe la ejecución " + id)));
    }

}
