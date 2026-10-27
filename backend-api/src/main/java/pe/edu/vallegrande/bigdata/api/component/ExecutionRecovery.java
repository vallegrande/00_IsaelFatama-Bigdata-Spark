package pe.edu.vallegrande.bigdata.api.component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
class ExecutionRecovery implements ApplicationRunner {

    private final ExecutionRepository executionRepository;

    private final Clock clock;

    @Override
    public void run(ApplicationArguments args) {
        Long recovered = executionRepository.findAll()
                .filter(execution -> !execution.status().isFinished())
                .flatMap(execution -> executionRepository.save(
                        execution.failed("Interrumpida: el servicio se reinició antes de que terminara", clock.instant())))
                .count()
                .onErrorResume(error -> {
                    log.warn("No se pudo revisar las ejecuciones pendientes: {}", error.getMessage());
                    return Mono.just(0L);
                })
                .block(Duration.ofSeconds(30));
        if (recovered != null && recovered > 0) {
            log.warn("{} ejecuciones interrumpidas se marcaron como fallidas", recovered);
        }
    }

}
