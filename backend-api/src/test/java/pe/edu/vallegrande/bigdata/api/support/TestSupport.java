package pe.edu.vallegrande.bigdata.api.support;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.config.WorkspaceProperties;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TestSupport {

    public static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    private TestSupport() {
    }

    public static ObjectMapper mapper() {
        return JsonMapper.builder()
                .findAndAddModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    public static ExecutionWorkspace workspace(Path root) {
        return new ExecutionWorkspace(new WorkspaceProperties(root.resolve("jobs"), root.resolve("datasets")));
    }

    public static Execution pending(String id, JobType type, Instant createdAt) {
        return Execution.pending(id, type, 0.05, null, createdAt);
    }

    public static Execution published(String id, JobType type, Instant createdAt) {
        return pending(id, type, createdAt).running(createdAt).finished(0, createdAt);
    }

    public static final class InMemoryExecutions implements ExecutionRepository {

        private final Map<String, Execution> store = new ConcurrentHashMap<>();

        @Override
        public Mono<Execution> save(Execution execution) {
            store.put(execution.id(), execution);
            return Mono.just(execution);
        }

        @Override
        public Mono<Execution> findById(String id) {
            return Mono.justOrEmpty(store.get(id));
        }

        @Override
        public Flux<Execution> findAll() {
            return Flux.fromIterable(store.values()).sort(Comparator.comparing(Execution::createdAt).reversed());
        }

        public Execution get(String id) {
            return store.get(id);
        }

    }

}
