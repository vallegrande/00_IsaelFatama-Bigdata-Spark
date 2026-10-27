package pe.edu.vallegrande.bigdata.api.repository.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionRepository;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Repository
@RequiredArgsConstructor
class FileExecutionRepository implements ExecutionRepository {

    private final ExecutionWorkspace workspace;

    private final ObjectMapper mapper;

    @Override
    public Mono<Execution> save(Execution execution) {
        return FileSupport.blocking(() -> {
            byte[] content = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(execution);
            FileSupport.writeAtomically(workspace.executionFile(execution.id()), content);
            return execution;
        });
    }

    @Override
    public Mono<Execution> findById(String id) {
        return FileSupport.blocking(() -> read(workspace.executionFile(id)));
    }

    @Override
    public Flux<Execution> findAll() {
        return FileSupport.blocking(this::readAll).flatMapMany(Flux::fromIterable);
    }

    private List<Execution> readAll() throws IOException {
        Path jobs = workspace.jobsDirectory();
        if (!Files.isDirectory(jobs)) {
            return List.of();
        }
        try (Stream<Path> directories = Files.list(jobs)) {
            return directories
                    .map(directory -> directory.resolve(WorkerProtocol.EXECUTION_FILE))
                    .map(this::read)
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparing(Execution::createdAt).reversed())
                    .toList();
        }
    }

    private Execution read(Path file) {
        if (!Files.isRegularFile(file)) {
            return null;
        }
        try {
            return mapper.readValue(file.toFile(), Execution.class);
        } catch (IOException error) {
            log.warn("Se ignora {} porque no se pudo leer: {}", file, error.getMessage());
            return null;
        }
    }

}
