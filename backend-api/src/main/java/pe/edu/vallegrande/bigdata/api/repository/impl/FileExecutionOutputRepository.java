package pe.edu.vallegrande.bigdata.api.repository.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionOutputRepository;
import pe.edu.vallegrande.bigdata.contracts.StageEvent;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
class FileExecutionOutputRepository implements ExecutionOutputRepository {

    private final ExecutionWorkspace workspace;

    private final ObjectMapper mapper;

    @Override
    public Mono<JsonNode> findResult(String executionId) {
        return FileSupport.readJson(mapper, workspace.outputDirectory(executionId).resolve(WorkerProtocol.RESULT_FILE));
    }

    @Override
    public Flux<StageEvent> findEvents(String executionId) {
        Path file = workspace.outputDirectory(executionId).resolve(WorkerProtocol.EVENTS_FILE);
        return FileSupport.blocking(() -> readEvents(file)).flatMapMany(Flux::fromIterable);
    }

    @Override
    public Mono<JsonNode> findQuality(String executionId) {
        return FileSupport.readJson(mapper, workspace.outputDirectory(executionId).resolve(WorkerProtocol.QUALITY_FILE));
    }

    @Override
    public Mono<JsonNode> findReconciliation(String executionId) {
        return FileSupport.readJson(mapper, workspace.outputDirectory(executionId).resolve(WorkerProtocol.RECONCILIATION_FILE));
    }

    @Override
    public Mono<PageResponse<Map<String, Object>>> findRejected(String executionId, PageQuery query) {
        Path file = workspace.outputDirectory(executionId)
                .resolve(WorkerProtocol.QUARANTINE)
                .resolve(WorkerProtocol.REJECTED_TABLE + ".csv");
        return FileSupport.blocking(() -> Files.isRegularFile(file) ? CsvPageReader.read(file, query) : null);
    }

    private List<StageEvent> readEvents(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            return null;
        }
        List<StageEvent> events = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                events.add(mapper.readValue(line, StageEvent.class));
            }
        }
        return events;
    }

}
