package pe.edu.vallegrande.bigdata.api.repository.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.repository.GoldTableRepository;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;

@Repository
@RequiredArgsConstructor
class FileGoldTableRepository implements GoldTableRepository {

    private static final Pattern TABLE_NAME = Pattern.compile("[a-z0-9_]+");

    private final ExecutionWorkspace workspace;

    private final ObjectMapper mapper;

    @Override
    public Mono<JsonNode> findSummary(String executionId) {
        return FileSupport.readJson(mapper, workspace.outputDirectory(executionId).resolve(WorkerProtocol.SUMMARY_FILE));
    }

    @Override
    public Mono<PageResponse<Map<String, Object>>> findPage(String executionId, String table, PageQuery query) {
        if (!TABLE_NAME.matcher(table).matches()) {
            return Mono.empty();
        }
        Path file = workspace.outputDirectory(executionId).resolve(WorkerProtocol.GOLD).resolve(table + ".csv");
        return FileSupport.blocking(() -> Files.isRegularFile(file) ? CsvPageReader.read(file, query) : null);
    }

}
