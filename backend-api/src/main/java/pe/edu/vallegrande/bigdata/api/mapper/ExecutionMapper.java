package pe.edu.vallegrande.bigdata.api.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionDetailResponse;
import pe.edu.vallegrande.bigdata.api.dto.response.ExecutionResponse;
import pe.edu.vallegrande.bigdata.api.model.Execution;

import java.time.Duration;

@Component
public class ExecutionMapper {

    public ExecutionResponse toResponse(Execution execution) {
        Long duration = execution.startedAt() != null && execution.finishedAt() != null
                ? Duration.between(execution.startedAt(), execution.finishedAt()).toMillis()
                : null;
        return ExecutionResponse.builder()
                .id(execution.id())
                .jobType(execution.jobType())
                .jobLabel(execution.jobType().label())
                .kind(execution.jobType().kind())
                .status(execution.status())
                .threshold(execution.threshold())
                .sourceExecutionId(execution.sourceExecutionId())
                .createdAt(execution.createdAt())
                .startedAt(execution.startedAt())
                .finishedAt(execution.finishedAt())
                .durationMs(duration)
                .exitCode(execution.exitCode())
                .message(execution.message())
                .build();
    }

    public ExecutionDetailResponse toDetail(Execution execution, JsonNode result) {
        return new ExecutionDetailResponse(toResponse(execution), result);
    }

}
