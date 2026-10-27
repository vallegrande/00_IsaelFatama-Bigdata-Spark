package pe.edu.vallegrande.bigdata.api.dto.response;

import lombok.Builder;
import pe.edu.vallegrande.bigdata.api.model.ExecutionStatus;
import pe.edu.vallegrande.bigdata.contracts.JobKind;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.time.Instant;

@Builder
public record ExecutionResponse(
        String id,
        JobType jobType,
        String jobLabel,
        JobKind kind,
        ExecutionStatus status,
        double threshold,
        String sourceExecutionId,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        Long durationMs,
        Integer exitCode,
        String message) {
}
