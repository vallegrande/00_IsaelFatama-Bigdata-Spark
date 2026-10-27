package pe.edu.vallegrande.bigdata.api.model;

import lombok.Builder;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.time.Instant;

@Builder(toBuilder = true)
public record Execution(
        String id,
        JobType jobType,
        ExecutionStatus status,
        double threshold,
        String sourceExecutionId,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        Integer exitCode,
        String message) {

    public static Execution pending(String id, JobType jobType, double threshold, String sourceExecutionId, Instant now) {
        return Execution.builder()
                .id(id)
                .jobType(jobType)
                .status(ExecutionStatus.PENDING)
                .threshold(threshold)
                .sourceExecutionId(sourceExecutionId)
                .createdAt(now)
                .build();
    }

    public Execution running(Instant now) {
        return toBuilder()
                .status(ExecutionStatus.RUNNING)
                .startedAt(now)
                .finishedAt(null)
                .exitCode(null)
                .message(null)
                .build();
    }

    public Execution finished(int exitCode, Instant now) {
        ExecutionStatus result = ExecutionStatus.fromExitCode(exitCode);
        String detail = result == ExecutionStatus.FAILED ? "El worker terminó con el código " + exitCode : null;
        return toBuilder().status(result).finishedAt(now).exitCode(exitCode).message(detail).build();
    }

    public Execution failed(String reason, Instant now) {
        return toBuilder().status(ExecutionStatus.FAILED).finishedAt(now).message(reason).build();
    }

    public Execution withMessage(String message) {
        return toBuilder().message(message).build();
    }

    public boolean isPublished() {
        return status == ExecutionStatus.PUBLISHED;
    }

}
