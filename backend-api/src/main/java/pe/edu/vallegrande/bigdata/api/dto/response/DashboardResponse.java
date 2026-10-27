package pe.edu.vallegrande.bigdata.api.dto.response;

import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.time.Instant;

public record DashboardResponse<T>(String executionId, JobType jobType, Instant publishedAt, T data) {
}
