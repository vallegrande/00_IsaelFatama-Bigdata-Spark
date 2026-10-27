package pe.edu.vallegrande.bigdata.api.dto.request;

public record CreateExecutionRequest(String jobType, Double threshold, String sourceExecutionId, String dataset) {
}
