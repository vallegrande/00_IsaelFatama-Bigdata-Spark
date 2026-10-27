package pe.edu.vallegrande.bigdata.api.dto.response;

import com.fasterxml.jackson.databind.JsonNode;

public record ExecutionDetailResponse(ExecutionResponse execution, JsonNode result) {
}
