package pe.edu.vallegrande.bigdata.api.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record ServingSnapshot(String batchId, Instant loadedAt, JsonNode summary) {
}
