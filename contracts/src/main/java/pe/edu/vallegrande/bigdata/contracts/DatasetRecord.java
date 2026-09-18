package pe.edu.vallegrande.bigdata.contracts;

import java.time.Instant;

public record DatasetRecord (String id, long rows, String sha256, Instant createdAt) {}
