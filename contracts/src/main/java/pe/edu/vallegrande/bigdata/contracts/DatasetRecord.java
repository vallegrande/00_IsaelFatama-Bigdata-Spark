package pe.edu.vallegrande.bigdata.contracts;

import java.time.Instant;
import java.util.Map;

public record DatasetRecord(String id, String name, DatasetOrigin origin, Map<String, Long> rows, long totalRows,
                            long sizeBytes, String sha256,
                            Instant createdAt) {
}
