package pe.edu.vallegrande.bigdata.contracts;

import java.time.Instant;

public record JobRecord(String id, String datasetId, String datasetName, JobStatus status, double maxRejectedRatio,
                        int sparkUiSeconds,
                        Instant createdAt,
                        Instant startedAt, Instant finishedAt, String message) {


    public JobRecord transition(JobStatus next, String detail) {
        return new JobRecord(id, datasetId, datasetName, next, maxRejectedRatio, sparkUiSeconds, createdAt,
                next == JobStatus.RUNNING ? Instant.now() : startedAt,
                next.terminal() ? Instant.now() : null, detail
        );
    }
}

/**
 * JOB-001
 * │
 * ├── datasetId          DATASET-01
 * ├── status             PENDING
 * ├── maxRejectedRatio   0.05
 * ├── createdAt          10:00
 * ├── startedAt          null
 * ├── finishedAt         null
 * └── message            Esperando procesamiento
 */
