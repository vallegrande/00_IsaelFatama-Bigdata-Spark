package pe.edu.vallegrande.bigdata.api.model;

import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;

public enum ExecutionStatus {

    PENDING,
    RUNNING,
    PUBLISHED,
    BLOCKED,
    FAILED;

    public static ExecutionStatus fromExitCode(int exitCode) {
        return switch (exitCode) {
            case WorkerProtocol.EXIT_SUCCEEDED -> PUBLISHED;
            case WorkerProtocol.EXIT_QUALITY_FAILED -> BLOCKED;
            default -> FAILED;
        };
    }

    public boolean isFinished() {
        return this != PENDING && this != RUNNING;
    }

}
