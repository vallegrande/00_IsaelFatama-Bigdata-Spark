package pe.edu.vallegrande.bigdata.contracts;

public record StageEvent (
        PipelineStage stage,
        StageStatus status,
        String timestamp,
        Long rows,
        Long durationMs,
        Integer sparkJobs,
        String detail
){
}
