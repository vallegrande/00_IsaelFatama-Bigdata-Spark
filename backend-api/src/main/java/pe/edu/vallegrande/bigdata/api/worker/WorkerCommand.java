package pe.edu.vallegrande.bigdata.api.worker;

import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.nio.file.Path;

public record WorkerCommand(JobType jobType, Path input, Path output, double threshold) {
}
