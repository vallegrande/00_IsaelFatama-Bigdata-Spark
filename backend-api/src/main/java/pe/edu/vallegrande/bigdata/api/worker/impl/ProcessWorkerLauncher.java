package pe.edu.vallegrande.bigdata.api.worker.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.config.WorkerProperties;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ProcessWorkerLauncher implements WorkerLauncher {

    private final WorkerProperties properties;

    private final Path modulesDirectory;

    public ProcessWorkerLauncher(WorkerProperties properties) {
        this.properties = properties;
        this.modulesDirectory = properties.modulesDirectory().toAbsolutePath().normalize();
    }

    @Override
    public boolean isAvailable(JobType jobType) {
        return Files.isRegularFile(jarOf(jobType));
    }

    @Override
    public String unavailableReason(JobType jobType) {
        return "No se encontró " + jarOf(jobType) + ". Compile el módulo con: mvn -pl spark/" + jobType.module() + " -am package";
    }

    @Override
    public Mono<Integer> launch(WorkerCommand command) {
        return Mono.fromCallable(() -> run(command)).subscribeOn(Schedulers.boundedElastic());
    }

    List<String> commandLine(WorkerCommand command) {
        List<String> line = new ArrayList<>();
        line.add(properties.javaCommand());
        line.addAll(properties.jvmOptions());
        line.add("-jar");
        line.add(jarOf(command.jobType()).toString());
        line.add(command.input().toAbsolutePath().toString());
        line.add(command.output().toAbsolutePath().toString());
        line.add(Double.toString(command.threshold()));
        return line;
    }

    private Path jarOf(JobType jobType) {
        return modulesDirectory.resolve(jobType.module()).resolve("target").resolve(jobType.jar());
    }

    private int run(WorkerCommand command) throws IOException, InterruptedException {
        Files.createDirectories(command.output());
        File logFile = command.output().resolve(WorkerProtocol.LOG_FILE).toFile();
        ProcessBuilder builder = new ProcessBuilder(commandLine(command))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
        log.info("Lanzando {} en {}", command.jobType(), command.output().getFileName());
        Process process = builder.start();
        try {
            if (!process.waitFor(properties.timeout().toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("El worker superó el tiempo máximo de " + properties.timeout().toMinutes() + " minutos");
            }
        } catch (InterruptedException interrupted) {
            process.destroyForcibly();
            throw interrupted;
        }
        log.info("{} terminó con código {}", command.output().getFileName(), process.exitValue());
        return process.exitValue();
    }

}
