package pe.edu.vallegrande.bigdata.api.worker.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.api.config.WorkerProperties;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessWorkerLauncherTest {

    @TempDir
    Path modules;

    private ProcessWorkerLauncher launcher() {
        return new ProcessWorkerLauncher(new WorkerProperties(modules, "java", 2, Duration.ofMinutes(5), List.of("--add-opens=java.base/java.lang=ALL-UNNAMED")));
    }

    @Test
    void aWorkerIsAvailableOnlyWhenItsJarIsBuilt() throws IOException {
        ProcessWorkerLauncher launcher = launcher();
        assertFalse(launcher.isAvailable(JobType.ACADEMIC_PERFORMANCE));

        Path jar = modules.resolve("job-academic-performance/target/job-academic-performance.jar");
        Files.createDirectories(jar.getParent());
        Files.createFile(jar);

        assertTrue(launcher.isAvailable(JobType.ACADEMIC_PERFORMANCE));
        assertFalse(launcher.isAvailable(JobType.ATTENDANCE_ALERT));
    }

    @Test
    void theUnavailableReasonSaysHowToBuildTheModule() {
        String reason = launcher().unavailableReason(JobType.DATA_LOAD);

        assertTrue(reason.contains("job-data-load.jar"));
        assertTrue(reason.contains("mvn -pl spark/job-data-load -am package"));
    }

    @Test
    void buildsTheDocumentedCommandLine() {
        WorkerCommand command = new WorkerCommand(JobType.ACADEMIC_PERFORMANCE, Path.of("entrada"), Path.of("salida"), 0.05);

        List<String> line = launcher().commandLine(command);

        assertEquals("java", line.get(0));
        assertEquals("--add-opens=java.base/java.lang=ALL-UNNAMED", line.get(1));
        assertEquals("-jar", line.get(2));
        assertTrue(line.get(3).endsWith(Path.of("job-academic-performance", "target", "job-academic-performance.jar").toString()));
        assertTrue(Path.of(line.get(4)).isAbsolute());
        assertTrue(line.get(4).endsWith("entrada"));
        assertTrue(line.get(5).endsWith("salida"));
        assertEquals("0.05", line.get(6));
    }

    @Test
    void theCommandUsesTheJobsOwnJar() {
        WorkerCommand command = new WorkerCommand(JobType.DATA_RECONCILIATION, Path.of("a"), Path.of("b"), 0);

        assertTrue(launcher().commandLine(command).contains(
                modules.toAbsolutePath().normalize().resolve("job-data-reconciliation/target/job-data-reconciliation.jar").toString()));
    }

}
