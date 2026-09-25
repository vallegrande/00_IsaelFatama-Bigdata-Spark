package pe.edu.vallegrande.bigdata.worker.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public record WorkerConfig(String appName, String master, int shufflePartitions, String driverHost, String timeZone,
                           boolean uiEnabled, int uiPort, boolean eventLogEnabled, Path eventLogDirectory) {

    private static final String FILE = "application.yaml";

    public static WorkerConfig load() {
        try (InputStream stream = WorkerConfig.class.getClassLoader().getResourceAsStream(FILE)) {
            if (stream == null) {
                throw new IllegalStateException("No se encontrol " + FILE + "en la carpeta resources");
            }

            JsonNode worker = new ObjectMapper(new YAMLFactory()).readTree(stream).path("worker");
            JsonNode eventLog = worker.path("event-log");
            return new WorkerConfig(
                    worker.path("app-name").asText("Vallegrande Academic Pipeline"),
                    System.getProperty("worker.master", worker.path("master").asText("local[*]")),
                    worker.path("shuffle-partitions").asInt(4),
                    worker.path("driver-host").asText("127.0.0.1"),
                    worker.path("time-zone").asText("UTC"),
                    Boolean.parseBoolean(System.getProperty("worker.ui.enabled",
                            worker.path("ui").path("enabled").asText("true"))),
                    worker.path("ui").path("port").asInt(4040),
                    Boolean.parseBoolean(System.getProperty("worker.event-log.enabled",
                            eventLog.path("enabled").asText("true"))),
                    Path.of(System.getProperty("worker.event-log.directoy", eventLog.path("directory").asText("runtime/spark-events")))
                            .toAbsolutePath().normalize()

            );

        } catch (IOException error) {
            throw new UncheckedIOException("No se pudo leer: + " + FILE, error);
        }
    }

    public WorkerConfig withoutUi() {
        return new WorkerConfig(appName, master, shufflePartitions, driverHost, timeZone, false, uiPort,
                false, eventLogDirectory);
    }

    public WorkerConfig forJob(String jobId) {
        String label = jobId.length() > 8 ? jobId.substring(0, 8) : jobId;
        return new WorkerConfig(appName + " - " + label, master, shufflePartitions, driverHost, timeZone, uiEnabled,
                uiPort, eventLogEnabled, eventLogDirectory);
    }
}
