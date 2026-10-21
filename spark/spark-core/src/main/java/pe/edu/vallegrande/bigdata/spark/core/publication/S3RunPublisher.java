package pe.edu.vallegrande.bigdata.spark.core.publication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/** Publishes one completed local Spark run into seven ordered S3 stage prefixes. */
public final class S3RunPublisher implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<StageTarget> TARGETS = List.of(
            new StageTarget(PipelineStage.BRONZE, "01-bronze"),
            new StageTarget(PipelineStage.VALIDATION, "02-validation"),
            new StageTarget(PipelineStage.QUARANTINE, "03-quarantine"),
            new StageTarget(PipelineStage.SILVER, "04-silver"),
            new StageTarget(PipelineStage.QUALITY_GATE, "05-quality-gate"),
            new StageTarget(PipelineStage.GOLD, "06-gold"),
            new StageTarget(PipelineStage.EXPORT, "07-export"));

    private final S3Client client;
    private final String bucket;
    private final String prefix;

    public S3RunPublisher(S3Client client, String bucket, String prefix) {
        this.client = client;
        this.bucket = requireValue(bucket, "bucket");
        this.prefix = normalizePrefix(prefix);
    }

    /** S3 is opt-in so unit tests and local development never require AWS credentials. */
    public static Optional<S3RunPublisher> fromEnvironment() {
        if (!Boolean.parseBoolean(System.getenv().getOrDefault("S3_UPLOAD_ENABLED", "false"))) {
            return Optional.empty();
        }
        String bucket = requireValue(System.getenv("S3_BUCKET"), "S3_BUCKET");
        String region = System.getenv().getOrDefault("AWS_REGION", System.getenv("AWS_DEFAULT_REGION"));
        region = requireValue(region, "AWS_REGION (o AWS_DEFAULT_REGION)");
        String prefix = System.getenv().getOrDefault("S3_PREFIX", "vallegrande");
        S3Client client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        return Optional.of(new S3RunPublisher(client, bucket, prefix));
    }

    public String publish(JobType jobType, Path input, Path output, PipelineResult result) throws IOException {
        String jobId = output.getFileName().toString();
        String runPrefix = join(prefix, "jobs", jobType.name().toLowerCase(Locale.ROOT), jobId);
        Map<PipelineStage, JsonNode> stageEvents = readStageEvents(output.resolve(WorkerProtocol.EVENTS_FILE));
        Map<PipelineStage, JsonNode> stageReports = readStageReports(output);
        Map<PipelineStage, List<Artifact>> artifacts = collectArtifacts(input, output, runPrefix);

        List<Map<String, Object>> artifactIndex = new ArrayList<>();
        artifacts.values().stream().flatMap(List::stream).forEach(artifact -> artifactIndex.add(Map.of(
                "key", artifact.key(), "bytes", artifact.size(), "contentType", artifact.contentType())));
        for (StageTarget target : TARGETS) {
            Path report = stageReportPath(output, target.stage());
            if (Files.isRegularFile(report)) {
                artifactIndex.add(Map.of(
                        "key", join(runPrefix, target.folder(), "stage.json"),
                        "bytes", Files.size(report),
                        "contentType", "application/json"));
            }
        }

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("jobId", jobId);
        manifest.put("jobType", jobType.name());
        manifest.put("createdAt", Instant.now().toString());
        manifest.put("published", result.published());
        manifest.put("received", result.received());
        manifest.put("accepted", result.accepted());
        manifest.put("rejected", result.rejected());
        manifest.put("rejectedRatio", result.rejectedRatio());
        manifest.put("stages", TARGETS.stream().map(target -> stageReceipt(target, stageEvents.get(target.stage()),
                stageReports.get(target.stage()), artifacts.getOrDefault(target.stage(), List.of()).size())).toList());
        manifest.put("artifacts", artifactIndex);
        Path manifestPath = output.resolve("manifest.json");
        JsonPublisher.write(manifestPath, manifest);

        for (StageTarget target : TARGETS) {
            for (Artifact artifact : artifacts.getOrDefault(target.stage(), List.of())) {
                upload(artifact.path(), artifact.key(), artifact.contentType());
            }
            Path report = stageReportPath(output, target.stage());
            if (Files.isRegularFile(report)) {
                upload(report, join(runPrefix, target.folder(), "stage.json"), "application/json");
            } else {
                uploadBytes(JSON.writeValueAsBytes(stageReceipt(target, stageEvents.get(target.stage()), null,
                                artifacts.getOrDefault(target.stage(), List.of()).size())),
                        join(runPrefix, target.folder(), "stage.json"), "application/json");
            }
        }
        upload(manifestPath, join(runPrefix, "07-export", "manifest.json"), "application/json");

        for (String file : List.of(WorkerProtocol.EVENTS_FILE, WorkerProtocol.SPARK_FILE,
                WorkerProtocol.RESULT_FILE, WorkerProtocol.LOG_FILE)) {
            Path path = output.resolve(file);
            if (Files.isRegularFile(path)) {
                upload(path, join(runPrefix, "metadata", file), contentType(path));
            }
        }
        return "s3://" + bucket + "/" + runPrefix + "/";
    }

    private Map<PipelineStage, List<Artifact>> collectArtifacts(Path input, Path output, String runPrefix) throws IOException {
        Map<PipelineStage, List<Artifact>> artifacts = new LinkedHashMap<>();
        for (StageTarget target : TARGETS) artifacts.put(target.stage(), new ArrayList<>());
        addTree(artifacts.get(PipelineStage.BRONZE), input, runPrefix, "01-bronze", ".csv");
        addFile(artifacts.get(PipelineStage.VALIDATION), output.resolve(WorkerProtocol.VALIDATION_FILE),
                runPrefix, "02-validation", "validation.json");
        addTree(artifacts.get(PipelineStage.QUARANTINE), output.resolve(WorkerProtocol.QUARANTINE),
                runPrefix, "03-quarantine", null);
        addTree(artifacts.get(PipelineStage.SILVER), output.resolve(WorkerProtocol.SILVER),
                runPrefix, "04-silver", null);
        addFile(artifacts.get(PipelineStage.QUALITY_GATE), output.resolve(WorkerProtocol.QUALITY_FILE),
                runPrefix, "05-quality-gate", WorkerProtocol.QUALITY_FILE);
        addTree(artifacts.get(PipelineStage.GOLD), output.resolve(WorkerProtocol.GOLD),
                runPrefix, "06-gold", null);
        return artifacts;
    }

    private void addTree(List<Artifact> artifacts, Path directory, String runPrefix, String stageFolder,
                         String requiredSuffix) throws IOException {
        if (!Files.isDirectory(directory)) return;
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path file : paths.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList()) {
                if (requiredSuffix != null && !file.getFileName().toString().endsWith(requiredSuffix)) continue;
                String relative = directory.relativize(file).toString().replace('\\', '/');
                artifacts.add(new Artifact(file, join(runPrefix, stageFolder, relative), Files.size(file), contentType(file)));
            }
        }
    }

    private void addFile(List<Artifact> artifacts, Path file, String runPrefix, String stageFolder,
                         String targetName) throws IOException {
        if (Files.isRegularFile(file)) {
            artifacts.add(new Artifact(file, join(runPrefix, stageFolder, targetName), Files.size(file), contentType(file)));
        }
    }

    private Map<PipelineStage, JsonNode> readStageEvents(Path eventsPath) throws IOException {
        Map<PipelineStage, JsonNode> latest = new LinkedHashMap<>();
        if (!Files.isRegularFile(eventsPath)) return latest;
        for (String line : Files.readAllLines(eventsPath)) {
            if (line.isBlank()) continue;
            JsonNode event = JSON.readTree(line);
            try {
                latest.put(PipelineStage.valueOf(event.path("stage").asText()), event);
            } catch (IllegalArgumentException ignored) {
                // Ignore forward-compatible events that are not pipeline stages.
            }
        }
        return latest;
    }

    private Map<PipelineStage, JsonNode> readStageReports(Path output) throws IOException {
        Map<PipelineStage, JsonNode> reports = new LinkedHashMap<>();
        for (StageTarget target : TARGETS) {
            Path report = stageReportPath(output, target.stage());
            if (Files.isRegularFile(report)) {
                reports.put(target.stage(), JSON.readTree(report.toFile()));
            }
        }
        return reports;
    }

    private Path stageReportPath(Path output, PipelineStage stage) {
        return output.resolve(WorkerProtocol.STAGE_REPORTS)
                .resolve(stage.name().toLowerCase(Locale.ROOT) + ".json");
    }

    private Map<String, Object> stageReceipt(StageTarget target, JsonNode event, JsonNode report, int artifactCount) {
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("schemaVersion", 1);
        receipt.put("stage", target.stage().name());
        receipt.put("folder", target.folder());
        receipt.put("status", event == null ? "NOT_REACHED" : event.path("status").asText("UNKNOWN"));
        receipt.put("detail", event == null ? "No se alcanzó esta etapa" : event.path("detail").asText(""));
        receipt.put("rows", event == null || event.path("rows").isNull() ? null : event.path("rows").asLong());
        receipt.put("artifactCount", artifactCount);
        receipt.put("report", report == null ? null : report.path("metrics"));
        return receipt;
    }

    private void upload(Path file, String key, String contentType) {
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromFile(file));
        System.out.println("S3 publicado: s3://" + bucket + "/" + key);
    }

    private void uploadBytes(byte[] content, String key, String contentType) {
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromBytes(content));
    }

    private String contentType(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) return "text/csv";
        if (name.endsWith(".json") || name.endsWith(".jsonl")) return "application/json";
        if (name.endsWith(".parquet")) return "application/vnd.apache.parquet";
        return "application/octet-stream";
    }

    private static String normalizePrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) return "";
        return prefix.replace('\\', '/').replaceAll("^/+|/+$", "");
    }

    private static String join(String... parts) {
        return Stream.of(parts).filter(part -> part != null && !part.isBlank())
                .map(part -> part.replace('\\', '/').replaceAll("^/+|/+$", ""))
                .filter(part -> !part.isBlank()).reduce((left, right) -> left + "/" + right).orElse("");
    }

    private static String requireValue(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalStateException("Configura " + name + " para publicar en S3");
        return value.trim();
    }

    @Override
    public void close() {
        client.close();
    }

    private record StageTarget(PipelineStage stage, String folder) { }

    private record Artifact(Path path, String key, long size, String contentType) { }
}
