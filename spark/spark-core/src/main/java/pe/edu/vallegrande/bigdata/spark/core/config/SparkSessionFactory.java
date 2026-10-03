package pe.edu.vallegrande.bigdata.spark.core.config;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(WorkerConfig config) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(config.appName())
                .master(config.master())
                .config("spark.ui.enabled", config.uiEnabled())
                .config("spark.ui.port", config.uiPort())
                .config("spark.ui.showConsoleProgress", false)
                .config("spark.driver.host", config.driverHost())
                .config("spark.driver.bindAddress", config.driverHost())
                .config("spark.sql.shuffle.partitions", config.shufflePartitions())
                .config("spark.sql.adaptive.enabled", true)
                .config("spark.sql.session.timeZone", config.timeZone())
                .config("spark.sql.datetime.java8API.enabled", true)
                .config("spark.sql.artifact.isolation.enabled", false);
        if (PermissiveLocalFileSystem.needed()) {
            builder.config("spark.hadoop.fs.file.impl", PermissiveLocalFileSystem.class.getName())
                    .config("spark.hadoop.fs.file.impl.disable.cache", true);
        }
        if (config.eventLogEnabled()) {
            builder.config("spark.eventLog.enabled", true)
                    .config("spark.eventLog.dir", eventLogDirectory(config).toUri().toString());
        }
        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }

    private static Path eventLogDirectory(WorkerConfig config) {
        try {
            return Files.createDirectories(config.eventLogDirectory());
        } catch (IOException error) {
            throw new UncheckedIOException("No se pudo crear la carpeta de eventos de Spark " + config.eventLogDirectory(), error);
        }
    }

    public static Map<String, Object> describe(SparkSession spark, Path output) throws IOException {
        SparkContext context = spark.sparkContext();
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("applicationId", context.applicationId());
        info.put("applicationName", context.appName());
        info.put("master", context.master());
        info.put("sparkVersion", spark.version());
        info.put("defaultParallelism", context.defaultParallelism());
        info.put("uiUrl", context.uiWebUrl().isDefined() ? context.uiWebUrl().get() : null);
        info.put("startedAt", Instant.ofEpochMilli(context.startTime()).toString());
        JsonPublisher.write(output.resolve(WorkerProtocol.SPARK_FILE), info);
        return info;
    }
}
