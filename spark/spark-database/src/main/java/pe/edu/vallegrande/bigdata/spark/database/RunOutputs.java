package pe.edu.vallegrande.bigdata.spark.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public final class RunOutputs {

    private static final String PARQUET = ".parquet";

    private RunOutputs() {
    }

    public static String batchId(Path run) {
        Path name = run.toAbsolutePath().normalize().getFileName();
        return name == null ? run.toString() : name.toString();
    }

    public static boolean exists(Path run, String layer, String table) {
        return Files.isRegularFile(file(run, layer, table));
    }

    public static Dataset<Row> read(SparkSession spark, Path run, String layer, String table) {
        Path file = file(run, layer, table);
        if (!Files.isRegularFile(file)) {
            throw new IllegalArgumentException("La ejecución " + batchId(run) + " no tiene " + layer + "/" + table + PARQUET);
        }
        return spark.read().parquet(file.toString());
    }

    public static List<String> tables(Path run, String layer) {
        Path directory = run.resolve(layer);
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(PARQUET))
                    .map(name -> name.substring(0, name.length() - PARQUET.length()))
                    .sorted()
                    .toList();
        } catch (IOException error) {
            throw new UncheckedIOException("No se pudo listar " + directory, error);
        }
    }

    private static Path file(Path run, String layer, String table) {
        return run.resolve(layer).resolve(table + PARQUET);
    }

}
