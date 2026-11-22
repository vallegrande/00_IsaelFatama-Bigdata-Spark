package pe.edu.vallegrande.bigdata.job.lectura;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.database.OracleConnection;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;
import pe.edu.vallegrande.bigdata.spark.core.publication.TablePublisher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class LecturaOracleJob {

    private LecturaOracleJob() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println(Constants.USAGE);
            System.exit(WorkerProtocol.EXIT_FAILED);
        }

        int exitCode = WorkerProtocol.EXIT_FAILED;

        SparkSession spark = null;

        try {
            Path output = Files.createDirectories(Path.of(args[1]));

            OracleConnection oracle = OracleConnection.fromEnvironment();
            spark = SparkSessionFactory.create(WorkerConfig.load().forJob(Constants.JOB_NAME,
                    output.getFileName().toString()));

            long started = System.currentTimeMillis();
            Map<String, Long> rowsByTable = read(spark, oracle, output);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("published", true);
            result.put("rowsByTable", rowsByTable);
            result.put("totalRows", rowsByTable.values().stream().mapToLong(Long::longValue).sum());
            result.put("outputFolder", Constants.OUTPUT_FOLDER);
            result.put("durationMs", System.currentTimeMillis() - started);
            JsonPublisher.write(output.resolve(Constants.RESULT_FILE), result);
            exitCode = WorkerProtocol.EXIT_SUCCEEDED;

        } catch (Exception error) {
            System.err.print(Constants.JOB_NAME + " termino con error: " + error.getMessage());
            error.printStackTrace();
        } finally {
            if(spark != null){
                spark.stop();
            }
        }
        System.exit(exitCode);

    }

    public static Map<String, Long> read(SparkSession spark, OracleConnection oracle, Path output) throws IOException {
        TablePublisher publisher = TablePublisher.csvAndParquet(output);
        Map<String, Long> rowsByTable = new LinkedHashMap<>();
        for (String table : Constants.TABLES) {
            Dataset<Row> source = spark.read().jdbc(oracle.url(), table, oracle.sparkProperties());
            long rows = publisher.publish(Constants.OUTPUT_FOLDER, table.toLowerCase(Locale.ROOT),
                    Transformer.toFileFormat(source));
            rowsByTable.put(table, rows);
            System.out.println(table + " : " + rows + " filas leidas");
        }
        return rowsByTable;
    }
}