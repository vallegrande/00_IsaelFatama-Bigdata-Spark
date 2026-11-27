package pe.edu.vallegrande.bigdata.job.carga;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.database.OracleConnection;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CargaOracleJob {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println(Constants.USAGE);
            System.exit(WorkerProtocol.EXIT_FAILED);
        }

        int exitCode = WorkerProtocol.EXIT_FAILED;

        SparkSession spark = null;

        try{
            Path execution = Path.of(args[0]);
            Path output = Files.createDirectories(Path.of(args[1]));

            String executionId = execution.getFileName().toString();
            OracleConnection oracle = OracleConnection.fromEnvironment();
            spark = SparkSessionFactory.create(WorkerConfig.load().forJob(Constants.JOB_NAME, executionId));

            long started = System.currentTimeMillis();
            List<LoadResult> loads = load(spark, execution, oracle, executionId);

            Map<String, Long> rowsByTable = new LinkedHashMap<>();
            loads.forEach(load -> rowsByTable.put(load.table(), load.total()));
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("published", true);
            result.put("rowsByTable", rowsByTable);
            result.put("loads", loads);
            result.put("totalRows", loads.stream().mapToLong(LoadResult::total).sum());
            result.put("insertedRows", loads.stream().mapToLong(LoadResult::inserted).sum());
            result.put("updatedRows", loads.stream().mapToLong(LoadResult::updated).sum());
            result.put("durationMs", System.currentTimeMillis() - started);
            JsonPublisher.write(output.resolve(Constants.RESULT_FILE), result);
            exitCode = WorkerProtocol.EXIT_SUCCEEDED;

        } catch (Exception error){
            System.out.println(Constants.JOB_NAME + " termino con error: " + error.getMessage());
            error.printStackTrace();
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
        System.exit(exitCode);
    }

    public static List<LoadResult> load(SparkSession spark, Path execution, OracleConnection oracle, String executionId) throws SQLException {

        LoadAudit audit = new LoadAudit(oracle, executionId);
        List<LoadResult> loads = new ArrayList<>();

        for(TargetTable table : Constants.TABLES){
            Dataset<Row> source = spark.read().parquet(execution.resolve(table.sourceFile()).toString());
            Transformer.prepare(source, table, executionId).foreachPartition(new MergeWriter(oracle, table.mergeSql()));
            LoadResult load = audit.register(table.name());
            loads.add(load);
            System.out.println(table.name() + ": " + load.inserted() + " insertadas, " + load.updated() + " actualizadas, " + load.total() + " total");
        }
        return loads;
    }
}