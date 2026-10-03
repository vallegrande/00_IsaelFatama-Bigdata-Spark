package pe.edu.vallegrande.bigdata.spark.core.launcher;

import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.nio.file.Path;
import java.util.Map;

public final class JobLauncher {

    private JobLauncher() {
    }

    public static void launch(JobDefinition job, String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Uso: " + job.type().mainClass() + " <input-directory> <output-directory> <threshold> [spark-ui-seconds]");
            System.exit(WorkerProtocol.EXIT_FAILED);
        }
        int exitCode;
        try {
            int uiSeconds = args.length == 4 ? Integer.parseInt(args[3]) : 0;
            exitCode = run(job, Path.of(args[0]), Path.of(args[1]), Double.parseDouble(args[2]), uiSeconds, WorkerConfig.load());
        } catch (Exception error) {
            System.err.println("El job " + job.type().label() + " terminó con error: " + error.getMessage());
            error.printStackTrace();
            exitCode = WorkerProtocol.EXIT_FAILED;
        }
        System.exit(exitCode);
    }

    public static int run(JobDefinition job, Path input, Path output, double threshold, int uiSeconds, WorkerConfig config)
            throws Exception {
        SparkSession spark = SparkSessionFactory.create(config.forJob(job.type().label(), output.getFileName().toString()));
        try {
            Map<String, Object> info = SparkSessionFactory.describe(spark, output);
            System.out.println(job.type().label() + " · Spark " + info.get("sparkVersion") + " en " + info.get("master")
                    + " · UI: " + info.get("uiUrl"));
            PipelineResult result = job.pipeline().run(spark, input, output, threshold);
            JsonPublisher.write(output.resolve(WorkerProtocol.RESULT_FILE), result);
            System.out.println("Resultado: " + (result.published() ? "PUBLICADO" : "BLOQUEADO POR CALIDAD")
                    + " · recibidas " + result.received() + " · válidas " + result.accepted() + " · rechazadas " + result.rejected());
            keepUiAlive(info, uiSeconds);
            return result.published() ? WorkerProtocol.EXIT_SUCCEEDED : WorkerProtocol.EXIT_QUALITY_FAILED;
        } finally {
            spark.stop();
        }
    }

    private static void keepUiAlive(Map<String, Object> info, int seconds) throws InterruptedException {
        if (seconds <= 0 || info.get("uiUrl") == null) {
            return;
        }
        System.out.println("Spark UI disponible en " + info.get("uiUrl") + " durante " + seconds + " segundos");
        Thread.sleep(seconds * 1000L);
    }
}
