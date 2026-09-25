package pe.edu.vallegrande.bigdata.worker;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

public final class WorkerMain {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) throw new IllegalArgumentException("input.csv output-directory threshold");

        SparkSession spark = SparkSession.builder().appName("Academic Pipeline")
                .master("local[2]").config("spark.ui.enabled", false)
                .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", 2).config("spark.sql.session.timeZone", "UTC").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        boolean published;

        try {
            published = new AcademicPipeline().run(spark, Path.of(args[0]), Path.of(args[1]),
                    Double.parseDouble(args[2])).published();
        } finally {
            spark.stop();
        }

        if (!published) System.exit(2);

    }

}
