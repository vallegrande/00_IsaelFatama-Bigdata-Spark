package pe.edu.vallegrande.bigdata.spark.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Database {

    Dataset<Row> read(SparkSession spark, TableRef table);

    long write(Dataset<Row> data, TableRef table, WriteMode mode, String batchId);

}
