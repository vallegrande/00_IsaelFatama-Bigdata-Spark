package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface GoldTableBuilder {

    String name();

    Dataset<Row> build(GoldInputs inputs);
}
