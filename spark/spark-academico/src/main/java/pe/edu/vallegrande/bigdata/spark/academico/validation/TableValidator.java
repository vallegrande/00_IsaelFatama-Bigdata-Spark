package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TableValidator {

    String table();

    Dataset<Row> check(Dataset<Row> bronze, ReferenceData references);

    Dataset<Row> silver(Dataset<Row> checked);

    default ReferenceData publish(Dataset<Row> checked, ReferenceData references) {
        return references;
    }
}
