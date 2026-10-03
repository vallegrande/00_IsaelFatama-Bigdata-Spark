package pe.edu.vallegrande.bigdata.spark.core.publication;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface TableFormat {

    String extension();

    void write(Path target, StructType schema, List<Row> rows) throws IOException;
}
