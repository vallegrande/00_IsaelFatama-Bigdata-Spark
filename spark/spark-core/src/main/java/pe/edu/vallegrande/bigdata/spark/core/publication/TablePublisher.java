package pe.edu.vallegrande.bigdata.spark.core.publication;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public final class TablePublisher {
    private final Path output;
    private final List<TableFormat> formats;

    public TablePublisher(Path output, List<TableFormat> formats) {
        this.output = output;
        this.formats = List.copyOf(formats);
    }

    public static TablePublisher csvAndParquet(Path output) {
        return new TablePublisher(output, List.of(new CsvPublisher(), new ParquetPublisher()));
    }

    public long publish(String layer, String name, Dataset<Row> table) throws IOException {
        List<Row> rows = table.collectAsList();
        Path directory = output.resolve(layer);
        for (TableFormat format : formats) {
            format.write(directory.resolve(name + "." + format.extension()), table.schema(), rows);
        }
        return rows.size();
    }

    public int formatsPerTable() {
        return formats.size();
    }
}
