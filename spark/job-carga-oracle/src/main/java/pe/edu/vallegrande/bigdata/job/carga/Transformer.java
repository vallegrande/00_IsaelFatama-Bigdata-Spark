package pe.edu.vallegrande.bigdata.job.carga;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public final class Transformer {

    private Transformer() {

    }

    public static Dataset<Row> prepare(Dataset<Row> dataset, TargetTable table, String executionId) {
        List<Column> columns = new ArrayList<>(table.columns().stream()
                .map(Transformer::typedColumn)
                .toList());
        columns.add(lit(executionId).alias(Constants.EXECUTION_COLUMN));
        return dataset.select(columns.toArray(new Column[0]));
    }

    public static Column typedColumn(String definition) {
        String[] parts = definition.split(Constants.TYPE_SEPARATOR);
        String name = parts[0];
        String type = parts[1];
        return col(name).cast(type).alias(name.toUpperCase(Locale.ROOT));
    }
}
