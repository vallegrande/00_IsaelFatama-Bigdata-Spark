package pe.edu.vallegrande.bigdata.spark.core.layer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.stream.Collectors;

public record BronzeTables(Map<String, Dataset<Row>> tables, Map<String, Long> rows) {

    public Dataset<Row> table(String name) {
        return tables.get(name);
    }

    public long totalRows() {
        return rows.values().stream().mapToLong(Long::longValue).sum();
    }

    public String describe() {
        return rows.entrySet().stream().map(entry -> entry.getKey() + " " + entry.getValue()).collect(Collectors.joining(", "));
    }

    public void release() {
        tables.values().forEach(Dataset::unpersist);
    }
}
