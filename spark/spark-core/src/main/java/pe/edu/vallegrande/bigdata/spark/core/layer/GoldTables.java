package pe.edu.vallegrande.bigdata.spark.core.layer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

public record GoldTables(String primary, Map<String, Dataset<Row>> tables, Dataset<Row> summary) {

    public GoldTables {
        if (!tables.containsKey(primary)) {
            throw new IllegalArgumentException("La tabla principal " + primary + " no está entre las tablas Gold");
        }
    }

    public Dataset<Row> primaryTable() {
        return tables.get(primary);
    }

    public Map<String, Object> summaryValues() {
        Row row = summary.first();
        Map<String, Object> values = new LinkedHashMap<>();
        for (String field : row.schema().fieldNames()) {
            int index = row.fieldIndex(field);
            values.put(field, row.isNullAt(index) ? null : row.get(index));
        }
        return values;
    }

    public void release() {
        tables.values().forEach(Dataset::unpersist);
    }
}
