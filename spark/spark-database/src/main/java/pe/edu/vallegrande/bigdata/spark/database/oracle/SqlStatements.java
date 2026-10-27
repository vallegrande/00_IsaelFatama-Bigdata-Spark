package pe.edu.vallegrande.bigdata.spark.database.oracle;

import java.util.List;
import java.util.stream.Collectors;

public final class SqlStatements {

    public static final String LOAD_SUFFIX = "_LOAD";

    private static final String DELETE_BATCH = "DELETE FROM %s WHERE BATCH_ID = ?";

    private static final String INSERT_BATCH = "INSERT INTO %s (%s) SELECT %s FROM %s WHERE BATCH_ID = ?";

    private static final String MERGE_BATCH = """
            MERGE INTO %s t
            USING (SELECT %s FROM %s WHERE BATCH_ID = ?) s
            ON (%s)
            %s
            WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)""";

    private static final String MERGE_UPDATE = "WHEN MATCHED THEN UPDATE SET %s";

    private static final String COUNT_BATCH = "SELECT COUNT(*) FROM %s WHERE BATCH_ID = ?";

    private SqlStatements() {
    }

    public static String loadTable(String target) {
        return target + LOAD_SUFFIX;
    }

    public static String deleteBatch(String table) {
        return DELETE_BATCH.formatted(table);
    }

    public static String countBatch(String table) {
        return COUNT_BATCH.formatted(table);
    }

    public static String insertBatch(String target, String load, List<String> columns) {
        String list = String.join(", ", columns);
        return INSERT_BATCH.formatted(target, list, list, load);
    }

    public static String mergeBatch(String target, String load, List<String> columns, List<String> keys) {
        String on = keys.stream().map(key -> "t." + key + " = s." + key).collect(Collectors.joining(" AND "));
        List<String> updatable = columns.stream().filter(column -> !keys.contains(column)).toList();
        String update = updatable.isEmpty() ? "" : MERGE_UPDATE.formatted(
                updatable.stream().map(column -> "t." + column + " = s." + column).collect(Collectors.joining(", ")));
        String list = String.join(", ", columns);
        String values = columns.stream().map(column -> "s." + column).collect(Collectors.joining(", "));
        return MERGE_BATCH.formatted(target, list, load, on, update, list, values).replace("\n\n", "\n");
    }

}
