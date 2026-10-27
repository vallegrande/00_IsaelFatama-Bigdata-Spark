package pe.edu.vallegrande.bigdata.api.constant;

public final class SqlQueries {

    public static final String SERVING_PREFIX = "SERVING_";

    public static final String SUMMARY_TABLE = SERVING_PREFIX + "RESUMEN";

    public static final String CURRENT_BATCH = """
            SELECT BATCH_ID, LOADED_AT, CONTENT
            FROM %s
            WHERE JOB_TYPE = ?
            ORDER BY LOADED_AT DESC
            FETCH FIRST 1 ROW ONLY""";

    public static final String COUNT_ROWS = "SELECT COUNT(*) FROM %s WHERE BATCH_ID = ?";

    public static final String PAGE_ROWS = """
            SELECT *
            FROM %s
            WHERE BATCH_ID = ?
            ORDER BY ROW_NO
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY""";

    private SqlQueries() {
    }

}
