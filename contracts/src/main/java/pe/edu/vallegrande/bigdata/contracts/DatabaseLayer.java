package pe.edu.vallegrande.bigdata.contracts;

public enum DatabaseLayer {

    STAGING("STG_"),

    CORE("CORE_"),

    SERVING("SERVING_"),

    CONTROL("ETL_");

    public static final String BATCH_COLUMN = "batch_id";

    public static final String ROW_COLUMN = "row_no";

    public static final String LOADED_COLUMN = "loaded_at";

    public static final String SUMMARY_TABLE = "resumen";

    public static final String JOB_TYPE_COLUMN = "job_type";

    public static final String CONTENT_COLUMN = "content";

    private final String prefix;

    DatabaseLayer(String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }

}
