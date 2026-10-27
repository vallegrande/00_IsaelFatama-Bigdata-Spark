package pe.edu.vallegrande.bigdata.api.constant;

public final class ApiPaths {

    public static final String API = "/api";

    public static final String JOBS = API + "/jobs";

    public static final String EXECUTIONS = API + "/executions";

    public static final String DATASETS = API + "/datasets";

    public static final String DASHBOARD = API + "/dashboard/{jobType}";

    public static final String BY_ID = "/{id}";

    public static final String EVENTS = BY_ID + "/events";

    public static final String QUALITY = BY_ID + "/quality";

    public static final String RECONCILIATION = BY_ID + "/reconciliation";

    public static final String REJECTED = BY_ID + "/rejected";

    public static final String GOLD_SUMMARY = BY_ID + "/gold/summary";

    public static final String GOLD_TABLE = BY_ID + "/gold/{table}";

    public static final String BY_TYPE = "/{type}";

    public static final String TABLES = "/tables";

    public static final String SUMMARY = "/summary";

    public static final String TABLE = "/tables/{table}";

    public static final String ALL_API = API + "/**";

    private ApiPaths() {
    }

}
