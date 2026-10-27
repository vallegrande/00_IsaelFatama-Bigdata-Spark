package pe.edu.vallegrande.bigdata.spark.database;

import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.util.List;

public final class DatabaseTables {

    public static final List<String> SERVING = JobType.servingTables();

    public static final String DDL_DIRECTORY = "db/oracle/";

    public static final List<String> DDL_SCRIPTS = List.of("01_staging.sql", "02_core.sql", "03_serving.sql", "04_control.sql");

    private DatabaseTables() {
    }

}
