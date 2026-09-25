package pe.edu.vallegrande.bigdata.contracts;

import java.util.List;

public final class WorkerProtocol {

    public static final String MAIN_CLASS = "pe.edu.vallegrande.bigdata.worker.WorkerMain";

    public static final int EXIT_SUCCEEDED = 0;

    public static final int EXIT_FAILED = 1;

    public static final int EXIT_QUALITY_FAILED = 2;

    public static final String EVENTS_FILE = "events.jsonl";

    public static final String SPARK_FILE = "spark.json";

    public static final String RESULT_FILE = "result.json";

    public static final String QUALITY_FILE = "quality.json";

    public static final String LOG_FILE = "worker.log";

    public static final String SILVER = "silver";

    public static final String QUARANTINE = "quarantine";

    public static final String GOLD = "gold";

    public static final String SUMMARY_FILE = "gold/resumen.json";

    public static final String REJECTED_TABLE = "rechazados";

    public static final List<String> GOLD_TABLES = List.of(
            "rendimiento", "cursos_resumen", "estudiantes_resumen", "secciones_resumen", "evaluaciones_resumen",
            "distribucion_notas", "asistencia_semanal"
    );

    private WorkerProtocol() {}
}
