package pe.edu.vallegrande.bigdata.contracts;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public enum JobType {

    ACADEMIC_PERFORMANCE(
            "job-academic-performance",
            "pe.edu.vallegrande.bigdata.job.performance.AcademicPerformanceJob",
            "Academic performance",
            JobKind.TRANSFORMATION,
            List.of("rendimiento", "cursos_resumen", "estudiantes_resumen", "secciones_resumen",
                    "evaluaciones_resumen", "distribucion_notas", "asistencia_semanal")
    ),
    ATTENDANCE_ALERT(
            "job-attendance-alert",
            "pe.edu.vallegrande.bigdata.job.attendance.AttendanceAlertJob",
            "Attendance alert",
            JobKind.TRANSFORMATION,
            List.of("alertas_asistencia", "alertas_por_curso")
    ),
    DATA_INGESTION(
            "job-data-ingestion",
            "pe.edu.vallegrande.bigdata.job.ingestion.DataIngestionJob",
            "Data ingestion from CSV into the database",
            JobKind.INGESTION,
            List.of()
    ),
    DATA_EXTRACTION(
            "job-data-extraction",
            "pe.edu.vallegrande.bigdata.job.extraction.DataExtractionJob",
            "Data extraction from the database",
            JobKind.EXTRACTION,
            List.of()
    ),
    DATA_LOAD(
            "job-data-load",
            "pe.edu.vallegrande.bigdata.job.load.DataLoadJob",
            "Data load into the database",
            JobKind.LOAD,
            List.of()
    ),
    DATA_RECONCILIATION(
            "job-data-reconciliation",
            "pe.edu.vallegrande.bigdata.job.reconciliation.DataReconciliationJob",
            "Reconciliation between files and the database",
            JobKind.RECONCILIATION,
            List.of()
    );

    public static final JobType DEFAULT = ACADEMIC_PERFORMANCE;

    private final String module;

    private final String mainClass;

    private final String label;

    private final JobKind kind;

    private final List<String> goldTables;

    JobType(String module, String mainClass, String label, JobKind kind, List<String> goldTables) {
        this.module = module;
        this.mainClass = mainClass;
        this.label = label;
        this.kind = kind;
        this.goldTables = goldTables;
    }

    public String module() {
        return module;
    }

    public String jar() {
        return module + ".jar";
    }

    public String mainClass() {
        return mainClass;
    }

    public String label() {
        return label;
    }

    public JobKind kind() {
        return kind;
    }

    public List<String> goldTables() {
        return goldTables;
    }

    public static List<String> servingTables() {
        return Arrays.stream(values())
                .filter(type -> type.kind == JobKind.TRANSFORMATION)
                .flatMap(type -> type.goldTables.stream())
                .distinct()
                .toList();
    }

    public static Optional<JobType> dashboardOf(List<String> goldTables) {
        return Arrays.stream(values())
                .filter(type -> type.kind == JobKind.TRANSFORMATION)
                .filter(type -> !goldTables.isEmpty() && type.goldTables.containsAll(goldTables))
                .findFirst();
    }
}
