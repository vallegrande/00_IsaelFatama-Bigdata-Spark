package pe.edu.vallegrande.bigdata.contracts;

import java.util.List;

public enum JobType {

    RENDIMIENTO_ACADEMICO(
            "job-rendimiento-academico",
            "pe.edu.vallegrande.bigdata.job.rendimiento.RendimientoAcademicoJob",
            "Rendimiento académico",
            List.of("rendimiento", "cursos_resumen", "estudiantes_resumen", "secciones_resumen",
                    "evaluaciones_resumen", "distribucion_notas", "asistencia_semanal")
    ),
    ALERTA_ASISTENCIA(
            "job-alerta-asistencia",
            "pe.edu.vallegrande.bigdata.job.asistencia.AlertaAsistenciaJob",
            "Alerta de Asistencia",
            List.of("alertas_asistencia", "alertas_por_curso")
    );

    public static final JobType DEFAULT = RENDIMIENTO_ACADEMICO;

    private final String module;

    private final String mainClass;

    private final String label;

    private final List<String> goldTables;

    JobType(String module, String mainClass, String label, List<String> goldTables) {
        this.module = module;
        this.mainClass = mainClass;
        this.label = label;
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

    public List<String> goldTables() {
        return goldTables;
    }
}
