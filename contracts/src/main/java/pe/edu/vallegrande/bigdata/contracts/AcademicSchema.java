package pe.edu.vallegrande.bigdata.contracts;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AcademicSchema {

    public static final String SEMESTRES = "semestres";

    public static final String CURSOS = "cursos";

    public static final String ESTUDIANTES = "estudiantes";

    public static final String NOTAS = "notas";

    public static final String ASISTENCIA = "asistencia";

    public static final List<String> TABLES = List.of(SEMESTRES, CURSOS, ESTUDIANTES, NOTAS, ASISTENCIA);

    public static final Map<String, List<String>> COLUMNS = Map.of(
            ASISTENCIA, List.of("estudiante_id", "curso_id", "periodo", "semana", "fecha", "estado"),
            NOTAS, List.of("estudiante_id", "curso_id", "periodo", "evaluacion", "peso", "nota", "fecha_registro"),
            ESTUDIANTES, List.of("estudiante_id", "nombres", "apellidos", "correo", "semestre_id", "seccion"),
            CURSOS, List.of("curso_id", "nombre", "semestre_id", "creditos", "horas_semanales", "docente"),
            SEMESTRES, List.of("semestre_id", "numero", "nombre", "periodo", "fecha_inicio", "fecha_fin")
    );

    public static final Map<String, List<String>> KEYS = Map.of(
            ASISTENCIA, List.of("estudiante_id", "curso_id", "periodo", "semana"),
            NOTAS, List.of("estudiante_id", "curso_id", "periodo", "evaluacion"),
            ESTUDIANTES, List.of("estudiante_id"),
            CURSOS, List.of("curso_id"),
            SEMESTRES, List.of("semestre_id")
    );

    public static final Map<String, Set<String>> OPTIONAL_COLUMNS = Map.of(
            NOTAS, Set.of("nota", "fecha_registro")
    );

    public static final Map<String, String> DESCRIPTIONS = Map.of(
            SEMESTRES, "Semestres de la carrera con su periodo y fecha",
            CURSOS, "Cursos de la malla curricular de Vallegrande",
            ESTUDIANTES, "Estudiantes matriculados con semestres",
            NOTAS, "Evaluaciones de estudiantes por cursos",
            ASISTENCIA, "Asistencia de estudiantes diarias."
    );

    public static final List<String> ATTENDANCE_STATES = List.of("P", "T", "F", "J");

    public static final int MAX_ROWS_PER_TABLE = 200_000;

    public static final long MAX_BYTES_PER_TABLE = 20L * 1024 * 1024;

    public static final int MAX_CELLS_LENGTH = 200;

    public static final String RULES_VERSION = "2.0.0";

    private AcademicSchema(){}

    public static List<String> columns(String table){
        List<String> columns = COLUMNS.get(table);
        if(columns == null){
            throw new IllegalArgumentException("Tabla desconocida. " + table);
        }
        return columns;
    }

    public static boolean optional(String table, String column){
        return OPTIONAL_COLUMNS.getOrDefault(table, Set.of()).contains(column);
    }

}
