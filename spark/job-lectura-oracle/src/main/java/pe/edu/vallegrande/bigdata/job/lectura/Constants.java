package pe.edu.vallegrande.bigdata.job.lectura;

import java.util.List;

public final class Constants {

    public static final String JOB_NAME = "Lectura desde Oracle";

    public static final String USAGE = "Uso: LecturaOracleJob <carpeta-de-entrada> <carpeta-de-salida>";

    public static final String RESULT_FILE = "result.json";

    public static final String OUTPUT_FOLDER = "oracle";

    public static final String[] AUDIT_COLUMNS = {
            "FECHA_CREACION", "FECHA_ACTUALIZACION", "EJECUCION_ID"
    };

    public static final String INTEGER_TYPE = "long";

    public static final String DECIMAL_TYPE = "double";

    public static final List<String> TABLES = List.of("SEMESTRES", "CURSOS", "ESTUDIANTES", "NOTAS", "ASISTENCIA",
            "RENDIMIENTO", "CURSOS_RESUMEN");

    private Constants() {

    }

}
