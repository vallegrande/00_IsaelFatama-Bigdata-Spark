package pe.edu.vallegrande.bigdata.job.carga;

import java.util.List;

public final class Constants {

    public static final String JOB_NAME = "Carga a Oracle";

    public static final String USAGE = "Uso: CargaOracleJob <carpeta-de-la-ejecucion> <carpeta-de-salida>";

    public static final String RESULT_FILE = "result.json";

    public static final int ROWS_PER_BATCH = 1000;

    public static final String TYPE_SEPARATOR = ":";

    public static final String EXECUTION_COLUMN = "EJECUCION_ID";

    public static final String COUNT_INSERTED = "SELECT COUNT(*) FROM %s WHERE EJECUCION_ID = ? AND FECHA_ACTUALIZACION IS NULL";

    public static final String COUNT_UPDATED = "SELECT COUNT(*) FROM %s WHERE EJECUCION_ID = ? AND FECHA_ACTUALIZACION IS NOT NULL";

    public static final String INSERT_LOAD = """
            INSERT INTO CARGAS (EJECUCION_ID, TABLA, FILAS_INSERTADAS, FILAS_ACTUALIZADAS, FECHA_CARGA)
            VALUES (?, ?, ?, ?, SYSTIMESTAMP)""";

    public static final String MERGE_SEMESTRES = """
            MERGE INTO SEMESTRES t
            USING (SELECT ? AS SEMESTRE_ID, ? AS NUMERO, ? AS NOMBRE, ? AS PERIODO, ? AS FECHA_INICIO, ? AS FECHA_FIN, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.SEMESTRE_ID = s.SEMESTRE_ID)
            WHEN MATCHED THEN UPDATE SET
                t.NUMERO = s.NUMERO, t.NOMBRE = s.NOMBRE, t.PERIODO = s.PERIODO,
                t.FECHA_INICIO = s.FECHA_INICIO, t.FECHA_FIN = s.FECHA_FIN, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (SEMESTRE_ID, NUMERO, NOMBRE, PERIODO, FECHA_INICIO, FECHA_FIN, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.SEMESTRE_ID, s.NUMERO, s.NOMBRE, s.PERIODO, s.FECHA_INICIO, s.FECHA_FIN, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_CURSOS = """
            MERGE INTO CURSOS t
            USING (SELECT ? AS CURSO_ID, ? AS NOMBRE, ? AS SEMESTRE_ID, ? AS CREDITOS, ? AS HORAS_SEMANALES, ? AS DOCENTE, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.CURSO_ID = s.CURSO_ID)
            WHEN MATCHED THEN UPDATE SET
                t.NOMBRE = s.NOMBRE, t.SEMESTRE_ID = s.SEMESTRE_ID, t.CREDITOS = s.CREDITOS,
                t.HORAS_SEMANALES = s.HORAS_SEMANALES, t.DOCENTE = s.DOCENTE, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (CURSO_ID, NOMBRE, SEMESTRE_ID, CREDITOS, HORAS_SEMANALES, DOCENTE, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.CURSO_ID, s.NOMBRE, s.SEMESTRE_ID, s.CREDITOS, s.HORAS_SEMANALES, s.DOCENTE, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_ESTUDIANTES = """
            MERGE INTO ESTUDIANTES t
            USING (SELECT ? AS ESTUDIANTE_ID, ? AS NOMBRES, ? AS APELLIDOS, ? AS CORREO, ? AS SEMESTRE_ID, ? AS SECCION, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.ESTUDIANTE_ID = s.ESTUDIANTE_ID)
            WHEN MATCHED THEN UPDATE SET
                t.NOMBRES = s.NOMBRES, t.APELLIDOS = s.APELLIDOS, t.CORREO = s.CORREO,
                t.SEMESTRE_ID = s.SEMESTRE_ID, t.SECCION = s.SECCION, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (ESTUDIANTE_ID, NOMBRES, APELLIDOS, CORREO, SEMESTRE_ID, SECCION, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.ESTUDIANTE_ID, s.NOMBRES, s.APELLIDOS, s.CORREO, s.SEMESTRE_ID, s.SECCION, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_NOTAS = """
            MERGE INTO NOTAS t
            USING (SELECT ? AS ESTUDIANTE_ID, ? AS CURSO_ID, ? AS PERIODO, ? AS EVALUACION, ? AS PESO, ? AS NOTA, ? AS FECHA_REGISTRO, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.ESTUDIANTE_ID = s.ESTUDIANTE_ID AND t.CURSO_ID = s.CURSO_ID AND t.PERIODO = s.PERIODO AND t.EVALUACION = s.EVALUACION)
            WHEN MATCHED THEN UPDATE SET
                t.PESO = s.PESO, t.NOTA = s.NOTA, t.FECHA_REGISTRO = s.FECHA_REGISTRO, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (ESTUDIANTE_ID, CURSO_ID, PERIODO, EVALUACION, PESO, NOTA, FECHA_REGISTRO, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.ESTUDIANTE_ID, s.CURSO_ID, s.PERIODO, s.EVALUACION, s.PESO, s.NOTA, s.FECHA_REGISTRO, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_ASISTENCIA = """
            MERGE INTO ASISTENCIA t
            USING (SELECT ? AS ESTUDIANTE_ID, ? AS CURSO_ID, ? AS PERIODO, ? AS SEMANA, ? AS FECHA, ? AS ESTADO, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.ESTUDIANTE_ID = s.ESTUDIANTE_ID AND t.CURSO_ID = s.CURSO_ID AND t.PERIODO = s.PERIODO AND t.SEMANA = s.SEMANA)
            WHEN MATCHED THEN UPDATE SET
                t.FECHA = s.FECHA, t.ESTADO = s.ESTADO, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (ESTUDIANTE_ID, CURSO_ID, PERIODO, SEMANA, FECHA, ESTADO, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.ESTUDIANTE_ID, s.CURSO_ID, s.PERIODO, s.SEMANA, s.FECHA, s.ESTADO, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_RENDIMIENTO = """
            MERGE INTO RENDIMIENTO t
            USING (SELECT ? AS ESTUDIANTE_ID, ? AS CURSO_ID, ? AS PERIODO, ? AS NOMBRES, ? AS APELLIDOS, ? AS SECCION,
                          ? AS CURSO, ? AS PROMEDIO, ? AS NOTA_FINAL, ? AS ASISTENCIA_PCT, ? AS CONDICION, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.ESTUDIANTE_ID = s.ESTUDIANTE_ID AND t.CURSO_ID = s.CURSO_ID AND t.PERIODO = s.PERIODO)
            WHEN MATCHED THEN UPDATE SET
                t.NOMBRES = s.NOMBRES, t.APELLIDOS = s.APELLIDOS, t.SECCION = s.SECCION, t.CURSO = s.CURSO,
                t.PROMEDIO = s.PROMEDIO, t.NOTA_FINAL = s.NOTA_FINAL, t.ASISTENCIA_PCT = s.ASISTENCIA_PCT,
                t.CONDICION = s.CONDICION, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (ESTUDIANTE_ID, CURSO_ID, PERIODO, NOMBRES, APELLIDOS, SECCION, CURSO,
                                          PROMEDIO, NOTA_FINAL, ASISTENCIA_PCT, CONDICION, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.ESTUDIANTE_ID, s.CURSO_ID, s.PERIODO, s.NOMBRES, s.APELLIDOS, s.SECCION, s.CURSO,
                        s.PROMEDIO, s.NOTA_FINAL, s.ASISTENCIA_PCT, s.CONDICION, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final String MERGE_CURSOS_RESUMEN = """
            MERGE INTO CURSOS_RESUMEN t
            USING (SELECT ? AS CURSO_ID, ? AS PERIODO, ? AS CURSO, ? AS DOCENTE, ? AS MATRICULADOS, ? AS APROBADOS,
                          ? AS DESAPROBADOS, ? AS PROMEDIO, ? AS ASISTENCIA_PROMEDIO, ? AS TASA_APROBACION, ? AS EJECUCION_ID FROM DUAL) s
            ON (t.CURSO_ID = s.CURSO_ID AND t.PERIODO = s.PERIODO)
            WHEN MATCHED THEN UPDATE SET
                t.CURSO = s.CURSO, t.DOCENTE = s.DOCENTE, t.MATRICULADOS = s.MATRICULADOS, t.APROBADOS = s.APROBADOS,
                t.DESAPROBADOS = s.DESAPROBADOS, t.PROMEDIO = s.PROMEDIO,
                t.ASISTENCIA_PROMEDIO = s.ASISTENCIA_PROMEDIO, t.TASA_APROBACION = s.TASA_APROBACION, t.FECHA_ACTUALIZACION = SYSTIMESTAMP, t.EJECUCION_ID = s.EJECUCION_ID
            WHEN NOT MATCHED THEN INSERT (CURSO_ID, PERIODO, CURSO, DOCENTE, MATRICULADOS, APROBADOS, DESAPROBADOS,
                                          PROMEDIO, ASISTENCIA_PROMEDIO, TASA_APROBACION, FECHA_CREACION, EJECUCION_ID)
                VALUES (s.CURSO_ID, s.PERIODO, s.CURSO, s.DOCENTE, s.MATRICULADOS, s.APROBADOS, s.DESAPROBADOS,
                        s.PROMEDIO, s.ASISTENCIA_PROMEDIO, s.TASA_APROBACION, SYSTIMESTAMP, s.EJECUCION_ID)""";

    public static final List<TargetTable> TABLES = List.of(
            new TargetTable("SEMESTRES", "silver/semestres.parquet",
                    List.of("semestre_id:string", "numero:int", "nombre:string", "periodo:string", "fecha_inicio:date", "fecha_fin:date"),
                    MERGE_SEMESTRES),
            new TargetTable("CURSOS", "silver/cursos.parquet",
                    List.of("curso_id:string", "nombre:string", "semestre_id:string", "creditos:int", "horas_semanales:int", "docente:string"),
                    MERGE_CURSOS),
            new TargetTable("ESTUDIANTES", "silver/estudiantes.parquet",
                    List.of("estudiante_id:string", "nombres:string", "apellidos:string", "correo:string", "semestre_id:string", "seccion:string"),
                    MERGE_ESTUDIANTES),
            new TargetTable("NOTAS", "silver/notas.parquet",
                    List.of("estudiante_id:string", "curso_id:string", "periodo:string", "evaluacion:string", "peso:double", "nota:double",
                            "fecha_registro:date"),
                    MERGE_NOTAS),
            new TargetTable("ASISTENCIA", "silver/asistencia.parquet",
                    List.of("estudiante_id:string", "curso_id:string", "periodo:string", "semana:int", "fecha:date", "estado:string"),
                    MERGE_ASISTENCIA),
            new TargetTable("RENDIMIENTO", "gold/rendimiento.parquet",
                    List.of("estudiante_id:string", "curso_id:string", "periodo:string", "nombres:string", "apellidos:string", "seccion:string",
                            "curso:string", "promedio:double", "nota_final:int", "asistencia_pct:double", "condicion:string"),
                    MERGE_RENDIMIENTO),
            new TargetTable("CURSOS_RESUMEN", "gold/cursos_resumen.parquet",
                    List.of("curso_id:string", "periodo:string", "curso:string", "docente:string", "matriculados:long", "aprobados:long",
                            "desaprobados:long", "promedio:double", "asistencia_promedio:double", "tasa_aprobacion:double"),
                    MERGE_CURSOS_RESUMEN));

    private Constants(){

    }
}
