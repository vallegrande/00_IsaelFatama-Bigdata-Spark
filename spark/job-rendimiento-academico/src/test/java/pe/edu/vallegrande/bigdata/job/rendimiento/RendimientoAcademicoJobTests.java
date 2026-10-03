package pe.edu.vallegrande.bigdata.job.rendimiento;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RendimientoAcademicoJobTests {
    private static final Path CLEAN = Path.of("..", "..", "data", "limpio");
    private static final Path ERRORS = Path.of("..", "..", "data", "con-errores");
    private static SparkSession spark;

    @TempDir
    Path directory;

    @BeforeAll
    static void startSpark() {
        spark = SparkSessionFactory.create(WorkerConfig.load().withoutUi());
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void calculatesGradesAttendanceAndConditionAsTheManualExample() throws Exception {
        Path input = Files.createDirectories(directory.resolve("manual"));
        Files.writeString(input.resolve("semestres.csv"), """
                semestre_id,numero,nombre,periodo,fecha_inicio,fecha_fin
                V,5,Quinto semestre,2026-II,2026-08-17,2026-12-18
                """);
        Files.writeString(input.resolve("cursos.csv"), """
                curso_id,nombre,semestre_id,creditos,horas_semanales,docente
                TBD,Talleres de Big Data,V,4,6,Docente
                """);
        Files.writeString(input.resolve("estudiantes.csv"), """
                estudiante_id,nombres,apellidos,correo,semestre_id,seccion
                E1,Ana,Torres,ana@vallegrande.edu.pe,V,A
                E2,Luis,Rojas,luis@vallegrande.edu.pe,V,A
                E3,Rosa,Díaz,rosa@vallegrande.edu.pe,V,B
                """);
        Files.writeString(input.resolve("notas.csv"), """
                estudiante_id,curso_id,periodo,evaluacion,peso,nota,fecha_registro
                E1,TBD,2026-II,EP,0.5,12,2026-10-12
                E1,TBD,2026-II,EF,0.5,13,2026-12-07
                E2,TBD,2026-II,EP,0.5,10,2026-10-12
                E2,TBD,2026-II,EF,0.5,,
                E3,TBD,2026-II,EP,0.5,18,2026-10-12
                E3,TBD,2026-II,EF,0.5,18,2026-12-07
                """);
        Files.writeString(input.resolve("asistencia.csv"), """
                estudiante_id,curso_id,periodo,semana,fecha,estado
                E1,TBD,2026-II,1,2026-08-18,P
                E1,TBD,2026-II,2,2026-08-25,P
                E1,TBD,2026-II,3,2026-09-01,T
                E1,TBD,2026-II,4,2026-09-08,F
                E2,TBD,2026-II,1,2026-08-18,P
                E2,TBD,2026-II,2,2026-08-25,P
                E2,TBD,2026-II,3,2026-09-01,P
                E2,TBD,2026-II,4,2026-09-08,J
                E3,TBD,2026-II,1,2026-08-18,P
                E3,TBD,2026-II,2,2026-08-25,F
                E3,TBD,2026-II,3,2026-09-01,F
                E3,TBD,2026-II,4,2026-09-08,P
                """);
        Path output = directory.resolve("manual-out");

        PipelineResult result = new RendimientoAcademicoJob().pipeline().run(spark, input, output, 0);

        assertTrue(result.published());
        List<String> rows = Files.readAllLines(output.resolve("gold/rendimiento.csv"));
        assertEquals("E3,Rosa,Díaz,B,V,2026-II,TBD,Talleres de Big Data,Docente,4,2,0,18,18,4,2,0,2,0,0.5,0.5,INHABILITADO,true,Inasistencias mayores al 30 %", rows.get(1));
        assertEquals("E2,Luis,Rojas,A,V,2026-II,TBD,Talleres de Big Data,Docente,4,2,1,10,,4,3,0,0,1,1,0,PENDIENTE,true,Promedio menor a 13; Evaluaciones pendientes", rows.get(2));
        assertEquals("E1,Ana,Torres,A,V,2026-II,TBD,Talleres de Big Data,Docente,4,2,0,12.5,13,4,2,1,1,0,0.75,0.25,APROBADO,true,Inasistencias en riesgo", rows.get(3));
    }

    @Test
    void cleanDataPublishesGoldWithoutQuarantine() throws Exception {
        Path output = directory.resolve("clean-out");

        PipelineResult result = new RendimientoAcademicoJob().pipeline().run(spark, CLEAN, output, 0.05);

        assertTrue(result.published());
        assertEquals(21735, result.received());
        assertEquals(0, result.rejected());
        assertEquals(1080, result.goldRows());
        assertTrue(Files.isRegularFile(output.resolve("silver/notas.parquet")));
        assertTrue(Files.isRegularFile(output.resolve("gold/rendimiento.parquet")));
        assertEquals(10, Files.readAllLines(output.resolve("gold/cursos_resumen.csv")).size());
        assertEquals(121, Files.readAllLines(output.resolve("gold/estudiantes_resumen.csv")).size());
        assertTrue(Files.readString(output.resolve("gold/resumen.json")).contains("\"matriculas\" : 1080"));
        assertTrue(Files.readString(output.resolve("events.jsonl")).contains("\"stage\":\"EXPORT\",\"status\":\"COMPLETED\""));
    }

    @Test
    void dataWithErrorsIsQuarantinedAndPublishedWithFivePercentTolerance() throws Exception {
        Path output = directory.resolve("errors-out");

        PipelineResult result = new RendimientoAcademicoJob().pipeline().run(spark, ERRORS, output, 0.05);

        assertTrue(result.published());
        assertEquals(245, result.rejected());
        String quality = Files.readString(output.resolve("quality.json"));
        assertTrue(quality.contains("\"rule\" : \"NOTA_FUERA_DE_RANGO\""), quality);
        assertTrue(quality.contains("\"rule\" : \"ESTUDIANTE_INEXISTENTE\""), quality);
        assertTrue(quality.contains("\"rule\" : \"REGISTRO_DUPLICADO\""), quality);
        String quarantine = Files.readString(output.resolve("quarantine/rechazados.csv"));
        assertTrue(quarantine.contains("estudiantes,124,VG260999,SEMESTRE_INEXISTENTE"), quarantine);
    }

    @Test
    void dataWithErrorsIsBlockedWithOnePercentTolerance() throws Exception {
        Path output = directory.resolve("blocked-out");

        PipelineResult result = new RendimientoAcademicoJob().pipeline().run(spark, ERRORS, output, 0.01);

        assertFalse(result.published());
        assertFalse(Files.exists(output.resolve("gold/rendimiento.csv")));
        assertTrue(Files.isRegularFile(output.resolve("quarantine/rechazados.csv")));
        assertTrue(Files.readString(output.resolve("events.jsonl")).contains("\"stage\":\"QUALITY_GATE\",\"status\":\"BLOCKED\""));
    }
}
