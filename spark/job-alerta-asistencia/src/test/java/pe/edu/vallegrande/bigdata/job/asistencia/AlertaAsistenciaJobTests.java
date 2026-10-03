package pe.edu.vallegrande.bigdata.job.asistencia;

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

class AlertaAsistenciaJobTests {
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
    void classifiesEachEnrollmentByAbsenceRatio() throws Exception {
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
                E1,TBD,2026-II,EF,1,13,2026-12-07
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

        PipelineResult result = new AlertaAsistenciaJob().pipeline().run(spark, input, output, 0);

        assertTrue(result.published());
        assertEquals(3, result.goldRows());
        List<String> rows = Files.readAllLines(output.resolve("gold/alertas_asistencia.csv"));
        assertEquals("E3,Rosa,Díaz,B,TBD,Talleres de Big Data,2026-II,4,2,0,0.5,1,0,INHABILITADO", rows.get(1));
        assertEquals("E1,Ana,Torres,A,TBD,Talleres de Big Data,2026-II,4,1,0,0.25,1,0,EN_RIESGO", rows.get(2));
        assertEquals("E2,Luis,Rojas,A,TBD,Talleres de Big Data,2026-II,4,0,1,0,1,1,NORMAL", rows.get(3));
        String summary = Files.readString(output.resolve("gold/resumen.json"));
        assertTrue(summary.contains("\"inhabilitados\" : 1"), summary);
        assertTrue(summary.contains("\"en_riesgo\" : 1"), summary);
        assertTrue(summary.contains("\"estudiantes_con_alerta\" : 2"), summary);
    }

    @Test
    void cleanDataPublishesOneAlertRowPerEnrollment() throws Exception {
        Path output = directory.resolve("clean-out");

        PipelineResult result = new AlertaAsistenciaJob().pipeline().run(spark, CLEAN, output, 0.05);

        assertTrue(result.published());
        assertEquals(21735, result.received());
        assertEquals(1080, result.goldRows());
        assertEquals(10, Files.readAllLines(output.resolve("gold/alertas_por_curso.csv")).size());
        assertTrue(Files.isRegularFile(output.resolve("gold/alertas_asistencia.parquet")));
        assertFalse(Files.exists(output.resolve("gold/rendimiento.csv")));
    }

    @Test
    void sharesTheQualityGateWithTheAcademicBatch() throws Exception {
        Path output = directory.resolve("blocked-out");

        PipelineResult result = new AlertaAsistenciaJob().pipeline().run(spark, ERRORS, output, 0.01);

        assertFalse(result.published());
        assertEquals(245, result.rejected());
        assertFalse(Files.exists(output.resolve("gold/alertas_asistencia.csv")));
    }
}
