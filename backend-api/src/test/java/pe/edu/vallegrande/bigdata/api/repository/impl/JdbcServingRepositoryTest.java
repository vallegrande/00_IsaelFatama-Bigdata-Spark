package pe.edu.vallegrande.bigdata.api.repository.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import pe.edu.vallegrande.bigdata.api.config.DatabaseProperties;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.exception.DatabaseUnavailableException;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcServingRepositoryTest {

    private JdbcTemplate jdbc;

    private JdbcServingRepository repository;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE SERVING_RESUMEN (JOB_TYPE VARCHAR2(200), CONTENT VARCHAR2(4000), ROW_NO NUMBER(10), "
                + "BATCH_ID VARCHAR2(200), LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
        jdbc.execute("CREATE TABLE SERVING_ALERTAS_POR_CURSO (CURSO_ID VARCHAR2(200), MATRICULAS NUMBER(19), EN_RIESGO NUMBER(19), "
                + "INASISTENCIA_PROMEDIO BINARY_DOUBLE, ACTIVO NUMBER(1), FECHA DATE, ROW_NO NUMBER(10), "
                + "BATCH_ID VARCHAR2(200), LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
        repository = new JdbcServingRepository(jdbc, new DatabaseProperties(null), new ObjectMapper());
    }

    @Test
    void theCurrentBatchIsTheLastOneLoadedForTheJob() {
        jdbc.update("INSERT INTO SERVING_RESUMEN (JOB_TYPE, CONTENT, ROW_NO, BATCH_ID, LOADED_AT) VALUES (?, ?, 1, ?, TIMESTAMP '2026-11-14 10:00:00')",
                "ATTENDANCE_ALERT", "{\"alumnos\":1}", "viejo");
        jdbc.update("INSERT INTO SERVING_RESUMEN (JOB_TYPE, CONTENT, ROW_NO, BATCH_ID, LOADED_AT) VALUES (?, ?, 1, ?, TIMESTAMP '2026-11-15 10:00:00')",
                "ATTENDANCE_ALERT", "{\"alumnos\":2}", "nuevo");
        jdbc.update("INSERT INTO SERVING_RESUMEN (JOB_TYPE, CONTENT, ROW_NO, BATCH_ID, LOADED_AT) VALUES (?, ?, 1, ?, TIMESTAMP '2026-11-16 10:00:00')",
                "ACADEMIC_PERFORMANCE", "{\"alumnos\":9}", "otro-job");

        StepVerifier.create(repository.findCurrent(JobType.ATTENDANCE_ALERT))
                .assertNext(snapshot -> {
                    assertEquals("nuevo", snapshot.batchId());
                    assertEquals(2, snapshot.summary().get("alumnos").asInt());
                    assertEquals(LocalDateTime.of(2026, 11, 15, 10, 0).atZone(ZoneId.systemDefault()).toInstant(), snapshot.loadedAt());
                })
                .verifyComplete();
    }

    @Test
    void withoutLoadsThereIsNoCurrentBatch() {
        StepVerifier.create(repository.findCurrent(JobType.ATTENDANCE_ALERT)).verifyComplete();
    }

    @Test
    void aPageKeepsTheOrderOfTheRowsAndHidesTechnicalColumns() {
        insertCourse("A", 1, "lote-1");
        insertCourse("B", 2, "lote-1");
        insertCourse("C", 3, "lote-1");
        insertCourse("X", 1, "lote-2");

        StepVerifier.create(repository.findPage("lote-1", "alertas_por_curso", PageQuery.of(0, 2)))
                .assertNext(page -> {
                    assertEquals(3, page.totalElements());
                    assertEquals(2, page.totalPages());
                    assertEquals(2, page.content().size());
                    assertEquals("A", page.content().get(0).get("curso_id"));
                    assertEquals("B", page.content().get(1).get("curso_id"));
                    assertFalse(page.content().get(0).containsKey("batch_id"));
                    assertFalse(page.content().get(0).containsKey("row_no"));
                    assertFalse(page.content().get(0).containsKey("loaded_at"));
                })
                .verifyComplete();
        StepVerifier.create(repository.findPage("lote-1", "alertas_por_curso", PageQuery.of(1, 2)))
                .assertNext(page -> assertEquals("C", page.content().get(0).get("curso_id")))
                .verifyComplete();
    }

    @Test
    void valuesComeBackWithTheirJsonTypes() {
        insertCourse("A", 1, "lote-1");

        StepVerifier.create(repository.findPage("lote-1", "alertas_por_curso", PageQuery.of(0, 10)))
                .assertNext(page -> {
                    var row = page.content().get(0);
                    assertEquals(40L, row.get("matriculas"));
                    assertEquals(0.25, row.get("inasistencia_promedio"));
                    assertEquals(true, row.get("activo"));
                    assertTrue(row.get("fecha").toString().startsWith("2026-08-17"), row.get("fecha").toString());
                })
                .verifyComplete();
    }

    @Test
    void aTableOutsideTheServingLayerIsRejectedBeforeAnyQuery() {
        StepVerifier.create(repository.findPage("lote-1", "x; DROP TABLE Y", PageQuery.of(0, 10)))
                .expectError(InvalidRequestException.class).verify();
    }

    @Test
    void aDatabaseFailureBecomesAnUnavailableError() {
        jdbc.execute("DROP TABLE SERVING_RESUMEN");

        StepVerifier.create(repository.findCurrent(JobType.ATTENDANCE_ALERT))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof DatabaseUnavailableException);
                    assertTrue(error.getMessage().contains("No se pudo consultar la base de datos"));
                })
                .verify();
    }

    private void insertCourse(String course, int rowNo, String batch) {
        jdbc.update("INSERT INTO SERVING_ALERTAS_POR_CURSO (CURSO_ID, MATRICULAS, EN_RIESGO, INASISTENCIA_PROMEDIO, ACTIVO, FECHA, ROW_NO, BATCH_ID) "
                + "VALUES (?, 40, 3, 0.25, 1, DATE '2026-08-17', ?, ?)", course, rowNo, batch);
    }

}
