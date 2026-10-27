package pe.edu.vallegrande.bigdata.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@AutoConfigureWebTestClient
class ApiFlowIntegrationTest {

    private static final Path DATASETS = Path.of("..", "data");

    private static final Path JOBS = createJobsDirectory();

    @DynamicPropertySource
    static void workspace(DynamicPropertyRegistry registry) {
        registry.add("bigdata.workspace.jobs-directory", JOBS::toString);
        registry.add("bigdata.workspace.datasets-directory", DATASETS::toString);
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:api-flow;MODE=Oracle;DB_CLOSE_DELAY=-1");
        registry.add("spring.datasource.username", () -> "sa");
        registry.add("spring.datasource.password", () -> "x");
    }

    private static Path createJobsDirectory() {
        try {
            return Files.createTempDirectory("bigdata-api-test");
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }

    @TestConfiguration
    static class FakeWorker {

        static final List<WorkerCommand> LAUNCHED = new CopyOnWriteArrayList<>();

        static volatile int exitCode = 0;

        @Bean
        @Primary
        WorkerLauncher fakeLauncher() {
            return new WorkerLauncher() {
                @Override
                public boolean isAvailable(JobType jobType) {
                    return true;
                }

                @Override
                public String unavailableReason(JobType jobType) {
                    return "";
                }

                @Override
                public Mono<Integer> launch(WorkerCommand command) {
                    return Mono.fromCallable(() -> {
                        LAUNCHED.add(command);
                        writeOutputs(command);
                        return exitCode;
                    });
                }
            };
        }

        private static void writeOutputs(WorkerCommand command) throws IOException {
            Path output = Files.createDirectories(command.output());
            Files.writeString(output.resolve("result.json"), "{\"published\":true,\"received\":21759,\"accepted\":21514,\"rejected\":245}");
            Files.writeString(output.resolve("quality.json"), "{\"rulesVersion\":\"2.0.0\",\"published\":true}");
            Files.writeString(output.resolve("events.jsonl"),
                    "{\"stage\":\"BRONZE\",\"status\":\"COMPLETED\",\"timestamp\":\"2026-11-14T10:00:05Z\",\"rows\":21759,\"durationMs\":5000,\"sparkJobs\":15,\"detail\":\"ok\"}\n");
            if (command.jobType() == JobType.ACADEMIC_PERFORMANCE && exitCode == 0) {
                Path gold = Files.createDirectories(output.resolve("gold"));
                Files.writeString(gold.resolve("resumen.json"), "{\"estudiantes\":119,\"periodo\":\"2026-II\"}");
                Files.writeString(gold.resolve("cursos_resumen.csv"), "﻿curso_id,curso,promedio,requiere_atencion\nAPS,Proyectos,15.26,true\nTBD,Big Data,14.9,false\nFIA,IA,,false\n");
            }
        }
    }

    @Autowired
    WebTestClient client;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ObjectMapper mapper;

    @Test
    void theCatalogListsEveryJobWithItsKindAndInput() {
        JsonNode jobs = get("/api/jobs");

        assertEquals(JobType.values().length, jobs.size());
        JsonNode performance = byField(jobs, "type", "ACADEMIC_PERFORMANCE");
        assertEquals("TRANSFORMATION", performance.get("kind").asText());
        assertEquals("CSV_FILES", performance.get("input").asText());
        assertTrue(performance.get("goldTables").toString().contains("cursos_resumen"));
        assertEquals("PREVIOUS_EXECUTION", byField(jobs, "type", "DATA_LOAD").get("input").asText());
        assertEquals("NONE", byField(jobs, "type", "DATA_EXTRACTION").get("input").asText());
    }

    @Test
    void aSingleJobCanBeFetchedWithAnyCaseOrHyphens() {
        assertEquals("ATTENDANCE_ALERT", get("/api/jobs/attendance-alert").get("type").asText());
        client.get().uri("/api/jobs/inventado").exchange().expectStatus().isNotFound()
                .expectBody().jsonPath("$.title").isEqualTo("No encontrado");
    }

    @Test
    void theAvailableDatasetsAreListed() {
        JsonNode datasets = get("/api/datasets");

        assertTrue(contains(datasets, "limpio"));
        assertTrue(contains(datasets, "con-errores"));
    }

    @Test
    void choosingADatasetRunsTheJobAndPublishesItToTheDashboard() {
        FakeWorker.exitCode = 0;
        String id = submitTransformation();

        awaitStatus(id, "PUBLISHED");

        JsonNode detail = get("/api/executions/" + id);
        assertEquals("PUBLISHED", detail.at("/execution/status").asText());
        assertEquals(21514, detail.at("/result/accepted").asInt());
        assertEquals(0, detail.at("/execution/exitCode").asInt());
        assertNotNull(detail.at("/execution/durationMs").numberValue());

        WorkerCommand launched = launchedFor(id);
        assertEquals("limpio", launched.input().getFileName().toString(), "el worker lee la carpeta del dataset elegido");

        assertEquals("BRONZE", get("/api/executions/" + id + "/events").get(0).get("stage").asText());
        assertEquals("2.0.0", get("/api/executions/" + id + "/quality").get("rulesVersion").asText());
        assertEquals(119, get("/api/executions/" + id + "/gold/summary").get("estudiantes").asInt());

        JsonNode page = get("/api/executions/" + id + "/gold/cursos_resumen?page=0&size=2");
        assertEquals(3, page.get("totalElements").asInt());
        assertEquals(2, page.get("totalPages").asInt());
        assertEquals("APS", page.at("/content/0/curso_id").asText());
        assertTrue(page.at("/content/0/requiere_atencion").isBoolean());

    }

    @Test
    void theDashboardReadsTheBatchLoadedInTheDatabase() {
        loadServingRows("lote-cargado");

        JsonNode dashboard = get("/api/dashboard/ACADEMIC_PERFORMANCE/tables/cursos_resumen?size=1");

        assertEquals("lote-cargado", dashboard.get("executionId").asText(), "el dashboard sirve el último lote cargado");
        assertEquals("Proyectos", dashboard.at("/data/content/0/curso").asText());
        assertTrue(dashboard.at("/data/content/0/requieren_atencion").isNumber());
        assertEquals(3, dashboard.at("/data/totalElements").asInt());
        assertEquals(119, get("/api/dashboard/ACADEMIC_PERFORMANCE/summary").at("/data/estudiantes").asInt());
        assertTrue(get("/api/dashboard/ACADEMIC_PERFORMANCE/tables").toString().contains("cursos_resumen"));
    }

    @Test
    void aDashboardWithoutLoadedDataTellsWhichJobToRun() {
        createServingTables();
        client.get().uri("/api/dashboard/ATTENDANCE_ALERT/summary").exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.detail").value(detail -> assertTrue(detail.toString().contains("DATA_LOAD")));
    }

    @Test
    void theExecutionListSupportsFilters() {
        FakeWorker.exitCode = 0;
        String id = submitTransformation();
        awaitStatus(id, "PUBLISHED");

        JsonNode published = get("/api/executions?jobType=academic-performance&status=published");

        assertTrue(containsId(published, id));
        assertFalse(containsId(get("/api/executions?jobType=ATTENDANCE_ALERT"), id));
        client.get().uri("/api/executions?status=inventado").exchange().expectStatus().isBadRequest();
    }

    @Test
    void aLoadCanBeChainedFromAPublishedTransformation() {
        FakeWorker.exitCode = 0;
        String source = submitTransformation();
        awaitStatus(source, "PUBLISHED");

        JsonNode load = post(body("jobType", "DATA_LOAD", "sourceExecutionId", source), 202);
        String loadId = load.get("id").asText();
        awaitStatus(loadId, "PUBLISHED");

        assertEquals(source, launchedFor(loadId).input().getFileName().toString(), "la carga lee la carpeta de salida de la transformación");
        assertEquals(source, get("/api/executions/" + loadId).at("/execution/sourceExecutionId").asText());
    }

    @Test
    void anExecutionWithoutInputIsRejectedWithAProblemDetail() {
        client.post().uri("/api/executions").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body("jobType", "ACADEMIC_PERFORMANCE"))
                .exchange().expectStatus().isBadRequest()
                .expectBody().jsonPath("$.title").isEqualTo("Petición inválida")
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.type").isEqualTo("about:blank")
                .jsonPath("$.instance").isEqualTo("/api/executions")
                .jsonPath("$.detail").value(detail -> assertTrue(detail.toString().contains("dataset")));
    }

    @Test
    void anUnknownDatasetIsNotFoundAndAnEscapingNameIsRejected() {
        post(body("jobType", "ACADEMIC_PERFORMANCE", "dataset", "no-existe"), 404);
        post(body("jobType", "ACADEMIC_PERFORMANCE", "dataset", "../jobs"), 400);
    }

    @Test
    void aRequestWithoutJobTypeOrBodyIsRejected() {
        post(body("dataset", "limpio"), 400);
        client.post().uri("/api/executions").contentType(MediaType.APPLICATION_JSON).exchange().expectStatus().isBadRequest();
    }

    @Test
    void uploadingFilesIsNoLongerSupported() {
        client.post().uri("/api/executions").contentType(MediaType.MULTIPART_FORM_DATA).bodyValue("x")
                .exchange().expectStatus().isEqualTo(415);
    }

    @Test
    void aLoadFromAnUnknownExecutionIsNotFound() {
        post(body("jobType", "DATA_LOAD", "sourceExecutionId", "no-existe"), 404);
    }

    @Test
    void unknownExecutionsAndTablesAreNotFound() {
        client.get().uri("/api/executions/no-existe").exchange().expectStatus().isNotFound();
        client.get().uri("/api/executions/no-existe/gold/summary").exchange().expectStatus().isNotFound();
        client.get().uri("/api/executions/no-existe/events").exchange().expectStatus().isNotFound();
    }

    @Test
    void anIdentifierThatTriesToEscapeTheWorkspaceIsRejected() {
        client.get().uri("/api/executions/{id}", "..%2F..%2Fsecreto").exchange().expectStatus().is4xxClientError();
        client.get().uri("/api/executions/{id}", "a b").exchange().expectStatus().isBadRequest();
    }

    @Test
    void thePageSizeIsBounded() {
        client.get().uri("/api/dashboard/ACADEMIC_PERFORMANCE/tables/cursos_resumen?size=100000").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/dashboard/ACADEMIC_PERFORMANCE/tables/cursos_resumen?page=-1").exchange().expectStatus().isBadRequest();
    }

    @Test
    void theDashboardOnlyServesTransformationJobs() {
        client.get().uri("/api/dashboard/DATA_LOAD/summary").exchange().expectStatus().isBadRequest();
        client.get().uri("/api/dashboard/inventado/summary").exchange().expectStatus().isNotFound();
        client.get().uri("/api/dashboard/ACADEMIC_PERFORMANCE/tables/no_existe").exchange().expectStatus().isNotFound();
    }

    @Test
    void aBlockedExecutionIsReportedAsBlockedAndNeverReachesTheDashboard() {
        FakeWorker.exitCode = 2;
        try {
            String id = submitTransformation();

            awaitStatus(id, "BLOCKED");

            client.get().uri("/api/executions/" + id + "/gold/summary").exchange().expectStatus().isNotFound();
            assertFalse(containsId(get("/api/executions?status=PUBLISHED"), id));
        } finally {
            FakeWorker.exitCode = 0;
        }
    }

    @Test
    void corsAllowsTheDashboardOrigin() {
        WebTestClient absolute = client.mutate().baseUrl("http://localhost:8080").build();

        absolute.options().uri("/api/jobs")
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "GET")
                .exchange().expectStatus().isOk()
                .expectHeader().valueEquals("Access-Control-Allow-Origin", "http://localhost:5173");
        absolute.options().uri("/api/jobs")
                .header("Origin", "http://sitio-malicioso.com")
                .header("Access-Control-Request-Method", "GET")
                .exchange().expectStatus().isForbidden();
    }

    @Test
    void actuatorHealthIsUp() {
        client.get().uri("/actuator/health").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
    }

    private String submitTransformation() {
        return post(body("jobType", "ACADEMIC_PERFORMANCE", "dataset", "limpio"), 202).get("id").asText();
    }

    private Map<String, Object> body(String... keysAndValues) {
        Map<String, Object> body = new LinkedHashMap<>();
        for (int index = 0; index < keysAndValues.length; index += 2) {
            body.put(keysAndValues[index], keysAndValues[index + 1]);
        }
        return body;
    }

    private WorkerCommand launchedFor(String id) {
        return FakeWorker.LAUNCHED.stream()
                .filter(command -> command.output().getFileName().toString().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private JsonNode post(Map<String, Object> body, int expectedStatus) {
        byte[] response = client.post().uri("/api/executions").contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isEqualTo(expectedStatus)
                .expectBody().returnResult().getResponseBody();
        return read(response);
    }

    private void createServingTables() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS SERVING_RESUMEN (JOB_TYPE VARCHAR2(200), CONTENT VARCHAR2(4000), ROW_NO NUMBER(10), "
                + "BATCH_ID VARCHAR2(200), LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS SERVING_CURSOS_RESUMEN (CURSO_ID VARCHAR2(200), CURSO VARCHAR2(200), PROMEDIO BINARY_DOUBLE, "
                + "REQUIEREN_ATENCION NUMBER(19), ROW_NO NUMBER(10), BATCH_ID VARCHAR2(200), LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
    }

    private void loadServingRows(String batchId) {
        createServingTables();
        jdbc.update("INSERT INTO SERVING_RESUMEN (JOB_TYPE, CONTENT, ROW_NO, BATCH_ID) VALUES ('ACADEMIC_PERFORMANCE', ?, 1, ?)",
                "{\"estudiantes\":119,\"periodo\":\"2026-II\"}", batchId);
        String[][] courses = {{"APS", "Proyectos", "15.26", "23"}, {"TBD", "Big Data", "14.9", "7"}, {"FIA", "IA", "13.1", "2"}};
        for (int index = 0; index < courses.length; index++) {
            jdbc.update("INSERT INTO SERVING_CURSOS_RESUMEN (CURSO_ID, CURSO, PROMEDIO, REQUIEREN_ATENCION, ROW_NO, BATCH_ID) VALUES (?, ?, ?, ?, ?, ?)",
                    courses[index][0], courses[index][1], Double.parseDouble(courses[index][2]), Long.parseLong(courses[index][3]), index + 1, batchId);
        }
    }

    private JsonNode get(String uri) {
        byte[] body = client.get().uri(uri).exchange().expectStatus().isOk().expectBody().returnResult().getResponseBody();
        return read(body);
    }

    private JsonNode read(byte[] body) {
        try {
            return mapper.readTree(body);
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }

    private void awaitStatus(String id, String status) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(status, get("/api/executions/" + id).at("/execution/status").asText()));
    }

    private JsonNode byField(JsonNode array, String field, String value) {
        for (JsonNode node : array) {
            if (value.equals(node.get(field).asText())) {
                return node;
            }
        }
        throw new AssertionError("No hay un elemento con " + field + "=" + value);
    }

    private boolean contains(JsonNode array, String value) {
        for (JsonNode node : array) {
            if (value.equals(node.asText())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsId(JsonNode array, String id) {
        for (JsonNode node : array) {
            if (id.equals(node.get("id").asText())) {
                return true;
            }
        }
        return false;
    }

}
