package pe.edu.vallegrande.bigdata.api.repository.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.StageStatus;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRepositoriesTest {

    private static final String ID = "run-0001";

    @TempDir
    Path root;

    ExecutionWorkspace workspace;

    FileExecutionRepository executions;

    FileGoldTableRepository gold;

    FileExecutionOutputRepository outputs;

    @BeforeEach
    void setUp() {
        workspace = TestSupport.workspace(root);
        executions = new FileExecutionRepository(workspace, TestSupport.mapper());
        gold = new FileGoldTableRepository(workspace, TestSupport.mapper());
        outputs = new FileExecutionOutputRepository(workspace, TestSupport.mapper());
    }

    @Test
    void savesAndReadsAnExecution() {
        Execution execution = TestSupport.pending("abc", JobType.ATTENDANCE_ALERT, TestSupport.NOW);

        StepVerifier.create(executions.save(execution).then(executions.findById("abc")))
                .expectNext(execution)
                .verifyComplete();
    }

    @Test
    void savingAgainReplacesTheState() {
        Execution pending = TestSupport.pending("abc", JobType.ATTENDANCE_ALERT, TestSupport.NOW);

        StepVerifier.create(executions.save(pending)
                        .then(executions.save(pending.running(TestSupport.NOW.plusSeconds(1))))
                        .then(executions.findById("abc")))
                .assertNext(found -> assertEquals("RUNNING", found.status().name()))
                .verifyComplete();
    }

    @Test
    void anUnknownExecutionIsEmpty() {
        StepVerifier.create(executions.findById("no-existe")).verifyComplete();
    }

    @Test
    void listsNewestFirstAndFindsTheLatestPublished() {
        Execution old = TestSupport.published("old", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW);
        Execution recent = TestSupport.published("recent", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW.plusSeconds(60));
        Execution blocked = TestSupport.pending("blocked", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW.plusSeconds(120)).running(TestSupport.NOW).finished(2, TestSupport.NOW);
        Execution otherJob = TestSupport.published("other", JobType.ATTENDANCE_ALERT, TestSupport.NOW.plusSeconds(180));

        StepVerifier.create(Flux.just(old, recent, blocked, otherJob).concatMap(executions::save).thenMany(executions.findAll()))
                .expectNextMatches(execution -> execution.id().equals("other"))
                .expectNextMatches(execution -> execution.id().equals("blocked"))
                .expectNextMatches(execution -> execution.id().equals("recent"))
                .expectNextMatches(execution -> execution.id().equals("old"))
                .verifyComplete();

        StepVerifier.create(executions.findLatestPublished(JobType.ACADEMIC_PERFORMANCE))
                .expectNextMatches(execution -> execution.id().equals("recent"))
                .verifyComplete();
    }

    @Test
    void withoutAnyExecutionTheListIsEmpty() {
        StepVerifier.create(executions.findAll()).verifyComplete();
    }

    @Test
    void aCorruptedFileDoesNotBreakTheListing() throws IOException {
        Path broken = Files.createDirectories(workspace.outputDirectory("roto")).resolve("execution.json");
        Files.writeString(broken, "{esto no es json");
        Execution good = TestSupport.pending("bueno", JobType.ATTENDANCE_ALERT, TestSupport.NOW);

        StepVerifier.create(executions.save(good).thenMany(executions.findAll()))
                .expectNext(good)
                .verifyComplete();
    }

    @Test
    void directoriesWithoutAnExecutionFileAreIgnored() throws IOException {
        Files.createDirectories(workspace.outputDirectory("muestra-antigua"));

        StepVerifier.create(executions.findAll()).verifyComplete();
    }

    @Test
    void readsAPageOfAGoldTableAndStripsTheBom() throws IOException {
        writeGold("cursos_resumen.csv", "﻿curso_id,curso,promedio\nAPS,Proyectos,15.2\nTBD,Big Data,\nFIA,IA,14\n");

        StepVerifier.create(gold.findPage(ID, "cursos_resumen", PageQuery.of(0, 2)))
                .assertNext(page -> {
                    assertEquals(2, page.content().size());
                    assertEquals(3, page.totalElements());
                    assertEquals(2, page.totalPages());
                    assertEquals("APS", page.content().get(0).get("curso_id"));
                    assertEquals(new BigDecimal("15.2"), page.content().get(0).get("promedio"));
                    assertNull(page.content().get(1).get("promedio"));
                })
                .verifyComplete();
    }

    @Test
    void numbersAreReturnedAsNumbersAndCodesStayAsText() throws IOException {
        writeGold("mezcla.csv", "codigo,periodo,cantidad,promedio,negativo,largo,cientifica,activo,inactivo,otro\n00018310,2026-II,119,15.26,-3,1234567890123456789,1E5,true,false,True\n");

        StepVerifier.create(gold.findPage(ID, "mezcla", PageQuery.of(0, 10)))
                .assertNext(page -> {
                    var row = page.content().get(0);
                    assertEquals("00018310", row.get("codigo"));
                    assertEquals("2026-II", row.get("periodo"));
                    assertEquals(119L, row.get("cantidad"));
                    assertEquals(new BigDecimal("15.26"), row.get("promedio"));
                    assertEquals(-3L, row.get("negativo"));
                    assertEquals("1234567890123456789", row.get("largo"));
                    assertEquals("1E5", row.get("cientifica"));
                    assertEquals(Boolean.TRUE, row.get("activo"));
                    assertEquals(Boolean.FALSE, row.get("inactivo"));
                    assertEquals("True", row.get("otro"));
                })
                .verifyComplete();
    }

    @Test
    void readsTheSecondPage() throws IOException {
        writeGold("cursos_resumen.csv", "curso_id,curso\nA,1\nB,2\nC,3\n");

        StepVerifier.create(gold.findPage(ID, "cursos_resumen", PageQuery.of(1, 2)))
                .assertNext(page -> {
                    assertEquals(1, page.content().size());
                    assertEquals("C", page.content().get(0).get("curso_id"));
                })
                .verifyComplete();
    }

    @Test
    void aMissingGoldTableIsEmpty() {
        StepVerifier.create(gold.findPage(ID, "no_existe", PageQuery.of(0, 10))).verifyComplete();
    }

    @Test
    void aTableNameThatTriesToEscapeIsNeverRead() throws IOException {
        Files.writeString(Files.createDirectories(workspace.outputDirectory(ID)).resolve("quality.csv"), "a\n1\n");

        StepVerifier.create(gold.findPage(ID, "../quality", PageQuery.of(0, 10))).verifyComplete();
    }

    @Test
    void readsTheGoldSummary() throws IOException {
        Files.createDirectories(workspace.outputDirectory(ID).resolve("gold"));
        Files.writeString(workspace.outputDirectory(ID).resolve("gold/resumen.json"), "{\"estudiantes\":119}");

        StepVerifier.create(gold.findSummary(ID))
                .assertNext(summary -> assertEquals(119, summary.get("estudiantes").asInt()))
                .verifyComplete();
        StepVerifier.create(gold.findSummary("otro")).verifyComplete();
    }

    @Test
    void readsTheStageEvents() throws IOException {
        Files.createDirectories(workspace.outputDirectory(ID));
        Files.writeString(workspace.outputDirectory(ID).resolve("events.jsonl"), """
                {"stage":"BRONZE","status":"RUNNING","timestamp":"2026-11-14T10:00:00Z","rows":null,"durationMs":null,"sparkJobs":null,"detail":"lee"}
                {"stage":"BRONZE","status":"COMPLETED","timestamp":"2026-11-14T10:00:05Z","rows":21759,"durationMs":5000,"sparkJobs":15,"detail":"listo"}
                """);

        StepVerifier.create(outputs.findEvents(ID))
                .assertNext(event -> {
                    assertEquals(PipelineStage.BRONZE, event.stage());
                    assertEquals(StageStatus.RUNNING, event.status());
                })
                .assertNext(event -> assertEquals(21759L, event.rows()))
                .verifyComplete();
    }

    @Test
    void noEventsYetMeansAnEmptyList() {
        StepVerifier.create(outputs.findEvents(ID)).verifyComplete();
    }

    @Test
    void readsResultQualityAndReconciliation() throws IOException {
        Files.createDirectories(workspace.outputDirectory(ID));
        Files.writeString(workspace.outputDirectory(ID).resolve("result.json"), "{\"published\":true}");
        Files.writeString(workspace.outputDirectory(ID).resolve("quality.json"), "{\"rulesVersion\":\"2.0.0\"}");
        Files.writeString(workspace.outputDirectory(ID).resolve("reconciliation.json"), "{\"reconciled\":true}");

        StepVerifier.create(outputs.findResult(ID)).assertNext(node -> assertTrue(node.get("published").asBoolean())).verifyComplete();
        StepVerifier.create(outputs.findQuality(ID)).assertNext(node -> assertEquals("2.0.0", node.get("rulesVersion").asText())).verifyComplete();
        StepVerifier.create(outputs.findReconciliation(ID)).assertNext(node -> assertTrue(node.get("reconciled").asBoolean())).verifyComplete();
        StepVerifier.create(outputs.findQuality("otro")).verifyComplete();
    }

    @Test
    void readsThePagedQuarantine() throws IOException {
        Path quarantine = Files.createDirectories(workspace.outputDirectory(ID).resolve("quarantine"));
        Files.writeString(quarantine.resolve("rechazados.csv"), "﻿tabla,fila,motivo\nnotas,4,nota fuera de escala\nnotas,9,fecha inválida\n");

        StepVerifier.create(outputs.findRejected(ID, PageQuery.of(0, 1)))
                .assertNext(page -> {
                    assertEquals(2, page.totalElements());
                    assertEquals("nota fuera de escala", page.content().get(0).get("motivo"));
                })
                .verifyComplete();
        StepVerifier.create(outputs.findRejected("otro", PageQuery.of(0, 1))).verifyComplete();
    }

    private void writeGold(String file, String content) throws IOException {
        Path goldDirectory = Files.createDirectories(workspace.outputDirectory(ID).resolve("gold"));
        Files.writeString(goldDirectory.resolve(file), content, StandardCharsets.UTF_8);
    }

}
