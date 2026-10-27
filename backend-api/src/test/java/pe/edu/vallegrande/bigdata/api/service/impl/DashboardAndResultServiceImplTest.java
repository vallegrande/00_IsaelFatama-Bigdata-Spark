package pe.edu.vallegrande.bigdata.api.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.api.component.ExecutionFinder;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.mapper.ExecutionMapper;
import pe.edu.vallegrande.bigdata.api.mapper.JobMapper;
import pe.edu.vallegrande.bigdata.api.repository.ExecutionOutputRepository;
import pe.edu.vallegrande.bigdata.api.model.ServingSnapshot;
import pe.edu.vallegrande.bigdata.api.repository.GoldTableRepository;
import pe.edu.vallegrande.bigdata.api.repository.ServingRepository;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DashboardAndResultServiceImplTest {

    private static final PageQuery FIRST = PageQuery.of(0, 10);

    private final TestSupport.InMemoryExecutions executions = new TestSupport.InMemoryExecutions();

    private final GoldTableRepository gold = mock(GoldTableRepository.class);

    private final ServingRepository serving = mock(ServingRepository.class);

    private final ExecutionOutputRepository outputs = mock(ExecutionOutputRepository.class);

    private final JsonNode summary = TestSupport.mapper().createObjectNode().put("estudiantes", 119);

    private DashboardServiceImpl dashboard;

    private ExecutionResultServiceImpl results;

    @BeforeEach
    void setUp() {
        dashboard = new DashboardServiceImpl(new JobServiceImpl(new JobMapper()), serving);
        results = new ExecutionResultServiceImpl(new ExecutionFinder(executions), outputs, gold, new ExecutionMapper());
    }

    @Test
    void servesTheCurrentBatchLoadedInTheDatabase() {
        when(serving.findCurrent(JobType.ACADEMIC_PERFORMANCE))
                .thenReturn(Mono.just(new ServingSnapshot("nuevo", TestSupport.NOW, summary)));

        StepVerifier.create(dashboard.findSummary("academic-performance"))
                .assertNext(response -> {
                    assertEquals("nuevo", response.executionId());
                    assertEquals(JobType.ACADEMIC_PERFORMANCE, response.jobType());
                    assertEquals(TestSupport.NOW, response.publishedAt());
                    assertEquals(119, response.data().get("estudiantes").asInt());
                })
                .verifyComplete();
    }

    @Test
    void withoutALoadedBatchTheDashboardExplainsWhatToRun() {
        when(serving.findCurrent(JobType.ATTENDANCE_ALERT)).thenReturn(Mono.empty());

        StepVerifier.create(dashboard.findSummary("ATTENDANCE_ALERT"))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof ResourceNotFoundException);
                    assertTrue(error.getMessage().contains("Aún no hay datos cargados"));
                    assertTrue(error.getMessage().contains("DATA_LOAD"));
                })
                .verify();
    }

    @Test
    void servesAPageOfATableOfTheCurrentBatch() {
        when(serving.findCurrent(JobType.ATTENDANCE_ALERT))
                .thenReturn(Mono.just(new ServingSnapshot("lote-1", TestSupport.NOW, summary)));
        PageResponse<Map<String, Object>> page = PageResponse.of(List.of(Map.of("curso_id", "TBD")), FIRST, 1);
        when(serving.findPage("lote-1", "alertas_por_curso", FIRST)).thenReturn(Mono.just(page));

        StepVerifier.create(dashboard.findTable("ATTENDANCE_ALERT", "alertas_por_curso", FIRST))
                .assertNext(response -> {
                    assertEquals("lote-1", response.executionId());
                    assertEquals("TBD", response.data().content().get(0).get("curso_id"));
                    assertEquals(TestSupport.NOW, response.publishedAt());
                })
                .verifyComplete();
    }

    @Test
    void aTableThatTheJobDoesNotProduceIsNotFound() {
        StepVerifier.create(dashboard.findTable("ATTENDANCE_ALERT", "rendimiento", FIRST))
                .expectError(ResourceNotFoundException.class).verify();
        verify(serving, never()).findPage(any(), any(), any());
    }

    @Test
    void onlyTransformationJobsHaveADashboard() {
        StepVerifier.create(dashboard.findSummary("DATA_LOAD")).expectError(InvalidRequestException.class).verify();
    }

    @Test
    void anUnknownJobIsNotFound() {
        StepVerifier.create(dashboard.findSummary("inventado")).expectError(ResourceNotFoundException.class).verify();
    }

    @Test
    void listsTheTablesOfAJobWithoutNeedingAnExecution() {
        assertEquals(List.of("alertas_asistencia", "alertas_por_curso"), dashboard.findTables("attendance-alert"));
    }

    @Test
    void theDetailIncludesTheWorkerResultWhenItExists() {
        executions.save(TestSupport.published("lote-1", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();
        when(outputs.findResult("lote-1")).thenReturn(Mono.just(summary));

        StepVerifier.create(results.findDetail("lote-1"))
                .assertNext(detail -> {
                    assertEquals("lote-1", detail.execution().id());
                    assertEquals(119, detail.result().get("estudiantes").asInt());
                })
                .verifyComplete();
    }

    @Test
    void theDetailOfARunningExecutionHasNoResultYet() {
        executions.save(TestSupport.pending("lote-2", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();
        when(outputs.findResult("lote-2")).thenReturn(Mono.empty());

        StepVerifier.create(results.findDetail("lote-2"))
                .assertNext(detail -> assertNull(detail.result()))
                .verifyComplete();
    }

    @Test
    void goldOfAnExecutionThatWasNotPublishedIsNotFound() {
        executions.save(TestSupport.pending("lote-3", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW).running(TestSupport.NOW).finished(2, TestSupport.NOW)).block();

        StepVerifier.create(results.findGoldSummary("lote-3"))
                .expectErrorSatisfies(error -> assertTrue(error.getMessage().contains("BLOCKED")))
                .verify();
    }

    @Test
    void goldTableMustBelongToTheJobOfTheExecution() {
        executions.save(TestSupport.published("lote-4", JobType.ATTENDANCE_ALERT, TestSupport.NOW)).block();

        StepVerifier.create(results.findGoldTable("lote-4", "rendimiento", FIRST)).expectError(ResourceNotFoundException.class).verify();
        verify(gold, never()).findPage(any(), eq("rendimiento"), any());
    }

    @Test
    void anUnknownExecutionIsAlwaysNotFound() {
        StepVerifier.create(results.findQuality("fantasma")).expectError(ResourceNotFoundException.class).verify();
        StepVerifier.create(results.findEvents("fantasma")).expectError(ResourceNotFoundException.class).verify();
        StepVerifier.create(results.findRejected("fantasma", FIRST)).expectError(ResourceNotFoundException.class).verify();
        verify(outputs, never()).findQuality(any());
    }

    @Test
    void aMissingQualityReportExplainsItIsNotReadyYet() {
        executions.save(TestSupport.published("lote-5", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();
        when(outputs.findQuality("lote-5")).thenReturn(Mono.empty());

        StepVerifier.create(results.findQuality("lote-5"))
                .expectErrorSatisfies(error -> assertTrue(error.getMessage().contains("aún no tiene")))
                .verify();
    }

}
