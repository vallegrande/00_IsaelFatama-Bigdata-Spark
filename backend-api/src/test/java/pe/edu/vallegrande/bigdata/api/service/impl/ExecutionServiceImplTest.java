package pe.edu.vallegrande.bigdata.api.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pe.edu.vallegrande.bigdata.api.component.ExecutionFinder;
import pe.edu.vallegrande.bigdata.api.component.ExecutionRunner;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.dto.request.CreateExecutionRequest;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.exception.WorkerUnavailableException;
import pe.edu.vallegrande.bigdata.api.mapper.ExecutionMapper;
import pe.edu.vallegrande.bigdata.api.mapper.JobMapper;
import pe.edu.vallegrande.bigdata.api.model.Execution;
import pe.edu.vallegrande.bigdata.api.model.ExecutionStatus;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;
import pe.edu.vallegrande.bigdata.api.worker.WorkerCommand;
import pe.edu.vallegrande.bigdata.api.worker.WorkerLauncher;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionServiceImplTest {

    @Mock
    WorkerLauncher launcher;

    @Mock
    ExecutionRunner runner;

    @TempDir
    Path root;

    TestSupport.InMemoryExecutions executions = new TestSupport.InMemoryExecutions();

    ExecutionWorkspace workspace;

    ExecutionServiceImpl service;

    @BeforeEach
    void setUp() {
        workspace = TestSupport.workspace(root);
        lenient().when(launcher.isAvailable(any())).thenReturn(true);
        service = new ExecutionServiceImpl(executions, launcher, runner, workspace, new ExecutionFinder(executions),
                new JobServiceImpl(new JobMapper()), new DatasetServiceImpl(workspace), new ExecutionMapper(),
                Clock.fixed(TestSupport.NOW, ZoneOffset.UTC));
    }

    @Test
    void aTransformationOfADatasetRegistersAPendingExecutionAndQueuesIt() throws IOException {
        dataset("limpio");

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, "limpio")))
                .assertNext(execution -> {
                    assertEquals(ExecutionStatus.PENDING, execution.status());
                    assertEquals(0.05, execution.threshold(), 0.0001);
                    assertEquals(TestSupport.NOW, execution.createdAt());
                    assertEquals(JobType.ACADEMIC_PERFORMANCE, execution.jobType());
                    assertTrue(ExecutionWorkspace.isValidId(execution.id()));
                })
                .verifyComplete();

        ArgumentCaptor<WorkerCommand> command = ArgumentCaptor.forClass(WorkerCommand.class);
        verify(runner).enqueue(any(Execution.class), command.capture());
        assertEquals(JobType.ACADEMIC_PERFORMANCE, command.getValue().jobType());
        assertEquals(workspace.datasetDirectory("limpio"), command.getValue().input());
        assertTrue(command.getValue().output().startsWith(workspace.jobsDirectory()));
    }

    @Test
    void theJobTypeAcceptsAnyCaseAndHyphens() throws IOException {
        dataset("limpio");

        StepVerifier.create(service.create(request("academic-performance", null, null, "limpio")))
                .assertNext(execution -> assertEquals(JobType.ACADEMIC_PERFORMANCE, execution.jobType()))
                .verifyComplete();
    }

    @Test
    void anExplicitToleranceIsUsed() throws IOException {
        dataset("limpio");

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", 0.2, null, "limpio")))
                .assertNext(execution -> assertEquals(0.2, execution.threshold(), 0.0001))
                .verifyComplete();
    }

    @Test
    void rejectsAToleranceOutsideZeroAndOne() throws IOException {
        dataset("limpio");

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", 1.5, null, "limpio"))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", -0.1, null, "limpio"))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", Double.NaN, null, "limpio"))).expectError(InvalidRequestException.class).verify();
    }

    @Test
    void theJobTypeIsRequiredAndMustExist() {
        StepVerifier.create(service.create(request(null, null, null, "limpio"))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request(" ", null, null, "limpio"))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request("inventado", null, null, "limpio"))).expectError(ResourceNotFoundException.class).verify();
    }

    @Test
    void aTransformationNeedsExactlyOneInputAndQueuesNothingOtherwise() {
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, null)))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof InvalidRequestException);
                    assertTrue(error.getMessage().contains("dataset"));
                })
                .verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, "extraccion", "limpio")))
                .expectError(InvalidRequestException.class).verify();

        verify(runner, never()).enqueue(any(), any());
    }

    @Test
    void anUnknownDatasetIsNotFoundAndAnIncompleteOneSaysWhatIsMissing() throws IOException {
        Files.createDirectories(workspace.datasetDirectory("incompleto"));

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, "no-existe")))
                .expectError(ResourceNotFoundException.class).verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, "incompleto")))
                .expectErrorSatisfies(error -> assertTrue(error.getMessage().contains("no tiene los archivos")))
                .verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, "../jobs")))
                .expectError(InvalidRequestException.class).verify();

        verify(runner, never()).enqueue(any(), any());
    }

    @Test
    void aTransformationCanReadTheLandingFolderOfAPublishedExtraction() {
        executions.save(TestSupport.published("extraccion", JobType.DATA_EXTRACTION, TestSupport.NOW)).block();

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, "extraccion", null)))
                .assertNext(execution -> assertEquals("extraccion", execution.sourceExecutionId()))
                .verifyComplete();

        ArgumentCaptor<WorkerCommand> command = ArgumentCaptor.forClass(WorkerCommand.class);
        verify(runner).enqueue(any(Execution.class), command.capture());
        assertEquals(workspace.outputDirectory("extraccion").resolve("landing"), command.getValue().input());
    }

    @Test
    void aTransformationCannotReadFromAnythingButAPublishedExtraction() {
        executions.save(TestSupport.published("otra-transformacion", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();
        executions.save(Execution.pending("extraccion-fallida", JobType.DATA_EXTRACTION, 0, null, TestSupport.NOW)
                .running(TestSupport.NOW).finished(1, TestSupport.NOW)).block();

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, "otra-transformacion", null)))
                .expectErrorSatisfies(error -> assertTrue(error.getMessage().contains("extracción publicada"))).verify();
        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, "extraccion-fallida", null)))
                .expectError(InvalidRequestException.class).verify();
        verify(runner, never()).enqueue(any(), any());
    }

    @Test
    void failsFastWhenTheWorkerIsNotBuilt() throws IOException {
        dataset("limpio");
        when(launcher.isAvailable(JobType.ACADEMIC_PERFORMANCE)).thenReturn(false);
        when(launcher.unavailableReason(JobType.ACADEMIC_PERFORMANCE)).thenReturn("compile el módulo");

        StepVerifier.create(service.create(request("ACADEMIC_PERFORMANCE", null, null, "limpio")))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof WorkerUnavailableException);
                    assertEquals("compile el módulo", error.getMessage());
                })
                .verify();

        verify(runner, never()).enqueue(any(), any());
    }

    @Test
    void anIngestionReadsTheChosenDatasetAndNothingElse() throws IOException {
        dataset("limpio");

        StepVerifier.create(service.create(request("DATA_INGESTION", null, null, "limpio")))
                .assertNext(execution -> assertEquals(JobType.DATA_INGESTION, execution.jobType()))
                .verifyComplete();

        ArgumentCaptor<WorkerCommand> command = ArgumentCaptor.forClass(WorkerCommand.class);
        verify(runner).enqueue(any(Execution.class), command.capture());
        assertEquals(workspace.datasetDirectory("limpio"), command.getValue().input());
    }

    @Test
    void anIngestionNeedsADatasetAndRejectsASourceExecution() {
        StepVerifier.create(service.create(request("DATA_INGESTION", null, null, null))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request("DATA_INGESTION", null, "otra", "limpio"))).expectError(InvalidRequestException.class).verify();
    }

    @Test
    void anExtractionNeedsNoInputAndIsStrict() {
        StepVerifier.create(service.create(request("DATA_EXTRACTION", null, null, null)))
                .assertNext(execution -> assertEquals(0.0, execution.threshold(), 0.0001))
                .verifyComplete();

        ArgumentCaptor<WorkerCommand> command = ArgumentCaptor.forClass(WorkerCommand.class);
        verify(runner).enqueue(any(Execution.class), command.capture());
        assertEquals(command.getValue().output(), command.getValue().input());
    }

    @Test
    void anExtractionRejectsAnyInput() {
        StepVerifier.create(service.create(request("DATA_EXTRACTION", null, null, "limpio"))).expectError(InvalidRequestException.class).verify();
        StepVerifier.create(service.create(request("DATA_EXTRACTION", null, "otra", null))).expectError(InvalidRequestException.class).verify();
    }

    @Test
    void aLoadNeedsASourceExecutionAndNoDataset() {
        StepVerifier.create(service.create(request("DATA_LOAD", null, null, null)))
                .expectErrorSatisfies(error -> assertTrue(error.getMessage().contains("sourceExecutionId")))
                .verify();
        StepVerifier.create(service.create(request("DATA_LOAD", null, "origen", "limpio"))).expectError(InvalidRequestException.class).verify();
    }

    @Test
    void aLoadFromAnUnknownExecutionIsNotFound() {
        StepVerifier.create(service.create(request("DATA_LOAD", null, "no-existe", null)))
                .expectError(ResourceNotFoundException.class).verify();
    }

    @Test
    void aLoadFromAnExecutionThatWasNotPublishedIsRejected() {
        executions.save(TestSupport.pending("origen", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW).running(TestSupport.NOW).finished(2, TestSupport.NOW)).block();

        StepVerifier.create(service.create(request("DATA_LOAD", null, "origen", null)))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof InvalidRequestException);
                    assertTrue(error.getMessage().contains("transformación publicada"));
                })
                .verify();
    }

    @Test
    void aReconciliationFromALoadIsRejected() {
        executions.save(TestSupport.published("carga", JobType.DATA_LOAD, TestSupport.NOW)).block();

        StepVerifier.create(service.create(request("DATA_RECONCILIATION", null, "carga", null)))
                .expectError(InvalidRequestException.class).verify();
    }

    @Test
    void aLoadReadsTheOutputFolderOfItsSourceExecution() {
        executions.save(TestSupport.published("origen", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();

        StepVerifier.create(service.create(request("DATA_LOAD", null, "origen", null)))
                .assertNext(execution -> assertEquals("origen", execution.sourceExecutionId()))
                .verifyComplete();

        ArgumentCaptor<WorkerCommand> command = ArgumentCaptor.forClass(WorkerCommand.class);
        verify(runner).enqueue(any(Execution.class), command.capture());
        assertEquals(workspace.outputDirectory("origen"), command.getValue().input());
    }

    @Test
    void findByIdReturnsTheExecutionOrNotFound() {
        executions.save(TestSupport.published("abc", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();

        StepVerifier.create(service.findById("abc")).expectNextMatches(response -> response.id().equals("abc")).verifyComplete();
        StepVerifier.create(service.findById("zzz")).expectError(ResourceNotFoundException.class).verify();
    }

    @Test
    void findAllFiltersByJobAndStatus() {
        executions.save(TestSupport.published("uno", JobType.ACADEMIC_PERFORMANCE, TestSupport.NOW)).block();
        executions.save(TestSupport.pending("dos", JobType.ATTENDANCE_ALERT, TestSupport.NOW.plusSeconds(1))).block();

        StepVerifier.create(service.findAll(null, null)).expectNextCount(2).verifyComplete();
        StepVerifier.create(service.findAll("attendance-alert", null)).expectNextMatches(e -> e.id().equals("dos")).verifyComplete();
        StepVerifier.create(service.findAll(null, "published")).expectNextMatches(e -> e.id().equals("uno")).verifyComplete();
        StepVerifier.create(service.findAll("ATTENDANCE_ALERT", "PUBLISHED")).verifyComplete();
        StepVerifier.create(service.findAll(null, "inventado")).expectError(InvalidRequestException.class).verify();
    }

    private CreateExecutionRequest request(String jobType, Double threshold, String source, String dataset) {
        return new CreateExecutionRequest(jobType, threshold, source, dataset);
    }

    private void dataset(String name) throws IOException {
        Path directory = Files.createDirectories(workspace.datasetDirectory(name));
        for (String table : AcademicSchema.TABLES) {
            Files.writeString(directory.resolve(table + ".csv"), "x\n");
        }
    }

}
