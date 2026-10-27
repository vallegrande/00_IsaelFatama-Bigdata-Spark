package pe.edu.vallegrande.bigdata.api.model;

import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionTest {

    private static final Instant T0 = Instant.parse("2026-11-14T10:00:00Z");

    private static final Instant T1 = T0.plusSeconds(5);

    private static final Instant T2 = T0.plusSeconds(65);

    private final Execution pending = Execution.pending("abc", JobType.ACADEMIC_PERFORMANCE, 0.05, null, T0);

    @Test
    void exitCodesMapToTheWorkerProtocol() {
        assertEquals(ExecutionStatus.PUBLISHED, ExecutionStatus.fromExitCode(0));
        assertEquals(ExecutionStatus.FAILED, ExecutionStatus.fromExitCode(1));
        assertEquals(ExecutionStatus.BLOCKED, ExecutionStatus.fromExitCode(2));
        assertEquals(ExecutionStatus.FAILED, ExecutionStatus.fromExitCode(137));
    }

    @Test
    void onlyPendingAndRunningAreNotFinished() {
        assertFalse(ExecutionStatus.PENDING.isFinished());
        assertFalse(ExecutionStatus.RUNNING.isFinished());
        assertTrue(ExecutionStatus.PUBLISHED.isFinished());
        assertTrue(ExecutionStatus.BLOCKED.isFinished());
        assertTrue(ExecutionStatus.FAILED.isFinished());
    }

    @Test
    void anExecutionMovesThroughItsLifecycleWithoutMutating() {
        Execution running = pending.running(T1);
        Execution done = running.finished(0, T2);

        assertEquals(ExecutionStatus.PENDING, pending.status());
        assertEquals(ExecutionStatus.RUNNING, running.status());
        assertEquals(T1, running.startedAt());
        assertEquals(ExecutionStatus.PUBLISHED, done.status());
        assertEquals(T1, done.startedAt());
        assertEquals(T2, done.finishedAt());
        assertEquals(0, done.exitCode());
        assertNull(done.message());
        assertTrue(done.isPublished());
    }

    @Test
    void aBlockedRunKeepsItsExitCodeAndAFailedOneExplainsWhy() {
        assertEquals(ExecutionStatus.BLOCKED, pending.running(T1).finished(2, T2).status());

        Execution failed = pending.running(T1).finished(1, T2);
        assertEquals(ExecutionStatus.FAILED, failed.status());
        assertNotNull(failed.message());
        assertFalse(failed.isPublished());
    }

    @Test
    void failedKeepsTheGivenReasonAndWithMessageOnlyChangesTheMessage() {
        Execution failed = pending.failed("sin jar", T1);

        assertEquals("sin jar", failed.message());
        assertEquals(ExecutionStatus.FAILED, failed.status());
        assertEquals("otro", failed.withMessage("otro").message());
        assertEquals(ExecutionStatus.FAILED, failed.withMessage("otro").status());
    }

    @Test
    void pendingCarriesItsDefinition() {
        Execution withSource = Execution.pending("x", JobType.DATA_LOAD, 0, "origen", T0);

        assertEquals("origen", withSource.sourceExecutionId());
        assertEquals(JobType.DATA_LOAD, withSource.jobType());
        assertEquals(T0, withSource.createdAt());
        assertNull(withSource.startedAt());
    }

}
