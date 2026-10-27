package pe.edu.vallegrande.bigdata.job.performance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.core.step.ExportStep;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldAnalytics;
import pe.edu.vallegrande.bigdata.spark.core.step.GoldStep;
import pe.edu.vallegrande.bigdata.spark.academic.step.BronzeStep;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AcademicPerformanceJobUnitTest {
    private GoldAnalytics analytics;
    private AcademicPerformanceJob job;

    @BeforeEach
    void setUp() {
        analytics = mock(GoldAnalytics.class);
        job = new AcademicPerformanceJob(analytics);
    }

    @Test
    void declaresTheRendimientoJobType() {
        assertEquals(JobType.ACADEMIC_PERFORMANCE, job.type());
    }

    @Test
    void composesTheSevenPipelineStagesInContractOrder() {
        assertEquals(List.of(PipelineStage.BRONZE, PipelineStage.VALIDATION, PipelineStage.QUARANTINE,
                        PipelineStage.SILVER, PipelineStage.QUALITY_GATE, PipelineStage.GOLD, PipelineStage.EXPORT),
                job.steps().stream().map(PipelineStep::stage).toList());
    }

    @Test
    void startsWithBronzeAndEndsWithExport() {
        List<PipelineStep> steps = job.steps();
        assertInstanceOf(BronzeStep.class, steps.get(0));
        assertInstanceOf(ExportStep.class, steps.get(steps.size() - 1));
    }

    @Test
    void goldStepUsesTheInjectedAnalyticsDescription() {
        when(analytics.description()).thenReturn("Indicadores de rendimiento académico");

        GoldStep goldStep = (GoldStep) job.steps().get(5);

        assertEquals("Indicadores de rendimiento académico", goldStep.description());
        verify(analytics).description();
    }

    @Test
    @SuppressWarnings("unchecked")
    void goldStepAnalyzesSilverAndStoresTheProducedTables() throws Exception {
        PipelineContext context = mock(PipelineContext.class);
        ValidationResult validation = mock(ValidationResult.class);
        GoldTables gold = mock(GoldTables.class);
        Dataset<Row> primaryTable = mock(Dataset.class);
        when(context.validation()).thenReturn(validation);
        when(analytics.analyze(validation)).thenReturn(gold);
        when(gold.primaryTable()).thenReturn(primaryTable);
        when(gold.primary()).thenReturn("rendimiento");
        when(gold.tables()).thenReturn(Map.of("rendimiento", primaryTable));
        when(gold.summaryValues()).thenReturn(Map.of("matriculas", 1080));
        when(primaryTable.count()).thenReturn(1080L);
        when(analytics.detail(1080L)).thenReturn("1080 matrículas procesadas");

        StepResult result = ((GoldStep) job.steps().get(5)).execute(context);

        assertEquals(1080L, result.rows());
        assertTrue(result.proceed());
        assertEquals("1080 matrículas procesadas", result.detail());
        verify(analytics).analyze(validation);
        verify(context).gold(gold);
    }
}
