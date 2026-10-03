package pe.edu.vallegrande.bigdata.spark.core.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;

import java.io.IOException;
import java.util.Map;

public final class SilverStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.SILVER;
    }

    @Override
    public String description() {
        return "Las filas válidas se guardan tipadas en CSV y Parquet";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long rows = 0;
        for (Map.Entry<String, Dataset<Row>> table : context.validation().silverTables().entrySet()) {
            rows += context.publisher().publish(WorkerProtocol.SILVER, table.getKey(), table.getValue());
        }
        return StepResult.completed(rows, rows + " filas limpias en " + context.validation().silverTables().size() + " tablas");
    }
}
