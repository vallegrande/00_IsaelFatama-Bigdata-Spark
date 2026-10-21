package pe.edu.vallegrande.bigdata.spark.core.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.JsonPublisher;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ExportStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.EXPORT;
    }

    @Override
    public String description() {
        return "Publicación de tablas Gold en CSV, Parquet y resumen JSON";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long files = 0;
        Map<String, Long> rowsByTable = new LinkedHashMap<>();
        for (Map.Entry<String, Dataset<Row>> table : context.gold().tables().entrySet()) {
            long rows = context.publisher().publish(WorkerProtocol.GOLD, table.getKey(), table.getValue());
            rowsByTable.put(table.getKey(), rows);
            files += context.publisher().formatsPerTable();
        }
        var summary = context.gold().summaryValues();
        JsonPublisher.write(context.output().resolve(WorkerProtocol.SUMMARY_FILE), summary);
        long totalFiles = files + 1;
        return StepResult.completed(totalFiles, totalFiles + " archivos publicados", Map.of(
                "tables", rowsByTable.keySet(),
                "rowsByTable", rowsByTable,
                "formats", List.of("csv", "parquet", "json"),
                "fileCount", totalFiles,
                "summary", summary));
    }
}
