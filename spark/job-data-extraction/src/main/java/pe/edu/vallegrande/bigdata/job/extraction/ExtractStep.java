package pe.edu.vallegrande.bigdata.job.extraction;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.PipelineStage;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.bigdata.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

final class ExtractStep implements PipelineStep {

    static final DatabaseLayer SOURCE_LAYER = DatabaseLayer.STAGING;

    private final Supplier<Database> database;

    ExtractStep(Supplier<Database> database) {
        this.database = database;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.EXTRACT;
    }

    @Override
    public String description() {
        return "Lectura de las cinco tablas desde la base de datos (" + SOURCE_LAYER + ") hacia archivos";
    }

    @Override
    public StepResult execute(PipelineContext context) throws Exception {
        Database db = database.get();
        Map<String, Long> rowsByTable = new LinkedHashMap<>();
        long total = 0;
        for (String table : AcademicSchema.TABLES) {
            Dataset<Row> data = DatabaseNaming.toLake(db.read(context.spark(), TableRef.of(SOURCE_LAYER, table)));
            long rows = context.publisher().publish(WorkerProtocol.LANDING, table, contractColumns(table, data));
            rowsByTable.put(table, rows);
            total += rows;
        }
        context.counts(total, 0);
        String detail = rowsByTable.entrySet().stream()
                .map(entry -> entry.getKey() + " " + entry.getValue())
                .collect(Collectors.joining(", "));
        return StepResult.completed(total, "Filas extraídas: " + detail, Map.of(
                "layer", SOURCE_LAYER.name(),
                "outputFolder", WorkerProtocol.LANDING,
                "rowsByTable", rowsByTable,
                "totalRows", total));
    }

    private Dataset<Row> contractColumns(String table, Dataset<Row> data) {
        List<String> available = List.of(data.columns());
        List<Column> selected = new ArrayList<>();
        for (String column : AcademicSchema.columns(table)) {
            if (!available.contains(column)) {
                throw new IllegalStateException("La tabla " + SOURCE_LAYER + "." + table + " no tiene la columna " + column);
            }
            selected.add(col(column));
        }
        return data.select(selected.toArray(Column[]::new));
    }

}
