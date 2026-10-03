package pe.edu.vallegrande.bigdata.spark.academico.extraction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public final class CsvExtractor {
    public static final String ROW = "fila";

    public BronzeTables read(SparkSession spark, Path inputDirectory) {
        Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
        Map<String, Long> rows = new LinkedHashMap<>();
        for (String table : AcademicSchema.TABLES) {
            Path file = inputDirectory.resolve(table + ".csv");
            if (!Files.isRegularFile(file)) {
                throw new IllegalArgumentException("Falta la tabla " + file.getFileName() + " en " + inputDirectory);
            }
            Dataset<Row> frame = spark.read()
                    .schema(textSchema(table))
                    .option("header", "true")
                    .option("encoding", "UTF-8")
                    .option("mode", "PERMISSIVE")
                    .csv(file.toString());
            for (String column : AcademicSchema.columns(table)) {
                frame = frame.withColumn(column, expr("nullif(trim(`" + column + "`), '')"));
            }
            frame = frame
                    .withColumn("_orden", monotonically_increasing_id())
                    .withColumn(ROW, row_number().over(Window.orderBy(col("_orden"))).plus(1))
                    .drop("_orden")
                    .cache();
            tables.put(table, frame);
            rows.put(table, frame.count());
        }
        return new BronzeTables(tables, rows);
    }

    private StructType textSchema(String table) {
        StructType schema = new StructType();
        for (String column : AcademicSchema.columns(table)) {
            schema = schema.add(column, DataTypes.StringType, true);
        }
        return schema;
    }
}
