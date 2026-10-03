package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.QualityRule;
import pe.edu.vallegrande.bigdata.spark.academico.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public final class ValidationRules {
    public static final String REASONS = ValidationResult.REASONS;

    private ValidationRules() {
    }

    public static Column rule(Column condition, QualityRule rule) {
        return when(coalesce(condition, lit(false)), lit(rule.name()));
    }

    public static Column invalidPositive(Column value) {
        return coalesce(value.leq(0), lit(true));
    }

    public static Dataset<Row> check(String table, Dataset<Row> frame, List<Column> rules) {
        List<Column> all = new ArrayList<>();
        all.add(rule(requiredMissing(table), QualityRule.CAMPO_REQUERIDO));
        all.addAll(rules);
        Dataset<Row> withReasons = frame.withColumn(REASONS, concat_ws("|", all.toArray(Column[]::new)));
        return markDuplicates(table, withReasons).cache();
    }

    public static Dataset<Row> valid(Dataset<Row> checked) {
        return checked.filter(col(REASONS).equalTo(""));
    }

    public static Dataset<Row> rejected(String table, Dataset<Row> checked) {
        Column[] original = columns(AcademicSchema.columns(table));
        Column[] key = AcademicSchema.KEYS.get(table).stream().map(column -> coalesce(col(column), lit("?"))).toArray(Column[]::new);
        return checked.filter(col(REASONS).notEqual(""))
                .select(lit(table).alias("tabla"), col(CsvExtractor.ROW), concat_ws(" · ", key).alias("clave"),
                        col(REASONS), to_json(struct(original), Map.of("ignoreNullFields", "false")).alias("registro"));
    }

    static Column[] columns(List<String> names) {
        return names.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
    }

    private static Dataset<Row> markDuplicates(String table, Dataset<Row> frame) {
        List<String> key = AcademicSchema.KEYS.get(table);
        Column keyPresent = lit(true);
        for (String column : key) {
            keyPresent = keyPresent.and(col(column).isNotNull());
        }
        var firstValid = Window.partitionBy(columns(key))
                .orderBy(when(col(REASONS).equalTo(""), 0).otherwise(1), col(CsvExtractor.ROW));
        return frame
                .withColumn("_rank", row_number().over(firstValid))
                .withColumn(REASONS, concat_ws("|", when(col(REASONS).notEqual(""), col(REASONS)),
                        rule(keyPresent.and(col("_rank").gt(1)), QualityRule.REGISTRO_DUPLICADO)))
                .drop("_rank");
    }

    private static Column requiredMissing(String table) {
        Column missing = lit(false);
        for (String column : AcademicSchema.columns(table)) {
            if (!AcademicSchema.optional(table, column)) {
                missing = missing.or(col(column).isNull());
            }
        }
        return missing;
    }
}
