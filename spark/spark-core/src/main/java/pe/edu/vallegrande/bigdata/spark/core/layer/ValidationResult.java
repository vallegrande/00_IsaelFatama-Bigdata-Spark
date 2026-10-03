package pe.edu.vallegrande.bigdata.spark.core.layer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public final class ValidationResult {
    public static final String REASONS = "motivos";
    private final Map<String, Dataset<Row>> checked;
    private final Map<String, Dataset<Row>> silver;
    private final Dataset<Row> rejected;
    private final Map<String, TableCount> counts;
    private final List<ReasonCount> reasons;
    private ValidationResult(Map<String, Dataset<Row>> checked, Map<String, Dataset<Row>> silver, Dataset<Row> rejected,
                             Map<String, TableCount> counts, List<ReasonCount> reasons) {
        this.checked = checked;
        this.silver = silver;
        this.rejected = rejected;
        this.counts = counts;
        this.reasons = reasons;
    }

    public static ValidationResult of(Map<String, Dataset<Row>> checked, Map<String, Dataset<Row>> silver, Dataset<Row> rejected) {
        Map<String, TableCount> counts = new LinkedHashMap<>();
        List<ReasonCount> reasons = new ArrayList<>();
        for (Map.Entry<String, Dataset<Row>> entry : checked.entrySet()) {
            Row totals = entry.getValue().agg(count("*"),
                    sum(when(col(REASONS).equalTo(""), 1).otherwise(0))).first();
            long received = totals.getLong(0);
            long accepted = totals.isNullAt(1) ? 0 : totals.getLong(1);
            counts.put(entry.getKey(), new TableCount(received, accepted, received - accepted));
            for (Row row : entry.getValue().filter(col(REASONS).notEqual(""))
                    .select(explode(split(col(REASONS), "\\|")).alias("motivo"))
                    .groupBy("motivo").count().orderBy(col("count").desc()).collectAsList()) {
                reasons.add(new ReasonCount(entry.getKey(), row.getString(0), row.getLong(1)));
            }
        }
        return new ValidationResult(checked, silver, rejected, counts, List.copyOf(reasons));
    }

    public Dataset<Row> silver(String table) {
        return silver.get(table);
    }

    public Map<String, Dataset<Row>> silverTables() {
        return silver;
    }

    public Dataset<Row> rejected() {
        return rejected;
    }

    public Map<String, TableCount> counts() {
        return counts;
    }

    public List<ReasonCount> reasons() {
        return reasons;
    }

    public long received() {
        return counts.values().stream().mapToLong(TableCount::received).sum();
    }

    public long accepted() {
        return counts.values().stream().mapToLong(TableCount::accepted).sum();
    }

    public long rejectedCount() {
        return counts.values().stream().mapToLong(TableCount::rejected).sum();
    }

    public void release() {
        checked.values().forEach(Dataset::unpersist);
    }

    public record TableCount(long received, long accepted, long rejected) {
    }

    public record ReasonCount(String table, String reason, long rows) {
    }
}
