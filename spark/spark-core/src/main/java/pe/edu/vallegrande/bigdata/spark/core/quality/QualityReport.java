package pe.edu.vallegrande.bigdata.spark.core.quality;

import java.util.List;
import java.util.Map;

public record QualityReport(String rulesVersion, long received, long accepted, long rejected, double rejectedRatio,
                            double maxRejectedRatio, boolean published, String decision, List<TableQuality> tables,
                            Map<String, Long> dimensions, List<ReasonQuality> reasons, List<Check> checks) {

    public record TableQuality(String table, long received, long accepted, long rejected, double rejectedRatio,
                               boolean withinTolerance) {
    }

    public record ReasonQuality(String table, String rule, String dimension, String description, long rows) {
    }

    public record Check(String name, boolean passed, String detail) {
    }
}
