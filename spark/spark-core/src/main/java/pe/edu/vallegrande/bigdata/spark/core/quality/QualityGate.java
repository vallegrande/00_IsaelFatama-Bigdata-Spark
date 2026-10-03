package pe.edu.vallegrande.bigdata.spark.core.quality;

import pe.edu.vallegrande.bigdata.contracts.QualityRule;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult.ReasonCount;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult.TableCount;

import java.util.*;

public final class QualityGate {
    private static final List<String> DIMENSIONS = List.of("COMPLETITUD", "VALIDEZ", "UNICIDAD", "INTEGRIDAD", "CONSISTENCIA");

    private final String rulesVersion;
    private final List<QualityCheck> extraChecks;

    public QualityGate(String rulesVersion, List<QualityCheck> extraChecks) {
        this.rulesVersion = rulesVersion;
        this.extraChecks = List.copyOf(extraChecks);
    }

    private static double round(double value) {
        return Math.round(value * 1_000_000d) / 1_000_000d;
    }

    public QualityReport evaluate(ValidationResult validation, double maxRejectedRatio) {
        List<QualityReport.TableQuality> tables = new ArrayList<>();
        List<QualityReport.Check> checks = new ArrayList<>();
        for (Map.Entry<String, TableCount> entry : validation.counts().entrySet()) {
            TableCount count = entry.getValue();
            double ratio = count.received() == 0 ? 1 : (double) count.rejected() / count.received();
            boolean within = ratio <= maxRejectedRatio;
            tables.add(new QualityReport.TableQuality(entry.getKey(), count.received(), count.accepted(), count.rejected(),
                    round(ratio), within));
            checks.add(new QualityReport.Check("Rechazo en " + entry.getKey(), within, String.format(Locale.ROOT,
                    "%d de %d filas rechazadas (%.2f %%); máximo %.2f %%", count.rejected(), count.received(), ratio * 100, maxRejectedRatio * 100)));
        }
        extraChecks.forEach(check -> checks.addAll(check.evaluate(validation)));
        checks.add(new QualityReport.Check("Conservación de filas", validation.received() == validation.accepted() + validation.rejectedCount(),
                "recibidas = aceptadas + rechazadas (" + validation.received() + " = " + validation.accepted() + " + " + validation.rejectedCount() + ")"));

        Map<String, Long> dimensions = new LinkedHashMap<>();
        DIMENSIONS.forEach(dimension -> dimensions.put(dimension, 0L));
        List<QualityReport.ReasonQuality> reasons = new ArrayList<>();
        for (ReasonCount reason : validation.reasons()) {
            QualityRule rule = QualityRule.valueOf(reason.reason());
            reasons.add(new QualityReport.ReasonQuality(reason.table(), rule.name(), rule.dimension(), rule.description(), reason.rows()));
            dimensions.merge(rule.dimension(), reason.rows(), Long::sum);
        }

        boolean published = checks.stream().allMatch(QualityReport.Check::passed);
        long received = validation.received();
        double ratio = received == 0 ? 1 : (double) validation.rejectedCount() / received;
        String decision = published
                ? "Publicado: las filas válidas pasan a Gold y las observadas quedan en cuarentena"
                : "Bloqueado: Gold no se publica porque el lote no cumple los controles de calidad";
        return new QualityReport(rulesVersion, received, validation.accepted(), validation.rejectedCount(),
                round(ratio), maxRejectedRatio, published, decision, tables, dimensions, reasons, checks);
    }
}
