package pe.edu.vallegrande.bigdata.spark.academico.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.spark.academico.extraction.CsvExtractor;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public final class AcademicValidator {
    private final List<TableValidator> validators;

    public AcademicValidator() {
        this(List.of(new SemestresValidator(), new CursosValidator(), new EstudiantesValidator(),
                new NotasValidator(), new AsistenciaValidator()));
    }

    public AcademicValidator(List<TableValidator> validators) {
        this.validators = List.copyOf(validators);
    }

    public ValidationResult validate(BronzeTables bronze) {
        Map<String, Dataset<Row>> checked = new LinkedHashMap<>();
        Map<String, Dataset<Row>> silver = new LinkedHashMap<>();
        ReferenceData references = ReferenceData.empty();
        Dataset<Row> rejected = null;
        for (TableValidator validator : validators) {
            Dataset<Row> table = validator.check(bronze.table(validator.table()), references);
            references = validator.publish(table, references);
            checked.put(validator.table(), table);
            silver.put(validator.table(), validator.silver(table));
            Dataset<Row> invalid = ValidationRules.rejected(validator.table(), table);
            rejected = rejected == null ? invalid : rejected.unionByName(invalid);
        }
        return ValidationResult.of(checked, silver, rejected.orderBy(col("tabla"), col(CsvExtractor.ROW)));
    }
}
