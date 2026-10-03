package pe.edu.vallegrande.bigdata.spark.core.step;

import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;

public interface GoldAnalytics {

    String description();

    GoldTables analyze(ValidationResult silver);

    String detail(long rows);
}
