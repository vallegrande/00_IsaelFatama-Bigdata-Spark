package pe.edu.vallegrande.bigdata.job.carga;

import java.io.Serializable;
import java.util.List;

public record TargetTable(String name, String sourceFile, List<String> columns, String mergeSql) implements Serializable {
}
