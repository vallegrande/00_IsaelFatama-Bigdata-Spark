package pe.edu.vallegrande.bigdata.spark.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

public final class DatabaseNaming {

    public static final int MAX_IDENTIFIER_LENGTH = 128;

    private static final Pattern VALID = Pattern.compile("[A-Z][A-Z0-9_$#]*");

    private DatabaseNaming() {
    }

    public static String physicalTable(TableRef table) {
        return identifier(table.layer().prefix() + table.name());
    }

    public static String physicalColumn(String column) {
        return identifier(column);
    }

    public static Dataset<Row> toDatabase(Dataset<Row> data) {
        return data.toDF(Arrays.stream(data.columns()).map(DatabaseNaming::physicalColumn).toArray(String[]::new));
    }

    public static Dataset<Row> toLake(Dataset<Row> data) {
        return data.toDF(Arrays.stream(data.columns()).map(column -> column.toLowerCase(Locale.ROOT)).toArray(String[]::new));
    }

    public static String identifier(String raw) {
        String name = raw.toUpperCase(Locale.ROOT);
        if (!VALID.matcher(name).matches()) {
            throw new IllegalArgumentException("Identificador inválido para la base de datos: " + raw);
        }
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException("El identificador supera " + MAX_IDENTIFIER_LENGTH + " caracteres: " + raw);
        }
        return name;
    }

}
