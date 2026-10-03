package pe.edu.vallegrande.bigdata.spark.core.publication;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class CsvPublisher implements TableFormat {
    private static final String BOM = "\uFEFF";

    static String format(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof Double number) {
            return number.isNaN() || number.isInfinite() ? "" : BigDecimal.valueOf(number).stripTrailingZeros().toPlainString();
        }
        if (value instanceof String text && !text.isEmpty() && "=+-@\t\r".indexOf(text.charAt(0)) >= 0) {
            return "'" + text;
        }
        return value.toString();
    }

    @Override
    public String extension() {
        return "csv";
    }

    @Override
    public void write(Path target, StructType schema, List<Row> rows) throws IOException {
        Path temporary = AtomicFiles.temporaryFor(target);
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(schema.fieldNames()).setRecordSeparator("\n").get();
        try (var writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
            writer.write(BOM);
            try (var printer = new CSVPrinter(writer, format)) {
                for (Row row : rows) {
                    List<String> values = new ArrayList<>(row.size());
                    for (int index = 0; index < row.size(); index++) {
                        values.add(format(row.isNullAt(index) ? null : row.get(index)));
                    }
                    printer.printRecord(values);
                }
            }
        }
        AtomicFiles.publish(temporary, target);
    }
}
