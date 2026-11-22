package pe.edu.vallegrande.bigdata.job.lectura;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.Locale;

import static org.apache.spark.sql.functions.col;

public final class Transformer {

    private Transformer(){}

    public static Dataset<Row> toFileFormat(Dataset<Row> table){
        Dataset<Row> business = table.drop(Constants.AUDIT_COLUMNS);
        Column[] columns =
                Arrays.stream(business.schema().fields()).map(Transformer::fileColumn).toArray(Column[]::new);
        return business.select(columns);
    }

    private static Column fileColumn(StructField field){
        Column column = col(field.name());
        if(field.dataType() instanceof DecimalType number){
            column = column.cast(number.scale() == 0 ? Constants.INTEGER_TYPE : Constants.DECIMAL_TYPE);
        }

        return column.alias(field.name().toLowerCase(Locale.ROOT));
    }

}
