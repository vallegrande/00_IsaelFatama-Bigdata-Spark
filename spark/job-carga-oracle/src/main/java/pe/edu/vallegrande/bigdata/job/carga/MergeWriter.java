package pe.edu.vallegrande.bigdata.job.carga;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.spark.core.database.OracleConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;

public final class MergeWriter implements ForeachPartitionFunction<Row> {

    private final OracleConnection oracle;

    private final String mergeSql;

    public MergeWriter(OracleConnection oracle, String mergeSql) {
        this.oracle = oracle;
        this.mergeSql = mergeSql;
    }

    @Override
    public void call(Iterator<Row> rows) throws Exception {
        try (Connection connection = oracle.open();
             PreparedStatement merge = connection.prepareStatement(mergeSql)) {
            connection.setAutoCommit(false);

            int pending = 0;
            while (rows.hasNext()) {
                Row row = rows.next();
                for (int index = 0; index < row.size(); index++) {
                    merge.setObject(index + 1, row.get(index));
                }

                merge.addBatch();

                if (++pending == Constants.ROWS_PER_BATCH) {
                    merge.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                merge.executeBatch();
            }
            connection.commit();
        }
    }
}
