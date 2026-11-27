package pe.edu.vallegrande.bigdata.job.carga;

import pe.edu.vallegrande.bigdata.spark.core.database.OracleConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class LoadAudit {

    private final OracleConnection oracle;

    public final String executionId;

    public LoadAudit(OracleConnection oracle, String executionId) {
        this.oracle = oracle;
        this.executionId = executionId;
    }

    public LoadResult register(String table) throws SQLException {
        try (Connection connection = oracle.open()) {
            long inserted = count(connection, Constants.COUNT_INSERTED.formatted(table));
            long updated = count(connection, Constants.COUNT_UPDATED.formatted(table));
            try (PreparedStatement insert = connection.prepareStatement(Constants.INSERT_LOAD)) {
                insert.setString(1, executionId);
                insert.setString(2, table);
                insert.setLong(3, inserted);
                insert.setLong(4, updated);
                insert.executeUpdate();
            }
            return new LoadResult(table, inserted, updated);
        }
    }

    private long count(Connection connection, String sql) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setString(1, executionId);
            try (ResultSet result = query.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }
}
