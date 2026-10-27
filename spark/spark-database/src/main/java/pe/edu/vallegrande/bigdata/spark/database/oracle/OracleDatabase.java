package pe.edu.vallegrande.bigdata.spark.database.oracle;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.spark.database.Database;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseTables;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;
import pe.edu.vallegrande.bigdata.spark.database.WriteMode;

import static org.apache.spark.sql.functions.lit;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleDatabase implements Database {

    private static final String OPTION_URL = "url";

    private static final String OPTION_USER = "user";

    private static final String OPTION_PASSWORD = "password";

    private static final String OPTION_DRIVER = "driver";

    private static final String OPTION_TABLE = "dbtable";

    private static final String OPTION_FETCH_SIZE = "fetchsize";

    private static final String OPTION_BATCH_SIZE = "batchsize";

    private static final String OPTION_PARTITIONS = "numPartitions";

    private static final String OPTION_DATE_AS_TIMESTAMP = "oracle.jdbc.mapDateToTimestamp";

    private static final String ORACLE_URL_PREFIX = "jdbc:oracle:";

    private static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";

    private static final String ROWS_PER_FETCH = "10000";

    private static final String ROWS_PER_BATCH = "10000";

    private static final String MAX_CONNECTIONS = "4";

    private final DatabaseConfig config;

    private final String schema;

    public OracleDatabase(DatabaseConfig config) {
        this.config = config;
        this.schema = config.schema() == null || config.schema().isBlank() ? null : DatabaseNaming.identifier(config.schema());
    }

    public static OracleDatabase fromEnvironment() {
        return new OracleDatabase(DatabaseConfig.requiredFromEnvironment());
    }

    @Override
    public Dataset<Row> read(SparkSession spark, TableRef table) {
        Dataset<Row> data = spark.read()
                .format("jdbc")
                .options(sparkOptions())
                .option(OPTION_TABLE, qualified(DatabaseNaming.physicalTable(table)))
                .option(OPTION_FETCH_SIZE, ROWS_PER_FETCH)
                .load();
        return DatabaseNaming.toLake(data);
    }

    @Override
    public long write(Dataset<Row> data, TableRef table, WriteMode mode, String batchId) {
        Dataset<Row> rows = DatabaseNaming.toDatabase(withBatch(data, batchId));
        List<String> columns = List.of(rows.columns());
        List<String> keys = table.keys().stream().map(DatabaseNaming::physicalColumn).toList();
        require(mode, columns, keys, batchId);

        String target = qualified(DatabaseNaming.physicalTable(table));
        String load = SqlStatements.loadTable(target);
        long count = rows.count();

        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            requireTable(connection, target);
            requireTable(connection, load);
            try (PreparedStatement clear = connection.prepareStatement(SqlStatements.deleteBatch(load))) {
                clear.setString(1, batchId);
                clear.executeUpdate();
            }
            copyToLoad(rows, load);
            apply(connection, mode, target, load, columns, keys, batchId);
        } catch (SQLException error) {
            throw new IllegalStateException("No se pudo escribir " + table + " en la base de datos: " + error.getMessage(), error);
        }
        return count;
    }

    private Dataset<Row> withBatch(Dataset<Row> data, String batchId) {
        boolean hasBatch = Arrays.stream(data.columns()).anyMatch(DatabaseLayer.BATCH_COLUMN::equalsIgnoreCase);
        return hasBatch ? data : data.withColumn(DatabaseLayer.BATCH_COLUMN, lit(batchId));
    }

    private Map<String, String> sparkOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(OPTION_URL, config.url());
        options.put(OPTION_USER, config.user());
        options.put(OPTION_PASSWORD, config.password());
        if (config.url().startsWith(ORACLE_URL_PREFIX)) {
            options.put(OPTION_DRIVER, ORACLE_DRIVER);
            options.put(OPTION_DATE_AS_TIMESTAMP, "false");
        }
        return options;
    }

    private String qualified(String name) {
        return schema == null ? name : schema + "." + name;
    }

    private void require(WriteMode mode, List<String> columns, List<String> keys, String batchId) {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("Se necesita el identificador del lote");
        }
        if (mode == WriteMode.MERGE && (keys.isEmpty() || !columns.containsAll(keys))) {
            throw new IllegalArgumentException("MERGE necesita que las columnas de la clave existan en los datos: " + keys);
        }
    }

    private void requireTable(Connection connection, String qualifiedName) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        int dot = qualifiedName.lastIndexOf('.');
        String tableSchema = dot < 0 ? connection.getSchema() : qualifiedName.substring(0, dot);
        String name = qualifiedName.substring(dot + 1);
        String escape = metadata.getSearchStringEscape();
        String pattern = escape == null ? name : name.replace("_", escape + "_");
        try (ResultSet tables = metadata.getTables(null, tableSchema, pattern, new String[]{"TABLE"})) {
            if (!tables.next()) {
                throw new IllegalStateException("La tabla " + qualifiedName + " no existe. Cree las tablas con SchemaInstaller "
                        + "o con los scripts de " + DatabaseTables.DDL_DIRECTORY);
            }
        }
    }

    private void copyToLoad(Dataset<Row> rows, String load) {
        rows.write()
                .format("jdbc")
                .options(sparkOptions())
                .option(OPTION_TABLE, load)
                .option(OPTION_BATCH_SIZE, ROWS_PER_BATCH)
                .option(OPTION_PARTITIONS, MAX_CONNECTIONS)
                .mode(SaveMode.Append)
                .save();
    }

    private void apply(Connection connection, WriteMode mode, String target, String load, List<String> columns,
                       List<String> keys, String batchId) throws SQLException {
        connection.setAutoCommit(false);
        try {
            switch (mode) {
                case MERGE -> run(connection, SqlStatements.mergeBatch(target, load, columns, keys), batchId);
                case REPLACE_BATCH -> {
                    run(connection, SqlStatements.deleteBatch(target), batchId);
                    run(connection, SqlStatements.insertBatch(target, load, columns), batchId);
                }
            }
            connection.commit();
        } catch (SQLException error) {
            connection.rollback();
            throw error;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private void run(Connection connection, String sql, String batchId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, batchId);
            statement.executeUpdate();
        }
    }

}
