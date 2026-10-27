package pe.edu.vallegrande.bigdata.spark.database.oracle;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.bigdata.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.SchemaInstaller;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;
import pe.edu.vallegrande.bigdata.spark.database.WriteMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleDatabaseTest {

    private static final StructType TERMS = new StructType()
            .add("semestre_id", DataTypes.StringType, false)
            .add("numero", DataTypes.IntegerType)
            .add("nombre", DataTypes.StringType)
            .add("periodo", DataTypes.StringType)
            .add("fecha_inicio", DataTypes.DateType)
            .add("fecha_fin", DataTypes.DateType);

    private static final List<String> KEYS = List.of("semestre_id");

    private static SparkSession spark;

    private static DatabaseConfig config;

    private static OracleDatabase database;

    @BeforeAll
    static void start() throws Exception {
        spark = SparkSessionFactory.create(WorkerConfig.load().withoutUi());
        config = new DatabaseConfig("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x", null);
        database = new OracleDatabase(config);
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            SchemaInstaller.install(connection);
        }
    }

    @BeforeEach
    void emptyTheTables() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            for (String table : List.of("CORE_SEMESTRES", "CORE_SEMESTRES_LOAD", "ETL_RECHAZADOS", "ETL_RECHAZADOS_LOAD")) {
                statement.execute("DELETE FROM " + table);
            }
        }
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void mergeStoresTheRowsAndKeepsTheBatchInTheLoadTable() throws Exception {
        TableRef table = TableRef.of(DatabaseLayer.CORE, "semestres", KEYS);

        long written = database.write(terms(term("I", 1), term("II", 2), term("III", 3)), table, WriteMode.MERGE, "lote-1");

        assertEquals(3, written);
        Dataset<Row> read = database.read(spark, table);
        assertEquals(List.of("semestre_id", "numero", "nombre", "periodo", "fecha_inicio", "fecha_fin", "batch_id"),
                List.of(read.columns()), "las columnas vuelven en minúsculas");
        assertEquals(3, read.count());
        assertEquals(3, count("CORE_SEMESTRES_LOAD"), "la tabla de carga conserva el lote para consultarlo");
        assertEquals("lote-1", read.first().getString(read.first().fieldIndex("batch_id")));
    }

    @Test
    void mergeIsIdempotentAndUpdatesExistingRowsByKey() throws Exception {
        TableRef table = TableRef.of(DatabaseLayer.CORE, "semestres", KEYS);
        database.write(terms(term("I", 1), term("II", 2)), table, WriteMode.MERGE, "lote-1");

        database.write(terms(term("I", 10), term("III", 3)), table, WriteMode.MERGE, "lote-2");
        database.write(terms(term("I", 10), term("III", 3)), table, WriteMode.MERGE, "lote-2");

        Dataset<Row> read = database.read(spark, table);
        assertEquals(3, read.count(), "repetir la carga no duplica filas");
        assertEquals(10, numberOf(read, "I"), "la fila existente se actualizó");
        assertEquals(2, numberOf(read, "II"), "la que no vino en el segundo lote no cambia");
        assertEquals(3, numberOf(read, "III"), "la nueva se insertó");
        assertEquals(4, count("CORE_SEMESTRES_LOAD"), "la tabla de carga conserva cada lote");
    }

    @Test
    void valuesKeepTheirTypesInARoundTrip() throws Exception {
        TableRef table = TableRef.of(DatabaseLayer.CORE, "semestres", KEYS);
        database.write(terms(term("I", 1)), table, WriteMode.MERGE, "lote-1");

        Row stored = database.read(spark, table).first();

        assertEquals(1, ((Number) stored.get(stored.fieldIndex("numero"))).intValue());
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT FECHA_INICIO FROM CORE_SEMESTRES")) {
            assertTrue(result.next());
            assertEquals(LocalDate.of(2026, 8, 17), result.getDate(1).toLocalDate());
        }
    }

    @Test
    void replaceBatchReplacesOnlyThatBatchAndNeverDuplicates() throws Exception {
        TableRef table = TableRef.of(DatabaseLayer.CONTROL, "rechazados");
        database.write(rejected("A", "B", "C"), table, WriteMode.REPLACE_BATCH, "lote-1");
        database.write(rejected("X", "Y"), table, WriteMode.REPLACE_BATCH, "lote-2");

        database.write(rejected("A2"), table, WriteMode.REPLACE_BATCH, "lote-1");
        database.write(rejected("A2"), table, WriteMode.REPLACE_BATCH, "lote-1");

        Dataset<Row> read = database.read(spark, table);
        assertEquals(1, read.filter("batch_id = 'lote-1'").count(), "el lote 1 quedó reemplazado, sin duplicados");
        assertEquals(2, read.filter("batch_id = 'lote-2'").count(), "el lote 2 no se tocó");
    }

    @Test
    void noTemporaryTablesAreEverCreated() throws Exception {
        database.write(terms(term("I", 1)), TableRef.of(DatabaseLayer.CORE, "semestres", KEYS), WriteMode.MERGE, "lote-1");

        assertEquals(List.of(), tablesLike("TMP\\_%"));
    }

    @Test
    void aTableThatWasNotCreatedByTheScriptsIsReportedClearly() {
        TableRef table = TableRef.of(DatabaseLayer.CORE, "no_existe", KEYS);

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                database.write(terms(term("I", 1)), table, WriteMode.MERGE, "lote-1"));

        assertTrue(error.getMessage().contains("CORE_NO_EXISTE"), error.getMessage());
        assertTrue(error.getMessage().contains("SchemaInstaller"), error.getMessage());
    }

    @Test
    void aFailedMergeLeavesTheTargetUntouched() throws Exception {
        TableRef table = TableRef.of(DatabaseLayer.CORE, "semestres", KEYS);
        database.write(terms(term("I", 1)), table, WriteMode.MERGE, "lote-1");

        assertThrows(IllegalStateException.class, () ->
                database.write(terms(term("I", 99), term("Z", 5), term("Z", 6)), table, WriteMode.MERGE, "lote-2"));

        assertEquals(1, count("CORE_SEMESTRES"), "la tabla destino quedó como estaba");
        assertEquals(1, numberOf(database.read(spark, table), "I"), "el cambio de la fila existente se deshizo");
    }

    @Test
    void mergeNeedsKeys() {
        assertThrows(IllegalArgumentException.class, () ->
                database.write(terms(term("I", 1)), TableRef.of(DatabaseLayer.CORE, "semestres"), WriteMode.MERGE, "lote-1"));
        assertThrows(IllegalArgumentException.class, () ->
                database.write(terms(term("I", 1)), TableRef.of(DatabaseLayer.CORE, "semestres", List.of("no_existe")), WriteMode.MERGE, "lote-1"));
    }

    @Test
    void everyWriteNeedsTheBatchId() {
        assertThrows(IllegalArgumentException.class, () ->
                database.write(rejected("A"), TableRef.of(DatabaseLayer.CONTROL, "rechazados"), WriteMode.REPLACE_BATCH, " "));
    }

    @Test
    void anInvalidTableNameIsRejectedBeforeTouchingTheDatabase() {
        assertThrows(IllegalArgumentException.class, () ->
                database.write(terms(term("I", 1)), TableRef.of(DatabaseLayer.CORE, "notas; DROP TABLE X", KEYS), WriteMode.MERGE, "lote-1"));
    }

    private Dataset<Row> terms(Row... rows) {
        return spark.createDataFrame(List.of(rows), TERMS);
    }

    private Row term(String id, int number) {
        return RowFactory.create(id, number, "Semestre " + id, "2026-II", LocalDate.of(2026, 8, 17), LocalDate.of(2026, 12, 18));
    }

    private Dataset<Row> rejected(String... keys) {
        StructType schema = new StructType().add("tabla", DataTypes.StringType).add("fila", DataTypes.LongType)
                .add("clave", DataTypes.StringType).add("motivos", DataTypes.StringType).add("registro", DataTypes.StringType);
        List<Row> rows = new ArrayList<>();
        for (String key : keys) {
            rows.add(RowFactory.create("notas", 1L, key, "nota fuera de rango", "{}"));
        }
        return spark.createDataFrame(rows, schema);
    }

    private int numberOf(Dataset<Row> data, String id) {
        return ((Number) data.filter("semestre_id = '" + id + "'").first().get(1)).intValue();
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(config.url(), config.user(), config.password());
    }

    private long count(String table) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            result.next();
            return result.getLong(1);
        }
    }

    private List<String> tablesLike(String pattern) throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '" + pattern + "' ESCAPE '\\'")) {
            while (result.next()) {
                names.add(result.getString(1));
            }
        }
        return names;
    }

}
