package pe.edu.vallegrande.bigdata.spark.database.seed;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseNaming;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseTables;
import pe.edu.vallegrande.bigdata.spark.database.TableRef;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class StagingSeeder {

    private static final char BOM = '﻿';

    private static final int BATCH = 1000;

    private StagingSeeder() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Uso: StagingSeeder <carpeta-con-los-5-csv>");
            System.exit(1);
        }
        DatabaseConfig config = DatabaseConfig.fromEnvironment();
        if (!config.isConfigured()) {
            System.err.println("Defina DB_PASSWORD (y DB_URL / DB_USER si no usa el Oracle local por defecto)");
            System.exit(1);
        }
        seed(config, Path.of(args[0])).forEach((table, rows) -> System.out.println(table + ": " + rows + " filas"));
    }

    public static Map<String, Long> seed(DatabaseConfig config, Path directory) throws IOException, SQLException {
        Map<String, Long> loaded = new LinkedHashMap<>();
        String schema = config.schema() == null || config.schema().isBlank() ? null : DatabaseNaming.identifier(config.schema());
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            for (String table : AcademicSchema.TABLES) {
                String name = DatabaseNaming.physicalTable(TableRef.of(DatabaseLayer.STAGING, table));
                String qualified = schema == null ? name : schema + "." + name;
                loaded.put(table, load(connection, qualified, table, directory.resolve(table + ".csv")));
            }
        }
        return loaded;
    }

    private static long load(Connection connection, String target, String table, Path csv) throws IOException, SQLException {
        List<String> columns = AcademicSchema.columns(table).stream().map(DatabaseNaming::physicalColumn).toList();
        requireTable(connection, target);
        String insert = "INSERT INTO " + target + " (" + String.join(", ", columns) + ") VALUES ("
                + "?, ".repeat(columns.size() - 1) + "?)";
        long rows = 0;
        connection.setAutoCommit(false);
        try (BufferedReader reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8);
             Statement delete = connection.createStatement();
             PreparedStatement statement = connection.prepareStatement(insert)) {
            delete.executeUpdate("DELETE FROM " + target);
            skipBom(reader);
            try (CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)) {
                requireHeader(table, parser.getHeaderNames(), csv);
                for (CSVRecord record : parser) {
                    for (int index = 0; index < columns.size(); index++) {
                        String value = index < record.size() ? record.get(index).trim() : "";
                        statement.setString(index + 1, value.isEmpty() ? null : value);
                    }
                    statement.addBatch();
                    if (++rows % BATCH == 0) {
                        statement.executeBatch();
                    }
                }
            }
            statement.executeBatch();
            connection.commit();
        } catch (IOException | SQLException | RuntimeException error) {
            connection.rollback();
            throw error;
        } finally {
            connection.setAutoCommit(true);
        }
        return rows;
    }

    private static void requireTable(Connection connection, String target) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery("SELECT 1 FROM " + target + " WHERE 1 = 0").close();
        } catch (SQLException error) {
            throw new IllegalStateException("La tabla " + target + " no existe. Cree las tablas con SchemaInstaller "
                    + "o con los scripts de " + DatabaseTables.DDL_DIRECTORY, error);
        }
    }

    private static void requireHeader(String table, List<String> header, Path csv) {
        if (!header.stream().map(String::trim).toList().equals(AcademicSchema.columns(table))) {
            throw new IllegalArgumentException("El encabezado de " + csv.getFileName() + " debe ser: "
                    + String.join(",", AcademicSchema.columns(table)));
        }
    }

    private static void skipBom(BufferedReader reader) throws IOException {
        reader.mark(1);
        if (reader.read() != BOM) {
            reader.reset();
        }
    }

}
