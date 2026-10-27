package pe.edu.vallegrande.bigdata.spark.database;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class SchemaInstaller {

    private static final String STATEMENT_END = ";";

    private static final String COMMENT = "--";

    private static final int ORACLE_NAME_ALREADY_USED = 955;

    private SchemaInstaller() {
    }

    public static void main(String[] args) throws Exception {
        DatabaseConfig config = DatabaseConfig.fromEnvironment();
        if (!config.isConfigured()) {
            System.err.println("Defina DB_PASSWORD (y DB_URL / DB_USER si no usa el Oracle local por defecto)");
            System.exit(1);
        }
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            System.out.println("Sentencias ejecutadas: " + install(connection));
        }
    }

    public static int install(Connection connection) throws IOException, SQLException {
        int executed = 0;
        for (String script : DatabaseTables.DDL_SCRIPTS) {
            for (String sql : statements(read(DatabaseTables.DDL_DIRECTORY + script))) {
                if (run(connection, sql)) {
                    executed++;
                }
            }
        }
        return executed;
    }

    static List<String> statements(String script) {
        String withoutComments = Arrays.stream(script.split("\\R"))
                .filter(line -> !line.trim().startsWith(COMMENT))
                .reduce("", (all, line) -> all + line + "\n");
        List<String> statements = new ArrayList<>();
        for (String part : withoutComments.split(STATEMENT_END)) {
            if (!part.isBlank()) {
                statements.add(part.trim());
            }
        }
        return statements;
    }

    private static boolean run(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            return true;
        } catch (SQLException error) {
            if (error.getErrorCode() == ORACLE_NAME_ALREADY_USED || alreadyExists(error)) {
                return false;
            }
            throw error;
        }
    }

    private static boolean alreadyExists(SQLException error) {
        String message = String.valueOf(error.getMessage()).toLowerCase();
        return message.contains("already exists");
    }

    private static String read(String resource) throws IOException {
        try (InputStream stream = SchemaInstaller.class.getClassLoader().getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IOException("No se encontró el script " + resource);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
