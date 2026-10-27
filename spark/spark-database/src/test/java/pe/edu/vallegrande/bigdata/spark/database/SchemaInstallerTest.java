package pe.edu.vallegrande.bigdata.spark.database;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaInstallerTest {

    @Test
    void splitsAScriptIntoStatementsIgnoringComments() {
        List<String> statements = SchemaInstaller.statements("-- comentario\nCREATE TABLE A (X NUMBER);\n\n-- otro\nCREATE TABLE B (Y NUMBER);\n");

        assertEquals(List.of("CREATE TABLE A (X NUMBER)", "CREATE TABLE B (Y NUMBER)"), statements);
    }

    @Test
    void createsEveryLayerAndCanRunTwice() throws Exception {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        try (Connection connection = DriverManager.getConnection(url, "sa", "x")) {
            int first = SchemaInstaller.install(connection);
            int second = SchemaInstaller.install(connection);

            assertTrue(first > 20, "se ejecutaron las sentencias de los 4 scripts: " + first);
            assertEquals(0, second, "ejecutarlo otra vez no cambia nada");
            for (String table : List.of("STG_NOTAS", "CORE_NOTAS", "CORE_NOTAS_LOAD", "SERVING_RENDIMIENTO",
                    "SERVING_CURSOS_RESUMEN_LOAD", "ETL_RECHAZADOS", "ETL_RECHAZADOS_LOAD")) {
                try (Statement statement = connection.createStatement();
                     ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    assertTrue(result.next(), table);
                }
            }
        }
    }

}
