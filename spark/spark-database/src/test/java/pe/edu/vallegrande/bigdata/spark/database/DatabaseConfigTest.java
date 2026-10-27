package pe.edu.vallegrande.bigdata.spark.database;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseConfigTest {

    @Test
    void readsTheEnvironmentVariables() {
        DatabaseConfig config = DatabaseConfig.from(Map.of(
                "DB_URL", "jdbc:oracle:thin:@//host:1521/svc",
                "DB_USER", "etl",
                "DB_PASSWORD", "secreto",
                "DB_SCHEMA", "CORE")::get);

        assertTrue(config.isConfigured());
        assertEquals("jdbc:oracle:thin:@//host:1521/svc", config.url());
        assertEquals("etl", config.user());
        assertEquals("CORE", config.schema());
    }

    @Test
    void pointsToTheLocalOracleByDefaultSoOnlyThePasswordIsRequired() {
        DatabaseConfig config = DatabaseConfig.from(Map.of("DB_PASSWORD", "secreto")::get);

        assertTrue(config.isConfigured());
        assertEquals(DatabaseConfig.DEFAULT_URL, config.url());
        assertEquals(DatabaseConfig.DEFAULT_USER, config.user());
        assertTrue(config.url().contains("localhost:1521"));
    }

    @Test
    void blankValuesFallBackToTheDefaults() {
        DatabaseConfig config = DatabaseConfig.from(Map.of("DB_URL", " ", "DB_USER", "", "DB_PASSWORD", "x")::get);

        assertEquals(DatabaseConfig.DEFAULT_URL, config.url());
        assertEquals(DatabaseConfig.DEFAULT_USER, config.user());
    }

    @Test
    void isNotConfiguredWithoutAPassword() {
        assertFalse(DatabaseConfig.from(key -> null).isConfigured());
        assertFalse(DatabaseConfig.from(Map.of("DB_URL", "jdbc:x", "DB_USER", "etl")::get).isConfigured());
        assertFalse(DatabaseConfig.from(Map.of("DB_PASSWORD", "  ")::get).isConfigured());
    }

    @Test
    void neverPrintsThePassword() {
        DatabaseConfig config = new DatabaseConfig("jdbc:x", "etl", "secreto", "CORE");

        assertFalse(config.toString().contains("secreto"));
        assertTrue(config.toString().contains("****"));
    }

}
