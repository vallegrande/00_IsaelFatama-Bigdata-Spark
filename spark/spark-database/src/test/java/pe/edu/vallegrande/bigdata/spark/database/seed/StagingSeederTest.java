package pe.edu.vallegrande.bigdata.spark.database.seed;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;
import pe.edu.vallegrande.bigdata.spark.database.DatabaseConfig;
import pe.edu.vallegrande.bigdata.spark.database.SchemaInstaller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StagingSeederTest {

    @TempDir
    Path directory;

    private final DatabaseConfig config = new DatabaseConfig("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "x", null);

    private void createTables() throws Exception {
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password())) {
            SchemaInstaller.install(connection);
        }
    }

    @Test
    void theTablesMustBeCreatedByTheScriptsFirst() throws Exception {
        writeBatch();

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> StagingSeeder.seed(config, directory));

        assertTrue(error.getMessage().contains("SchemaInstaller"), error.getMessage());
    }

    @Test
    void loadsTheFiveCsvIntoTheStagingTablesAsRawText() throws Exception {
        createTables();
        writeBatch();

        Map<String, Long> loaded = StagingSeeder.seed(config, directory);

        assertEquals(AcademicSchema.TABLES, List.copyOf(loaded.keySet()));
        assertEquals(2L, loaded.get("estudiantes"));
        assertEquals(1L, loaded.get("semestres"));
        assertEquals(2, count("STG_ESTUDIANTES"));
        assertEquals("Ana", query("SELECT NOMBRES FROM STG_ESTUDIANTES WHERE ESTUDIANTE_ID = 'E1'"));
        assertNull(query("SELECT SECCION FROM STG_ESTUDIANTES WHERE ESTUDIANTE_ID = 'E2'"), "una celda vacía se guarda como NULL");
    }

    @Test
    void seedingAgainReplacesTheContentInsteadOfDuplicatingIt() throws Exception {
        createTables();
        writeBatch();

        StagingSeeder.seed(config, directory);
        StagingSeeder.seed(config, directory);

        assertEquals(2, count("STG_ESTUDIANTES"));
    }

    @Test
    void rejectsAFileWhoseHeaderIsNotTheContract() throws Exception {
        createTables();
        writeBatch();
        Files.writeString(directory.resolve("cursos.csv"), "a,b\n1,2\n");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> StagingSeeder.seed(config, directory));

        assertTrue(error.getMessage().contains("cursos.csv"));
    }

    private void writeBatch() throws Exception {
        Files.writeString(directory.resolve("semestres.csv"), "﻿semestre_id,numero,nombre,periodo,fecha_inicio,fecha_fin\nV,5,Quinto,2026-II,2026-08-17,2026-12-18\n");
        Files.writeString(directory.resolve("cursos.csv"), "curso_id,nombre,semestre_id,creditos,horas_semanales,docente\nTBD,Big Data,V,4,6,Docente\n");
        Files.writeString(directory.resolve("estudiantes.csv"), "estudiante_id,nombres,apellidos,correo,semestre_id,seccion\nE1,Ana,Torres,ana@x.pe,V,A\nE2,Luis,Rojas,luis@x.pe,V,\n");
        Files.writeString(directory.resolve("notas.csv"), "estudiante_id,curso_id,periodo,evaluacion,peso,nota,fecha_registro\nE1,TBD,2026-II,EF,1,13,2026-12-07\n");
        Files.writeString(directory.resolve("asistencia.csv"), "estudiante_id,curso_id,periodo,semana,fecha,estado\nE1,TBD,2026-II,1,2026-08-18,P\n");
    }

    private long count(String table) throws Exception {
        return Long.parseLong(query("SELECT COUNT(*) FROM " + table));
    }

    private String query(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        }
    }

}
