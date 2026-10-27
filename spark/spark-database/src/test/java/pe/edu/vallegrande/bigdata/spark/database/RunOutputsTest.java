package pe.edu.vallegrande.bigdata.spark.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunOutputsTest {

    @TempDir
    Path temp;

    @Test
    void theBatchIdIsTheRunFolderName() {
        assertEquals("3f2a9c1e", RunOutputs.batchId(temp.resolve("3f2a9c1e")));
    }

    @Test
    void listsOnlyParquetTablesSorted() throws IOException {
        Path gold = Files.createDirectories(temp.resolve("gold"));
        Files.createFile(gold.resolve("rendimiento.parquet"));
        Files.createFile(gold.resolve("rendimiento.csv"));
        Files.createFile(gold.resolve("cursos_resumen.parquet"));
        Files.createFile(gold.resolve("resumen.json"));

        assertEquals(List.of("cursos_resumen", "rendimiento"), RunOutputs.tables(temp, "gold"));
    }

    @Test
    void aMissingLayerHasNoTables() {
        assertTrue(RunOutputs.tables(temp, "gold").isEmpty());
    }

    @Test
    void existsChecksTheParquetFile() throws IOException {
        Path silver = Files.createDirectories(temp.resolve("silver"));
        Files.createFile(silver.resolve("notas.parquet"));

        assertTrue(RunOutputs.exists(temp, "silver", "notas"));
        assertFalse(RunOutputs.exists(temp, "silver", "cursos"));
    }

    @Test
    void readingAMissingTableExplainsWhichRunLacksIt() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RunOutputs.read(null, temp, "silver", "notas"));

        assertTrue(error.getMessage().contains("silver/notas.parquet"));
    }

}
