package pe.edu.vallegrande.bigdata.api.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatasetServiceImplTest {

    @TempDir
    Path root;

    ExecutionWorkspace workspace;

    DatasetServiceImpl service;

    @BeforeEach
    void setUp() {
        workspace = TestSupport.workspace(root);
        service = new DatasetServiceImpl(workspace);
    }

    @Test
    void listsOnlyCompleteDatasetsSorted() throws IOException {
        dataset("limpio", AcademicSchema.TABLES);
        dataset("con-errores", AcademicSchema.TABLES);
        dataset("incompleto", List.of("notas", "cursos"));
        Files.writeString(workspace.datasetsDirectory().resolve("suelto.txt"), "no es un dataset");

        assertEquals(List.of("con-errores", "limpio"), service.findAll());
    }

    @Test
    void withoutADatasetsFolderTheListIsEmpty() {
        assertEquals(List.of(), service.findAll());
    }

    @Test
    void resolvesTheFolderOfACompleteDataset() throws IOException {
        dataset("limpio", AcademicSchema.TABLES);

        assertEquals(workspace.datasetDirectory("limpio"), service.resolve("limpio"));
    }

    @Test
    void anUnknownDatasetIsNotFound() {
        assertThrows(ResourceNotFoundException.class, () -> service.resolve("no-existe"));
    }

    @Test
    void anIncompleteDatasetSaysWhichFilesAreMissing() throws IOException {
        dataset("incompleto", List.of("semestres", "cursos", "estudiantes"));

        InvalidRequestException error = assertThrows(InvalidRequestException.class, () -> service.resolve("incompleto"));

        assertTrue(error.getMessage().contains("notas.csv"));
        assertTrue(error.getMessage().contains("asistencia.csv"));
    }

    @Test
    void anInvalidNameIsRejectedBeforeTouchingTheDisk() {
        assertThrows(InvalidRequestException.class, () -> service.resolve("../jobs"));
    }

    private void dataset(String name, List<String> tables) throws IOException {
        Path directory = Files.createDirectories(workspace.datasetsDirectory().resolve(name));
        for (String table : tables) {
            Files.writeString(directory.resolve(table + ".csv"), "x\n");
        }
    }

}
