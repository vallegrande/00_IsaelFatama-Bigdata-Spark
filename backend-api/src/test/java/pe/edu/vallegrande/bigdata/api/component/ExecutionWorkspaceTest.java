package pe.edu.vallegrande.bigdata.api.component;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.support.TestSupport;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionWorkspaceTest {

    @TempDir
    Path root;

    @Test
    void buildsPathsInsideTheWorkspace() {
        ExecutionWorkspace workspace = TestSupport.workspace(root);

        Path output = workspace.outputDirectory("3f2a9c1e-aaaa");

        assertTrue(output.startsWith(workspace.jobsDirectory()));
        assertEquals("execution.json", workspace.executionFile("3f2a9c1e-aaaa").getFileName().toString());
        assertTrue(workspace.datasetDirectory("limpio").startsWith(root.toAbsolutePath().normalize().resolve("datasets")));
    }

    @Test
    void rejectsIdentifiersThatTryToEscapeTheWorkspace() {
        ExecutionWorkspace workspace = TestSupport.workspace(root);

        assertThrows(InvalidRequestException.class, () -> workspace.outputDirectory(".."));
        assertThrows(InvalidRequestException.class, () -> workspace.outputDirectory("../secreto"));
        assertThrows(InvalidRequestException.class, () -> workspace.outputDirectory("a/b"));
        assertThrows(InvalidRequestException.class, () -> workspace.outputDirectory(""));
        assertThrows(InvalidRequestException.class, () -> workspace.outputDirectory(null));
    }

    @Test
    void rejectsDatasetNamesThatTryToEscapeTheDatasetsFolder() {
        ExecutionWorkspace workspace = TestSupport.workspace(root);

        assertThrows(InvalidRequestException.class, () -> workspace.datasetDirectory(".."));
        assertThrows(InvalidRequestException.class, () -> workspace.datasetDirectory("../jobs"));
        assertThrows(InvalidRequestException.class, () -> workspace.datasetDirectory("a/b"));
        assertThrows(InvalidRequestException.class, () -> workspace.datasetDirectory(" "));
        assertThrows(InvalidRequestException.class, () -> workspace.datasetDirectory(null));
        assertEquals("con-errores", workspace.datasetDirectory("con-errores").getFileName().toString());
    }

    @Test
    void generatedIdsAreValid() {
        ExecutionWorkspace workspace = TestSupport.workspace(root);

        assertTrue(ExecutionWorkspace.isValidId(workspace.newId()));
        assertFalse(ExecutionWorkspace.isValidId("con espacio"));
    }

}
