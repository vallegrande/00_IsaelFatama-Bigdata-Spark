package pe.edu.vallegrande.bigdata.api.component;

import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.config.WorkspaceProperties;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.contracts.WorkerProtocol;

import java.nio.file.Path;
import java.util.UUID;
import java.util.regex.Pattern;

@Component
public class ExecutionWorkspace {

    private static final Pattern VALID_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9-]{0,63}");

    private static final Pattern VALID_DATASET = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,63}");

    private final Path jobsDirectory;

    private final Path datasetsDirectory;

    public ExecutionWorkspace(WorkspaceProperties properties) {
        this.jobsDirectory = properties.jobsDirectory().toAbsolutePath().normalize();
        this.datasetsDirectory = properties.datasetsDirectory().toAbsolutePath().normalize();
    }

    public static boolean isValidId(String id) {
        return id != null && VALID_ID.matcher(id).matches();
    }

    public String newId() {
        return UUID.randomUUID().toString();
    }

    public Path jobsDirectory() {
        return jobsDirectory;
    }

    public Path datasetsDirectory() {
        return datasetsDirectory;
    }

    public Path outputDirectory(String executionId) {
        if (!isValidId(executionId)) {
            throw new InvalidRequestException("Identificador de ejecución inválido");
        }
        return inside(jobsDirectory, executionId, "Identificador de ejecución inválido");
    }

    public Path datasetDirectory(String name) {
        if (name == null || !VALID_DATASET.matcher(name).matches()) {
            throw new InvalidRequestException("Nombre de dataset inválido");
        }
        return inside(datasetsDirectory, name, "Nombre de dataset inválido");
    }

    public Path executionFile(String executionId) {
        return outputDirectory(executionId).resolve(WorkerProtocol.EXECUTION_FILE);
    }

    private static Path inside(Path root, String child, String message) {
        Path path = root.resolve(child).normalize();
        if (!path.startsWith(root)) {
            throw new InvalidRequestException(message);
        }
        return path;
    }

}
