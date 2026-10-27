package pe.edu.vallegrande.bigdata.api.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pe.edu.vallegrande.bigdata.api.component.ExecutionWorkspace;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.service.DatasetService;
import pe.edu.vallegrande.bigdata.contracts.AcademicSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class DatasetServiceImpl implements DatasetService {

    private final ExecutionWorkspace executionWorkspace;

    @Override
    public List<String> findAll() {
        Path root = executionWorkspace.datasetsDirectory();
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (Stream<Path> directories = Files.list(root)) {
            return directories
                    .filter(Files::isDirectory)
                    .filter(directory -> missingFiles(directory).isEmpty())
                    .map(directory -> directory.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (IOException error) {
            throw new UncheckedIOException("No se pudo listar " + root, error);
        }
    }

    @Override
    public Path resolve(String name) {
        Path directory = executionWorkspace.datasetDirectory(name);
        if (!Files.isDirectory(directory)) {
            throw new ResourceNotFoundException("No existe el dataset '" + name + "'");
        }
        List<String> missing = missingFiles(directory);
        if (!missing.isEmpty()) {
            throw new InvalidRequestException("El dataset '" + name + "' no tiene los archivos: " + String.join(", ", missing));
        }
        return directory;
    }

    private List<String> missingFiles(Path directory) {
        return AcademicSchema.TABLES.stream()
                .filter(table -> !Files.isRegularFile(directory.resolve(table + ".csv")))
                .map(table -> table + ".csv")
                .toList();
    }

}
