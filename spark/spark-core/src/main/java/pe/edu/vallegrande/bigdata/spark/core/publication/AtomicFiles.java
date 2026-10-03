package pe.edu.vallegrande.bigdata.spark.core.publication;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class AtomicFiles {
    private static final int ATTEMPTS = 10;

    private AtomicFiles() {
    }

    static Path temporaryFor(Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        Files.deleteIfExists(temporary);
        return temporary;
    }

    static void publish(Path temporary, Path target) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (AccessDeniedException locked) {
                if (attempt >= ATTEMPTS) {
                    throw locked;
                }
                try {
                    Thread.sleep(attempt * 25L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Publicación interrumpida", interrupted);
                }
            }
        }
    }
}
