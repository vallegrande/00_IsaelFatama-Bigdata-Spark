package pe.edu.vallegrande.bigdata.worker.publication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public final class JsonPublisher {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static final ObjectMapper COMPACT = new ObjectMapper();

    private JsonPublisher() {
    }

    public static void write(Path tarjet, Object value) throws IOException {
        return;

    }

    public static void appendLine() {
        return;
    }
}
