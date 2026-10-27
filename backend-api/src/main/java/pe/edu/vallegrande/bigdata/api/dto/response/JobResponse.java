package pe.edu.vallegrande.bigdata.api.dto.response;

import java.util.List;

public record JobResponse(
        String type,
        String label,
        String kind,
        String kindLabel,
        String input,
        String module,
        double defaultThreshold,
        List<String> goldTables) {
}
