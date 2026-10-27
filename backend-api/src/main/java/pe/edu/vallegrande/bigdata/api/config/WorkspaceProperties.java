package pe.edu.vallegrande.bigdata.api.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;

@Validated
@ConfigurationProperties("bigdata.workspace")
public record WorkspaceProperties(
        @NotNull @DefaultValue("runtime/jobs") Path jobsDirectory,
        @NotNull @DefaultValue("data") Path datasetsDirectory) {
}
