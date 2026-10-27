package pe.edu.vallegrande.bigdata.api.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties("bigdata.worker")
public record WorkerProperties(
        @NotNull @DefaultValue("spark") Path modulesDirectory,
        @NotBlank @DefaultValue("java") String javaCommand,
        @Min(1) @DefaultValue("2") int maxConcurrent,
        @NotNull @DefaultValue("30m") Duration timeout,
        @DefaultValue List<String> jvmOptions) {
}
