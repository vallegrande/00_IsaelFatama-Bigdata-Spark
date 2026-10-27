package pe.edu.vallegrande.bigdata.api.config;

import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("bigdata.database")
public record DatabaseProperties(@Pattern(regexp = "|[A-Za-z][A-Za-z0-9_$#]*") String schema) {

    public String qualify(String table) {
        return schema == null || schema.isBlank() ? table : schema.toUpperCase() + "." + table;
    }

}
