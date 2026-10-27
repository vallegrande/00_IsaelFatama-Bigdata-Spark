package pe.edu.vallegrande.bigdata.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

@ConfigurationProperties("bigdata.cors")
public record CorsProperties(@DefaultValue List<String> allowedOrigins) {
}
