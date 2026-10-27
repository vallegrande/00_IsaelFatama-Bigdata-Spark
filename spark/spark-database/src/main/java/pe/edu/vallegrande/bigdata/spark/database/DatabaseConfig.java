package pe.edu.vallegrande.bigdata.spark.database;

import java.util.function.Function;

public record DatabaseConfig(String url, String user, String password, String schema) {

    public static final String ENV_URL = "DB_URL";

    public static final String ENV_USER = "DB_USER";

    public static final String ENV_PASSWORD = "DB_PASSWORD";

    public static final String ENV_SCHEMA = "DB_SCHEMA";

    public static final String DEFAULT_URL = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1";

    public static final String DEFAULT_USER = "bigdata";

    public static final String MISSING_PASSWORD = "Defina la variable de entorno " + ENV_PASSWORD
            + " (y " + ENV_URL + " o " + ENV_USER + " si no usa el Oracle local por defecto)";

    public static DatabaseConfig fromEnvironment() {
        return from(System::getenv);
    }

    public static DatabaseConfig requiredFromEnvironment() {
        DatabaseConfig config = fromEnvironment();
        if (!config.isConfigured()) {
            throw new IllegalStateException(MISSING_PASSWORD);
        }
        return config;
    }

    static DatabaseConfig from(Function<String, String> environment) {
        return new DatabaseConfig(
                orDefault(environment.apply(ENV_URL), DEFAULT_URL),
                orDefault(environment.apply(ENV_USER), DEFAULT_USER),
                environment.apply(ENV_PASSWORD),
                environment.apply(ENV_SCHEMA));
    }

    public boolean isConfigured() {
        return notBlank(url) && notBlank(user) && notBlank(password);
    }

    private static String orDefault(String value, String fallback) {
        return notBlank(value) ? value.trim() : fallback;
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    @Override
    public String toString() {
        return "DatabaseConfig[url=" + url + ", user=" + user + ", password=" + (notBlank(password) ? "****" : "(vacía)")
                + ", schema=" + schema + "]";
    }

}
