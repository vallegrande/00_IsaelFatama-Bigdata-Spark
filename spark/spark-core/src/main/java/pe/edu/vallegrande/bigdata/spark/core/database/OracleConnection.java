package pe.edu.vallegrande.bigdata.spark.core.database;

import oracle.jdbc.driver.SQLStateMapping;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public record OracleConnection(String url, String user, String password) implements Serializable {

    public static final String ENV_URL = "DB_URL";

    public static final String ENV_USER = "DB_USER";

    public static final String ENV_PASSWORD = "DB_PASSWORD";

    public static final String DRIVER = "oracle.jdbc.OracleDriver";

    private static final String MISSING = "Define las variables de entorno: " + ENV_URL + ", " + ENV_USER + ", " +
            ENV_PASSWORD;


    public static OracleConnection fromEnvironment() {
        String url = System.getenv(ENV_URL);
        String user = System.getenv(ENV_USER);
        String password = System.getenv(ENV_PASSWORD);
        if (isBlank(url) || isBlank(user) || isBlank(password)) {
            throw new IllegalStateException(MISSING);
        }
        return new OracleConnection(url, user, password);
    }


    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    @Override
    public String toString() {
        return "OracleConnection[url=" + url + ", user=" + user + "]";
    }

    public Connection open() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public Properties sparkProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("driver", DRIVER);
        properties.setProperty("oracle.jdbc.mapDateToTimestamp", "false");
        return properties;
    }
}
