package pe.edu.vallegrande.bigdata.api.exception;

public class DatabaseUnavailableException extends RuntimeException {

    public DatabaseUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
