package pe.edu.vallegrande.bigdata.api.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail handleNotFound(ResourceNotFoundException error) {
        return problem(HttpStatus.NOT_FOUND, "No encontrado", error.getMessage());
    }

    @ExceptionHandler(InvalidRequestException.class)
    public ProblemDetail handleInvalidRequest(InvalidRequestException error) {
        return problem(HttpStatus.BAD_REQUEST, "Petición inválida", error.getMessage());
    }

    @ExceptionHandler(WorkerUnavailableException.class)
    public ProblemDetail handleWorkerUnavailable(WorkerUnavailableException error) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Worker no disponible", error.getMessage());
    }

    @ExceptionHandler(DatabaseUnavailableException.class)
    public ProblemDetail handleDatabaseUnavailable(DatabaseUnavailableException error) {
        log.error("Base de datos no disponible", error);
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Base de datos no disponible", error.getMessage());
    }

    @ExceptionHandler(ErrorResponseException.class)
    public ProblemDetail handleFrameworkError(ErrorResponseException error) {
        return error.getBody();
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception error) {
        log.error("Error no controlado", error);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno", "Ocurrió un error inesperado");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        return problem;
    }

}
