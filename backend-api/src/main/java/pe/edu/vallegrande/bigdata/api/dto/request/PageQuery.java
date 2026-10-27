package pe.edu.vallegrande.bigdata.api.dto.request;

import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;

public record PageQuery(int page, int size) {

    public static final int DEFAULT_SIZE = 50;

    public static final int MAX_SIZE = 500;

    public PageQuery {
        if (page < 0) {
            throw new InvalidRequestException("La página no puede ser negativa");
        }
        if (size < 1 || size > MAX_SIZE) {
            throw new InvalidRequestException("El tamaño de página debe estar entre 1 y " + MAX_SIZE);
        }
    }

    public static PageQuery of(Integer page, Integer size) {
        return new PageQuery(page == null ? 0 : page, size == null ? DEFAULT_SIZE : size);
    }

    public long offset() {
        return (long) page * size;
    }

}
