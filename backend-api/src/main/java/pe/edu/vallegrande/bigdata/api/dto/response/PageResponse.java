package pe.edu.vallegrande.bigdata.api.dto.response;

import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;

import java.util.List;

public record PageResponse<T>(List<T> content, int page, int size, long totalElements, int totalPages) {

    public static <T> PageResponse<T> of(List<T> content, PageQuery query, long totalElements) {
        int totalPages = (int) Math.ceil((double) totalElements / query.size());
        return new PageResponse<>(List.copyOf(content), query.page(), query.size(), totalElements, totalPages);
    }

}
