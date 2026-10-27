package pe.edu.vallegrande.bigdata.api.dto;

import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PageTest {

    @Test
    void pageQueryUsesDefaultsAndComputesTheOffset() {
        PageQuery query = PageQuery.of(null, null);

        assertEquals(0, query.page());
        assertEquals(PageQuery.DEFAULT_SIZE, query.size());
        assertEquals(150, PageQuery.of(3, 50).offset());
    }

    @Test
    void pageQueryRejectsOutOfRangeValues() {
        assertThrows(InvalidRequestException.class, () -> PageQuery.of(-1, 10));
        assertThrows(InvalidRequestException.class, () -> PageQuery.of(0, 0));
        assertThrows(InvalidRequestException.class, () -> PageQuery.of(0, PageQuery.MAX_SIZE + 1));
    }

    @Test
    void pageResponseComputesTheTotalPages() {
        PageResponse<String> page = PageResponse.of(List.of("a", "b"), PageQuery.of(0, 2), 5);

        assertEquals(3, page.totalPages());
        assertEquals(5, page.totalElements());
        assertEquals(0, PageResponse.of(List.<String>of(), PageQuery.of(0, 10), 0).totalPages());
    }

}
