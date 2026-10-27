package pe.edu.vallegrande.bigdata.api.config;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.cors.reactive.CorsWebFilter;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CorsConfigTest {

    private final CorsWebFilter filter = new CorsConfig().corsWebFilter(new CorsProperties(List.of("http://localhost:5173")));

    @Test
    void aPreflightFromAnAllowedOriginIsAnswered() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.options("http://localhost:8080/api/jobs")
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "GET"));
        AtomicBoolean reachedTheController = new AtomicBoolean();

        filter.filter(exchange, e -> {
            reachedTheController.set(true);
            return Mono.empty();
        }).block();

        assertEquals("http://localhost:5173", exchange.getResponse().getHeaders().getFirst("Access-Control-Allow-Origin"));
        assertFalse(reachedTheController.get());
    }

    @Test
    void aPreflightFromAnotherOriginIsForbidden() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.options("http://localhost:8080/api/jobs")
                .header("Origin", "http://sitio-malicioso.com")
                .header("Access-Control-Request-Method", "GET"));

        filter.filter(exchange, e -> Mono.empty()).block();

        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
        assertNull(exchange.getResponse().getHeaders().getFirst("Access-Control-Allow-Origin"));
    }

    @Test
    void aSimpleRequestFromAnAllowedOriginGetsTheHeader() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost:8080/api/jobs")
                .header("Origin", "http://localhost:5173"));
        AtomicBoolean reachedTheController = new AtomicBoolean();

        filter.filter(exchange, e -> {
            reachedTheController.set(true);
            return Mono.empty();
        }).block();

        assertTrue(reachedTheController.get());
        assertEquals("http://localhost:5173", exchange.getResponse().getHeaders().getFirst("Access-Control-Allow-Origin"));
    }

}
