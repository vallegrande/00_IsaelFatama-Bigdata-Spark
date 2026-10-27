package pe.edu.vallegrande.bigdata.api.repository.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import pe.edu.vallegrande.bigdata.api.config.DatabaseProperties;
import pe.edu.vallegrande.bigdata.api.constant.SqlQueries;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;
import pe.edu.vallegrande.bigdata.api.exception.DatabaseUnavailableException;
import pe.edu.vallegrande.bigdata.api.exception.InvalidRequestException;
import pe.edu.vallegrande.bigdata.api.model.ServingSnapshot;
import pe.edu.vallegrande.bigdata.api.repository.ServingRepository;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;
import pe.edu.vallegrande.bigdata.contracts.JobType;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

@Repository
@RequiredArgsConstructor
public class JdbcServingRepository implements ServingRepository {

    private static final Set<String> TECHNICAL_COLUMNS = Set.of(
            DatabaseLayer.BATCH_COLUMN, DatabaseLayer.ROW_COLUMN, DatabaseLayer.LOADED_COLUMN);

    private static final int ORACLE_BINARY_FLOAT = 100;

    private static final int ORACLE_BINARY_DOUBLE = 101;

    private static final String ORACLE_DATE = "DATE";

    private static final String DATABASE_ERROR = "No se pudo consultar la base de datos";

    private final JdbcTemplate jdbc;

    private final DatabaseProperties properties;

    private final ObjectMapper objectMapper;

    @Override
    public Mono<ServingSnapshot> findCurrent(JobType type) {
        return blocking(() -> {
            String sql = SqlQueries.CURRENT_BATCH.formatted(properties.qualify(SqlQueries.SUMMARY_TABLE));
            List<ServingSnapshot> found = jdbc.query(sql, (row, index) -> new ServingSnapshot(
                    row.getString(1), row.getTimestamp(2).toInstant(), parse(row.getString(3))), type.name());
            return found.isEmpty() ? null : found.get(0);
        });
    }

    @Override
    public Mono<PageResponse<Map<String, Object>>> findPage(String batchId, String table, PageQuery query) {
        return blocking(() -> {
            String name = properties.qualify(tableName(table));
            Long total = jdbc.queryForObject(SqlQueries.COUNT_ROWS.formatted(name), Long.class, batchId);
            List<Map<String, Object>> rows = jdbc.query(SqlQueries.PAGE_ROWS.formatted(name),
                    (row, index) -> toMap(row), batchId, query.offset(), query.size());
            return PageResponse.of(rows, query, total == null ? 0 : total);
        });
    }

    private String tableName(String table) {
        if (!JobType.servingTables().contains(table)) {
            throw new InvalidRequestException("La tabla '" + table + "' no existe en la capa de consulta");
        }
        return SqlQueries.SERVING_PREFIX + table.toUpperCase(Locale.ROOT);
    }

    private <T> Mono<T> blocking(Callable<T> work) {
        return Mono.fromCallable(work)
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorMap(DataAccessException.class,
                        error -> new DatabaseUnavailableException(DATABASE_ERROR + ": " + rootMessage(error), error));
    }

    private String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }

    private JsonNode parse(String content) {
        try {
            return objectMapper.readTree(content);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("El resumen guardado en la base no es un JSON válido", error);
        }
    }

    private Map<String, Object> toMap(ResultSet row) throws SQLException {
        ResultSetMetaData meta = row.getMetaData();
        Map<String, Object> values = new LinkedHashMap<>();
        for (int column = 1; column <= meta.getColumnCount(); column++) {
            String name = meta.getColumnLabel(column).toLowerCase(Locale.ROOT);
            if (!TECHNICAL_COLUMNS.contains(name)) {
                values.put(name, value(row, meta, column));
            }
        }
        return values;
    }

    private Object value(ResultSet row, ResultSetMetaData meta, int column) throws SQLException {
        if (row.getObject(column) == null) {
            return null;
        }
        return switch (meta.getColumnType(column)) {
            case Types.BOOLEAN, Types.BIT -> row.getBoolean(column);
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> row.getLong(column);
            case Types.NUMERIC, Types.DECIMAL -> number(meta, column, row.getBigDecimal(column));
            case Types.FLOAT, Types.REAL, Types.DOUBLE, ORACLE_BINARY_FLOAT, ORACLE_BINARY_DOUBLE -> row.getDouble(column);
            case Types.DATE -> row.getDate(column).toLocalDate().toString();
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> timestamp(row, meta, column);
            default -> row.getString(column);
        };
    }

    private String timestamp(ResultSet row, ResultSetMetaData meta, int column) throws SQLException {
        LocalDateTime moment = row.getTimestamp(column).toLocalDateTime();
        return ORACLE_DATE.equalsIgnoreCase(meta.getColumnTypeName(column)) ? moment.toLocalDate().toString() : moment.toString();
    }

    private Object number(ResultSetMetaData meta, int column, BigDecimal value) throws SQLException {
        if (meta.getScale(column) != 0) {
            return value.doubleValue();
        }
        if (meta.getPrecision(column) == 1) {
            return value.signum() != 0;
        }
        return value.longValueExact();
    }

}
