package pe.edu.vallegrande.bigdata.spark.database.oracle;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementsTest {

    private final String target = "BIGDATA.CORE_NOTAS";

    private final String load = SqlStatements.loadTable(target);

    @Test
    void theLoadTableIsPermanentAndNamedAfterItsTarget() {
        assertEquals("BIGDATA.CORE_NOTAS_LOAD", load);
    }

    @Test
    void mergeUpdatesNonKeyColumnsAndInsertsTheRest() {
        String sql = SqlStatements.mergeBatch(target, load, List.of("ESTUDIANTE_ID", "EVALUACION", "NOTA"), List.of("ESTUDIANTE_ID", "EVALUACION"));

        assertEquals("""
                MERGE INTO BIGDATA.CORE_NOTAS t
                USING (SELECT ESTUDIANTE_ID, EVALUACION, NOTA FROM BIGDATA.CORE_NOTAS_LOAD WHERE BATCH_ID = ?) s
                ON (t.ESTUDIANTE_ID = s.ESTUDIANTE_ID AND t.EVALUACION = s.EVALUACION)
                WHEN MATCHED THEN UPDATE SET t.NOTA = s.NOTA
                WHEN NOT MATCHED THEN INSERT (ESTUDIANTE_ID, EVALUACION, NOTA) VALUES (s.ESTUDIANTE_ID, s.EVALUACION, s.NOTA)""", sql);
    }

    @Test
    void mergeOfATableWhereEveryColumnIsKeyOnlyInserts() {
        String sql = SqlStatements.mergeBatch(target, load, List.of("A", "B"), List.of("A", "B"));

        assertFalse(sql.contains("WHEN MATCHED"), sql);
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT (A, B)"), sql);
    }

    @Test
    void theBatchIsAlwaysAParameterNeverPartOfTheSql() {
        assertEquals("DELETE FROM BIGDATA.CORE_NOTAS WHERE BATCH_ID = ?", SqlStatements.deleteBatch(target));
        assertEquals("INSERT INTO BIGDATA.CORE_NOTAS (A, B) SELECT A, B FROM BIGDATA.CORE_NOTAS_LOAD WHERE BATCH_ID = ?",
                SqlStatements.insertBatch(target, load, List.of("A", "B")));
        assertEquals("SELECT COUNT(*) FROM BIGDATA.CORE_NOTAS WHERE BATCH_ID = ?", SqlStatements.countBatch(target));
    }

}
