package pe.edu.vallegrande.bigdata.spark.database;

import org.junit.jupiter.api.Test;
import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatabaseNamingTest {

    @Test
    void tablesGetTheLayerPrefixInUpperCase() {
        assertEquals("CORE_NOTAS", DatabaseNaming.physicalTable(TableRef.of(DatabaseLayer.CORE, "notas")));
        assertEquals("SERVING_CURSOS_RESUMEN", DatabaseNaming.physicalTable(TableRef.of(DatabaseLayer.SERVING, "cursos_resumen")));
        assertEquals("STG_ASISTENCIA", DatabaseNaming.physicalTable(TableRef.of(DatabaseLayer.STAGING, "asistencia")));
        assertEquals("ETL_RECHAZADOS", DatabaseNaming.physicalTable(TableRef.of(DatabaseLayer.CONTROL, "rechazados")));
    }

    @Test
    void columnsAreUpperCased() {
        assertEquals("ESTUDIANTE_ID", DatabaseNaming.physicalColumn("estudiante_id"));
        assertEquals("BATCH_ID", DatabaseNaming.physicalColumn(DatabaseLayer.BATCH_COLUMN));
    }

    @Test
    void rejectsIdentifiersThatNeedQuotes() {
        assertThrows(IllegalArgumentException.class, () -> DatabaseNaming.physicalColumn("nota final"));
        assertThrows(IllegalArgumentException.class, () -> DatabaseNaming.physicalColumn("1nota"));
        assertThrows(IllegalArgumentException.class, () -> DatabaseNaming.physicalColumn("nota;drop"));
    }

    @Test
    void rejectsIdentifiersThatAreTooLong() {
        assertThrows(IllegalArgumentException.class, () -> DatabaseNaming.physicalColumn("a".repeat(129)));
    }

}
