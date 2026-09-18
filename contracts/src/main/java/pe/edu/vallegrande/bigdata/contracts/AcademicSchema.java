package pe.edu.vallegrande.bigdata.contracts;

import java.util.List;

public final class AcademicSchema {

    public static final List<String> COLUMNS = List.of("record_id", "student_id", "course", "period", "score", "attendance_rate");

    public static final String HEADER = String.join(",", COLUMNS);

    public static final int DEMO_ROWS = 2000;

    public static final int MAX_ROWS = 2000;

    public static final String RULES_VERSION = "1.0.0";

    public AcademicSchema() {

    }
}
