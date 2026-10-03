package pe.edu.vallegrande.bigdata.job.rendimiento.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.bigdata.contracts.GradingPolicy;

import static org.apache.spark.sql.functions.*;

public final class PerformanceBuilder implements GoldTableBuilder {
    private static final String[] ENROLLMENT = {"estudiante_id", "curso_id", "periodo"};

    private static Column[] columns() {
        Column[] columns = new Column[ENROLLMENT.length];
        for (int index = 0; index < ENROLLMENT.length; index++) {
            columns[index] = col(ENROLLMENT[index]);
        }
        return columns;
    }

    @Override
    public String name() {
        return "rendimiento";
    }

    @Override
    public Dataset<Row> build(GoldInputs inputs) {
        return enrollments(inputs)
                .join(gradesByEnrollment(inputs.notas()), ENROLLMENT, "left")
                .join(attendanceByEnrollment(inputs.asistencia()), ENROLLMENT, "left")
                .na().fill(0L, new String[]{"evaluaciones", "evaluaciones_registradas", "sesiones", "presentes",
                        "tardanzas", "faltas", "justificadas"})
                .withColumn("promedio", round(try_divide(col("puntaje"), col("peso_registrado")), 2))
                .withColumn("nota_final", when(finalGradeComplete(), floor(col("puntaje").plus(0.500001)).cast("int")))
                .withColumn("asistencia_pct", round(try_divide(col("presentes").plus(col("tardanzas")),
                        col("sesiones").minus(col("justificadas"))), 4))
                .withColumn("inasistencia_pct", round(try_divide(col("faltas"), col("sesiones")), 4))
                .withColumn("inhabilitado", disqualified())
                .withColumn("condicion", condition())
                .withColumn("evaluaciones_pendientes", col("evaluaciones").minus(col("evaluaciones_registradas")))
                .withColumn("alertas", alerts())
                .withColumn("requiere_atencion", lowAverage().or(disqualified()).or(absenceRisk()))
                .select("estudiante_id", "nombres", "apellidos", "seccion", "semestre_id", "periodo", "curso_id", "curso",
                        "docente", "creditos", "evaluaciones", "evaluaciones_pendientes", "promedio", "nota_final",
                        "sesiones", "presentes", "tardanzas", "faltas", "justificadas", "asistencia_pct",
                        "inasistencia_pct", "condicion", "requiere_atencion", "alertas")
                .orderBy("curso_id", "apellidos", "nombres");
    }

    private Dataset<Row> enrollments(GoldInputs inputs) {
        return inputs.estudiantes()
                .join(inputs.cursos().select(col("curso_id"), col("nombre").alias("curso"), col("semestre_id"),
                        col("creditos"), col("docente")), "semestre_id")
                .join(inputs.semestres().select(col("semestre_id"), col("periodo")), "semestre_id");
    }

    private Dataset<Row> gradesByEnrollment(Dataset<Row> notas) {
        return notas.groupBy(columns()).agg(
                count("*").alias("evaluaciones"),
                count("nota").alias("evaluaciones_registradas"),
                sum("peso").alias("peso_total"),
                sum(when(col("nota").isNotNull(), col("peso")).otherwise(0)).alias("peso_registrado"),
                sum(col("nota").multiply(col("peso"))).alias("puntaje"));
    }

    private Dataset<Row> attendanceByEnrollment(Dataset<Row> asistencia) {
        return asistencia.groupBy(columns()).agg(
                count("*").alias("sesiones"),
                countState("P", "presentes"),
                countState("T", "tardanzas"),
                countState("F", "faltas"),
                countState("J", "justificadas"));
    }

    private Column countState(String state, String alias) {
        return sum(when(col("estado").equalTo(state), 1).otherwise(0)).alias(alias);
    }

    private Column finalGradeComplete() {
        return col("evaluaciones").gt(0)
                .and(col("evaluaciones").equalTo(col("evaluaciones_registradas")))
                .and(abs(col("peso_total").minus(1)).lt(0.000001));
    }

    private Column disqualified() {
        return coalesce(try_divide(col("faltas"), col("sesiones")).gt(GradingPolicy.MAX_ABSENCE_RATIO), lit(false));
    }

    private Column lowAverage() {
        return coalesce(coalesce(col("nota_final"), col("promedio")).lt(GradingPolicy.PASSING_SCORE), lit(false));
    }

    private Column absenceRisk() {
        return coalesce(col("inasistencia_pct").geq(GradingPolicy.RISK_ABSENCE_RATIO), lit(false));
    }

    private Column condition() {
        return when(col("inhabilitado"), GradingPolicy.INHABILITADO)
                .when(col("nota_final").isNull(), GradingPolicy.PENDIENTE)
                .when(col("nota_final").geq(GradingPolicy.PASSING_SCORE), GradingPolicy.APROBADO)
                .otherwise(GradingPolicy.DESAPROBADO);
    }

    private Column alerts() {
        return concat_ws("; ",
                when(lowAverage(), lit("Promedio menor a " + GradingPolicy.PASSING_SCORE)),
                when(disqualified(), lit("Inasistencias mayores al 30 %")),
                when(absenceRisk().and(disqualified().equalTo(false)), lit("Inasistencias en riesgo")),
                when(col("evaluaciones_pendientes").gt(0), lit("Evaluaciones pendientes")));
    }
}
