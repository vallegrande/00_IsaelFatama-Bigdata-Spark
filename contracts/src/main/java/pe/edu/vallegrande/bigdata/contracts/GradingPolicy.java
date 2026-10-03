package pe.edu.vallegrande.bigdata.contracts;

public final class GradingPolicy {

    public static final double MIN_SCORE = 0;

    public static final double MAX_SCORE = 20;

    public static final int PASSING_SCORE = 13;

    public static final double MAX_ABSENCE_RATIO = 0.30;

    public static final double RISK_ABSENCE_RATIO = 0.20;

    public static final double DEFAULT_MAX_REJECTED_RATIO = 0.05;

    public static final String APROBADO = "APROBADO";

    public static final String DESAPROBADO = "DESAPROBADO";

    public static final String INHABILITADO  = "INHABILITADO";

    public static final String PENDIENTE = "PENDIENTE";

    public static final String EN_RIESGO = "EN_RIESGO";

    public static final String NORMAL = "NORMAL";

    private GradingPolicy() {

    }
}
