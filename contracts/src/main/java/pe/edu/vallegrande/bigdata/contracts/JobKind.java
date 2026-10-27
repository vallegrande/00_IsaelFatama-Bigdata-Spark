package pe.edu.vallegrande.bigdata.contracts;

public enum JobKind {

    TRANSFORMATION("Transformation", JobInput.CSV_FILES, GradingPolicy.DEFAULT_MAX_REJECTED_RATIO),
    INGESTION("Ingestion", JobInput.DATASET, 0),
    EXTRACTION("Extraction", JobInput.NONE, 0),
    LOAD("Load", JobInput.PREVIOUS_EXECUTION, 0),
    RECONCILIATION("Reconciliation", JobInput.PREVIOUS_EXECUTION, 0);

    private final String label;

    private final JobInput input;

    private final double defaultThreshold;

    JobKind(String label, JobInput input, double defaultThreshold) {
        this.label = label;
        this.input = input;
        this.defaultThreshold = defaultThreshold;
    }

    public String label() {
        return label;
    }

    public JobInput input() {
        return input;
    }

    public double defaultThreshold() {
        return defaultThreshold;
    }

}
