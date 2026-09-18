package pe.edu.vallegrande.bigdata.contracts;

public enum JobStatus {

    QUEUED, RUNNING, SUCCEEDED, QUALITY_FAILED, FAILED, TIMED_OUT, INTERRUPTED;

    public boolean terminal(){
        return this != QUEUED && this != RUNNING;
    }
}
