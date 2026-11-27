package pe.edu.vallegrande.bigdata.job.carga;

public record LoadResult(String table, long inserted, long updated) {

    public long total() {
        return inserted + updated;
    }
}
