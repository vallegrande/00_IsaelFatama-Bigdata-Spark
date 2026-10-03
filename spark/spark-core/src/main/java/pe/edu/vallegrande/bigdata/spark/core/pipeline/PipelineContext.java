package pe.edu.vallegrande.bigdata.spark.core.pipeline;

import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.bigdata.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.GoldTables;
import pe.edu.vallegrande.bigdata.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.bigdata.spark.core.publication.TablePublisher;
import pe.edu.vallegrande.bigdata.spark.core.quality.QualityReport;

import java.nio.file.Path;

public final class PipelineContext {
    private final SparkSession spark;
    private final Path input;
    private final Path output;
    private final double maxRejectedRatio;
    private final TablePublisher publisher;
    private BronzeTables bronze;
    private ValidationResult validation;
    private QualityReport quality;
    private GoldTables gold;

    public PipelineContext(SparkSession spark, Path input, Path output, double maxRejectedRatio, TablePublisher publisher) {
        this.spark = spark;
        this.input = input;
        this.output = output;
        this.maxRejectedRatio = maxRejectedRatio;
        this.publisher = publisher;
    }

    public SparkSession spark() {
        return spark;
    }

    public Path input() {
        return input;
    }

    public Path output() {
        return output;
    }

    public double maxRejectedRatio() {
        return maxRejectedRatio;
    }

    public TablePublisher publisher() {
        return publisher;
    }

    public BronzeTables bronze() {
        return bronze;
    }

    public void bronze(BronzeTables value) {
        this.bronze = value;
    }

    public ValidationResult validation() {
        return validation;
    }

    public void validation(ValidationResult value) {
        this.validation = value;
    }

    public QualityReport quality() {
        return quality;
    }

    public void quality(QualityReport value) {
        this.quality = value;
    }

    public GoldTables gold() {
        return gold;
    }

    public void gold(GoldTables value) {
        this.gold = value;
    }

    public void release() {
        if (gold != null) {
            gold.release();
        }
        if (validation != null) {
            validation.release();
        }
        if (bronze != null) {
            bronze.release();
        }
    }
}
