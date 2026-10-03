package pe.edu.vallegrande.bigdata.spark.core.publication;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

public final class ParquetPublisher implements TableFormat {

    @Override
    public String extension() {
        return "parquet";
    }


    @Override
    public void write(Path target, StructType schema, List<Row> rows) throws IOException {
        Path temporary = AtomicFiles.temporaryFor(target);
        MessageType parquetSchema = toParquet(schema);
        SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
        StructField[] fields = schema.fields();
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(temporary))
                .withConf(new PlainParquetConfiguration())
                .withType(parquetSchema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (Row row : rows) {
                Group group = groups.newGroup();
                for (int index = 0; index < fields.length; index++) {
                    if (!row.isNullAt(index)) {
                        append(group, index, row.get(index), fields[index].dataType());
                    }
                }
                writer.write(group);
            }
        }
        AtomicFiles.publish(temporary, target);
    }

    private MessageType toParquet(StructType schema) {
        var message = Types.buildMessage();
        for (StructField field : schema.fields()) {
            DataType type = field.dataType();
            if (type instanceof StringType) {
                message.addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(field.name()));
            } else if (type instanceof BooleanType) {
                message.addField(Types.optional(PrimitiveTypeName.BOOLEAN).named(field.name()));
            } else if (type instanceof LongType) {
                message.addField(Types.optional(PrimitiveTypeName.INT64).named(field.name()));
            } else if (type instanceof IntegerType) {
                message.addField(Types.optional(PrimitiveTypeName.INT32).named(field.name()));
            } else if (type instanceof DoubleType) {
                message.addField(Types.optional(PrimitiveTypeName.DOUBLE).named(field.name()));
            } else if (type instanceof DateType) {
                message.addField(Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(field.name()));
            } else {
                throw new IllegalArgumentException("Tipo no soportado en Parquet: " + field.name() + " " + type);
            }
        }
        return message.named("vallegrande_academic");
    }

    private void append(Group group, int index, Object value, DataType type) {
        if (type instanceof StringType) {
            group.add(index, value.toString());
        } else if (type instanceof BooleanType) {
            group.add(index, (Boolean) value);
        } else if (type instanceof LongType) {
            group.add(index, ((Number) value).longValue());
        } else if (type instanceof IntegerType) {
            group.add(index, ((Number) value).intValue());
        } else if (type instanceof DoubleType) {
            group.add(index, ((Number) value).doubleValue());
        } else if (type instanceof DateType) {
            group.add(index, (int) ((LocalDate) value).toEpochDay());
        }
    }
}
