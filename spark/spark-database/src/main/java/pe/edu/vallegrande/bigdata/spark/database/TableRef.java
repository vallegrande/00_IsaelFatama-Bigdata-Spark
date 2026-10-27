package pe.edu.vallegrande.bigdata.spark.database;

import pe.edu.vallegrande.bigdata.contracts.DatabaseLayer;

import java.util.List;
import java.util.Objects;

public record TableRef(DatabaseLayer layer, String name, List<String> keys) {

    public TableRef {
        Objects.requireNonNull(layer, "layer");
        Objects.requireNonNull(name, "name");
        keys = List.copyOf(Objects.requireNonNull(keys, "keys"));
    }

    public static TableRef of(DatabaseLayer layer, String name) {
        return new TableRef(layer, name, List.of());
    }

    public static TableRef of(DatabaseLayer layer, String name, List<String> keys) {
        return new TableRef(layer, name, keys);
    }

    @Override
    public String toString() {
        return layer + "." + name;
    }

}
