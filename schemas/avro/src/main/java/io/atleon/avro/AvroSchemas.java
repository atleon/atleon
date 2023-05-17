package io.atleon.avro;

import io.atleon.util.TypeResolution;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

import java.util.Optional;
import java.util.function.Supplier;

public final class AvroSchemas {

    private AvroSchemas() {

    }

    public static Schema getOrReflectNullable(Object data) {
        return makeNullable(getOrReflect(data));
    }

    public static Schema getOrReflect(Object data) {
        return getOrSupply(data, () -> ReflectData.get().getSchema(data.getClass()));
    }

    public static Schema getOrSupply(Object data, Supplier<Schema> schemaSupplier) {
        try {
            return data instanceof GenericContainer
                ? GenericContainer.class.cast(data).getSchema()
                : SpecificData.get().getSchema(TypeResolution.safelyGetClass(data));
        } catch (AvroRuntimeException e) {
            return schemaSupplier.get();
        }
    }

    public static Schema makeNullable(Schema schema) {
        return isNull(schema) ? schema : ReflectData.makeNullable(schema);
    }

    public static Optional<Schema> reduceNonNull(Schema schema) {
        if (isUnion(schema)) {
            return schema.getTypes().stream().reduce((schema1, schema2) -> isNull(schema1) ? schema2 : schema1);
        } else {
            return isNull(schema) ? Optional.empty() : Optional.of(schema);
        }
    }

    public static boolean isNullable(Schema schema) {
        return isUnion(schema) && schema.getTypes().stream().anyMatch(AvroSchemas::isNull);
    }

    public static boolean isUnion(Schema schema) {
        return schema.getType() == Schema.Type.UNION;
    }

    public static boolean isRecord(Schema schema) {
        return schema.getType() == Schema.Type.RECORD;
    }

    public static boolean isNull(Schema schema) {
        return schema.getType() == Schema.Type.NULL;
    }
}
