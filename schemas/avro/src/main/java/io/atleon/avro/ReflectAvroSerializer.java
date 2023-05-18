package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.lang.reflect.Type;

/**
 * An {@link AvroSerializer} that uses Avro's {@link ReflectData} for type loading and
 * {@link ReflectDatumWriter} for serialization.
 *
 * @param <T> The type of data to be serialized
 */
public final class ReflectAvroSerializer<T> extends AvroSerializer<T> {

    private final boolean allowNull;

    public ReflectAvroSerializer() {
        this.allowNull = false;
    }

    private ReflectAvroSerializer(boolean schemaCachingEnabled, boolean schemaGenerationEnabled, boolean allowNull) {
        super(schemaCachingEnabled, schemaGenerationEnabled);
        this.allowNull = allowNull;
    }

    public ReflectAvroSerializer<T> withSchemaCachingEnabled(boolean schemaCachingEnabled) {
        return new ReflectAvroSerializer<>(schemaCachingEnabled, schemaGenerationEnabled, allowNull);
    }

    public ReflectAvroSerializer<T> withSchemaGenerationEnabled(boolean schemaGenerationEnabled) {
        return new ReflectAvroSerializer<>(schemaCachingEnabled, schemaGenerationEnabled, allowNull);
    }

    public ReflectAvroSerializer<T> withAllowNull(boolean allowNull) {
        return new ReflectAvroSerializer<>(schemaCachingEnabled, schemaGenerationEnabled, allowNull);
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        return getReflectData().getSchema(dataType);
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Schema schema) {
        return new ReflectDatumWriter<>(schema, getReflectData());
    }

    private ReflectData getReflectData() {
        return allowNull ? ReflectData.AllowNull.get() : ReflectData.get();
    }
}
