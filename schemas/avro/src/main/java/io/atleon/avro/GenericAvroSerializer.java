package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import java.lang.reflect.Type;

/**
 * An {@link AvroSerializer} that serializes objects that implement
 * {@link org.apache.avro.generic.GenericRecord}
 */
public final class GenericAvroSerializer<T> extends AvroSerializer<T> {

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        throw new UnsupportedOperationException("Cannot load schema when using generic deserialization for type=" + dataType);
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Schema schema) {
        return new GenericDatumWriter<>(schema);
    }
}
