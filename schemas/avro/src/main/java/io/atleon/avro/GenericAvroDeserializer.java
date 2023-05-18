package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.lang.reflect.Type;

/**
 * An {@link AvroDeserializer} that deserializes into instances of
 * {@link org.apache.avro.generic.GenericRecord}
 */
public final class GenericAvroDeserializer<T> extends AvroDeserializer<T> {

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        throw new UnsupportedOperationException("Cannot load schema when using generic deserialization for type=" + dataType);
    }

    @Override
    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return new GenericDatumReader<>(writerSchema, readerSchema);
    }
}
