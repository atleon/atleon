package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import java.lang.reflect.Type;

/**
 * An {@link AvroDeserializer} that deserializes into instances of
 * {@link org.apache.avro.specific.SpecificRecord}, which are generally objects generated from Avro
 * schema files (i.e. AVDL, AVSC, etc.)
 */
public final class SpecificAvroDeserializer<T> extends AvroDeserializer<T> {

    public SpecificAvroDeserializer() {

    }

    private SpecificAvroDeserializer(boolean readerSchemaLoadingEnabled, boolean readerReferenceSchemaGenerationEnabled) {
        super(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled);
    }

    public SpecificAvroDeserializer<T> withReaderSchemaLoadingEnabled(boolean readerSchemaLoadingEnabled) {
        return new SpecificAvroDeserializer<>(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled);
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        return SpecificData.get().getSchema(dataType);
    }

    @Override
    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return new SpecificDatumReader<>(writerSchema, readerSchema);
    }
}
