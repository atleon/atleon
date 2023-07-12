package io.atleon.avro;

import io.atleon.schema.KeyableSchema;
import io.atleon.schema.SchematicDeserializer;
import io.atleon.util.Throwing;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * This deserializer makes a best effort to take advantage of Avro Compatibility rules such that
 * deserialization does not break over time as writers may update their schemas. In other words, as
 * long as Avro writers make forward compatible changes to their schemas, this deserialization
 * should not break, even once writers take advantage of those changes (i.e. by populating data for
 * newly-added fields). This is accomplished by attempting to load appropriate reader Schemas to
 * match with any given writer Schema. Doing so requires not just deduction of the runtime types
 * being deserialized, but also instantiation of those types to cover cases when a data type may be
 * able to explicitly say what its Schema is (i.e. in the case of
 * {@link org.apache.avro.generic.GenericContainer GenericContainer}).
 *
 * <p>This "reference data instantiation" indirectly allows this deserializer to also handle
 * generic deserialization types (however inadvisable that may be). Generic data fields are
 * recursively instantiated based on writer schema-specified type information, and when coupled
 * with reader schema generation based on that instantiated reference data, continues to allow
 * backward compatible deserialization of those generic data types.
 */
public final class AvroDeserializer<T> implements SchematicDeserializer<T, Schema> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDeserializer.class);

    private final AvroSchemaCache<Serializable> readerSchemasByWriterKey = new AvroSchemaCache<>();

    private final GenericData genericData;

    private final Function<Type, Schema> typeSchemaLoader;

    private final boolean readerSchemaLoadingEnabled;

    private final boolean readerReferenceSchemaGenerationEnabled;

    AvroDeserializer(GenericData genericData, Function<Type, Schema> typeSchemaLoader) {
        this(genericData, typeSchemaLoader, true, false);
    }

    private AvroDeserializer(
        GenericData genericData,
        Function<Type, Schema> typeSchemaLoader,
        boolean readerSchemaLoadingEnabled,
        boolean readerReferenceSchemaGenerationEnabled
    ) {
        this.genericData = genericData;
        this.typeSchemaLoader = typeSchemaLoader;
        this.readerSchemaLoadingEnabled = readerSchemaLoadingEnabled;
        this.readerReferenceSchemaGenerationEnabled = readerReferenceSchemaGenerationEnabled;
    }

    public static <T> AvroDeserializer<T> generic() {
        return create(GenericData.get());
    }

    public static <T> AvroDeserializer<T> specific() {
        return create(SpecificData.get());
    }

    public static <T> AvroDeserializer<T> reflect() {
        return create(AtleonReflectData.get());
    }

    public static <T> AvroDeserializer<T> create(GenericData genericData) {
        return new AvroDeserializer<>(genericData, GenericDatas.createTypeSchemaLoader(genericData));
    }

    public AvroDeserializer<T> withReaderSchemaLoadingEnabled(boolean readerSchemaLoadingEnabled) {
        return new AvroDeserializer<>(
            genericData,
            typeSchemaLoader,
            readerSchemaLoadingEnabled,
            readerReferenceSchemaGenerationEnabled
        );
    }

    public AvroDeserializer<T> withReaderReferenceSchemaGenerationEnabled(boolean readerReferenceSchemaGenerationEnabled) {
        return new AvroDeserializer<>(
            genericData,
            typeSchemaLoader,
            readerSchemaLoadingEnabled,
            readerReferenceSchemaGenerationEnabled
        );
    }

    @Override
    public T deserialize(byte[] data, Function<ByteBuffer, KeyableSchema<Schema>> dataBufferToWriterSchema) {
        try {
            return deserializeUnsafe(data, dataBufferToWriterSchema);
        } catch (IOException e) {
            throw Throwing.propagate(e);
        }
    }

    private T deserializeUnsafe(
        byte[] data,
        Function<ByteBuffer, KeyableSchema<Schema>> dataBufferToWriterSchema
    ) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        KeyableSchema<Schema> keyableWriterSchema = dataBufferToWriterSchema.apply(buffer);
        Schema readerSchema = keyableWriterSchema.key()
            .map(writerKey -> readerSchemasByWriterKey.load(writerKey, __ -> loadReaderSchema(keyableWriterSchema.schema())))
            .orElseGet(() -> loadReaderSchema(keyableWriterSchema.schema()));
        Decoder decoder = DecoderFactory.get()
            .binaryDecoder(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), null);
        return createDatumReader(keyableWriterSchema.schema(), readerSchema).read(null, decoder);
    }

    private Schema loadReaderSchema(Schema writerSchema) {
        try {
            return readerSchemaLoadingEnabled ? loadReaderSchemaUnsafe(writerSchema) : writerSchema;
        } catch (Exception e) {
            LOGGER.error("Failed to load readerSchema. Defaulting to writerSchema={}", writerSchema, e);
            return writerSchema;
        }
    }

    private Schema loadReaderSchemaUnsafe(Schema writerSchema) {
        // Note: If deserialization type is a SpecificRecord, it conventionally has a no-arg public constructor
        Object referenceData = AvroDeserialization.instantiateReferenceData(writerSchema);
        return AvroSchemas.getOrSupply(referenceData, () -> loadReaderSchemaUnsafe(writerSchema, referenceData));
    }

    private Schema loadReaderSchemaUnsafe(Schema writerSchema, Object referenceData) {
        return readerReferenceSchemaGenerationEnabled
            ? AvroDeserialization.generateReaderReferenceSchema(referenceData, writerSchema, typeSchemaLoader)
            : typeSchemaLoader.apply(referenceData.getClass());
    }

    private DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return (DatumReader<T>) genericData.createDatumReader(writerSchema, readerSchema);
    }
}
