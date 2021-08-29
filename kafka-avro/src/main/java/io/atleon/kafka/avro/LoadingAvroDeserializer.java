package io.atleon.kafka.avro;

import io.atleon.util.ConfigLoading;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This Deserializer makes a best effort to take advantage of Avro Compatibility rules such that
 * deserialization does not break over time as writers may update their schemas. In other words, as
 * long as Avro writers make backward compatible changes to their schemas, this deserialization
 * should not break, even once writers take advantage of those changes (i.e. by populating data for
 * newly-added fields). This is accomplished by attempting to load appropriate reader Schemas to
 * match with any given writer Schema. Doing so requires not just deduction of the runtime types
 * being deserialized, but also instantiation of those types to cover cases when a data type may be
 * able to explicitly say what its Schema is (i.e. in the case of
 * {@link org.apache.avro.generic.GenericContainer GenericContainer}).
 *
 * <p>This "reference data instantiation" indirectly allows this deserializer to also handle
 * generic deserialization types (however inadvisable that may be). Generic data fields are
 * recursively instantiated based on writer Schema-specified type information, and when coupled
 * with reader Schema generation based on that instantiated reference data, continues to allow
 * backward compatible deserialization of those generic data types.
 *
 * <p>The details left up to extensions of this class are how to load/generate Schemas for Types
 * and what DatumReader(s) to use.
 */
public abstract class LoadingAvroDeserializer<T> extends LoadingAvroSerDe implements Deserializer<T> {

    public static final String READ_NULL_ON_FAILURE_PROPERTY = "read.null.on.failure";

    public static final String READER_SCHEMA_LOADING_PROPERTY = "reader.schema.loading";

    public static final String READER_REFERENCE_SCHEMA_GENERATION_PROPERTY = "reader.reference.schema.generation";

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadingAvroDeserializer.class);

    private static final AvroSchemaCache<Integer> READER_SCHEMA_CACHE_BY_WRITER_ID = new AvroSchemaCache<>();

    private boolean readNullOnFailure = false;

    private boolean readerSchemaLoading = true;

    private boolean readerReferenceSchemaGeneration = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configureClientProperties(new KafkaAvroDeserializerConfig(configs));
        this.readNullOnFailure = ConfigLoading.load(configs, READ_NULL_ON_FAILURE_PROPERTY, Boolean::valueOf, readNullOnFailure);
        this.readerSchemaLoading = ConfigLoading.load(configs, READER_SCHEMA_LOADING_PROPERTY, Boolean::valueOf, readerSchemaLoading);
        this.readerReferenceSchemaGeneration = ConfigLoading.load(configs, READER_REFERENCE_SCHEMA_GENERATION_PROPERTY, Boolean::valueOf, readerReferenceSchemaGeneration);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return data == null || data.length == 0 ? null : deserializeNonNull(topic, data);
    }

    @Override
    public void close() {

    }

    protected T deserializeNonNull(String topic, byte[] data) {
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        byte firstByte = dataBuffer.get();
        int writerSchemaId = dataBuffer.getInt();
        try {
            validateByte(MAGIC_BYTE, firstByte);
            return deserializeNonNull(topic, writerSchemaId, dataBuffer);
        } catch (RestClientException e) {
            throw new IllegalStateException("Failed to retrieve Schema for id: " + writerSchemaId, e);
        } catch (Exception e) {
            if (readNullOnFailure) {
                LOGGER.warn("Failed to deserialize Avro message. Returning null", e);
                return null;
            }
            throw new IllegalArgumentException("Failed to deserialize Avro message", e);
        }
    }

    protected T deserializeNonNull(String topic, int writerSchemaId, ByteBuffer dataBuffer) throws IOException, RestClientException {
        Schema writerSchema = getById(writerSchemaId);
        Schema readerSchema = READER_SCHEMA_CACHE_BY_WRITER_ID.load(writerSchemaId, key -> getReaderSchema(topic, writerSchema));
        return deserializeNonNullWithSchemas(writerSchema, readerSchema, dataBuffer);
    }

    protected Schema getReaderSchema(String topic, Schema writerSchema) {
        try {
            return readerSchemaLoading ? loadReaderSchema(writerSchema) : writerSchema;
        } catch (Exception e) {
            LOGGER.error("Failed to load Reader Schema for topic={}. Defaulting to writerSchema={} e={}", topic, writerSchema, e);
            return writerSchema;
        }
    }

    protected Schema loadReaderSchema(Schema writerSchema) throws Exception {
        Object referenceData = AvroDeserialization.instantiateReferenceData(writerSchema);
        return AvroSchemas.getOrSupply(referenceData, () -> readerReferenceSchemaGeneration ?
            AvroDeserialization.generateReaderReferenceSchema(referenceData, writerSchema, this::loadTypeSchema) :
            loadTypeSchema(referenceData.getClass()));
    }

    protected abstract T deserializeNonNullWithSchemas(Schema writerSchema, Schema readerSchema, ByteBuffer dataBuffer) throws IOException;
}
