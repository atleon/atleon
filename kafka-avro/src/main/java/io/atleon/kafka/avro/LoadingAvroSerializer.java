package io.atleon.kafka.avro;

import io.atleon.util.ConfigLoading;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This Serializer implements serialization-time Avro Schema loading such that Schemas for written
 * data types do not need to be generated/provided prior to runtime. In addition to supporting
 * plain data types and Avro-native data types, this Serializer also supports usage with generic
 * data types via Schema generation based on values populated at serialization time.
 *
 * <p>It should be noted that usage of Schema generation needs to be combined with some form of
 * Schema caching as the native Schema Registry Client puts a cap on the number of generated
 * Schemas that can be registered (by Object identity). Usage with generic data types can
 * complicate this restriction if it is possible for serialized generic types to change during the
 * course of an application's lifetime. In all other cases when types are otherwise stable on a
 * per-subject (note: OLD subject) basis, this class provides a cache that can be enabled to
 * satisfy the max-Schema-registration cap.
 */
public abstract class LoadingAvroSerializer<T> extends LoadingAvroSerDe implements Serializer<T> {

    public static final String WRITER_SCHEMA_CACHING_PROPERTY = "writer.schema.caching";

    public static final String WRITER_SCHEMA_GENERATION_PROPERTY = "writer.schema.generation";

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadingAvroSerializer.class);

    private static final Map<String, AvroSchemaCache<Class>> WRITER_SCHEMA_CACHES_BY_OLD_SUBJECT = new ConcurrentHashMap<>();

    private boolean writerSchemaCaching = false;

    private boolean writerSchemaGeneration = false;

    private boolean isKey = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configureClientProperties(new KafkaAvroSerializerConfig(configs));
        this.writerSchemaCaching = ConfigLoading.load(configs, WRITER_SCHEMA_CACHING_PROPERTY, Boolean::valueOf, writerSchemaCaching);
        this.writerSchemaGeneration = ConfigLoading.load(configs, WRITER_SCHEMA_GENERATION_PROPERTY, Boolean::valueOf, writerSchemaGeneration);
        this.isKey = isKey;
    }

    // Based on Confluent's KafkaAvroSerializer http://bit.ly/2pfy381
    @Override
    public byte[] serialize(String topic, T data) {
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema handling and return a null value in Kafka, instead
        // of an Avro encoded null.
        try {
            return data == null ? null : serializeNonNull(topic, data);
        } catch (RestClientException e) {
            LOGGER.warn("Error registering Avro Schema", e);
            throw new SerializationException("Error registering Avro Schema", e);
        } catch (IOException | RuntimeException e) {
            // Avro can throw AvroRuntimeException, NullPointerException, ClassCastException, etc
            LOGGER.warn("Error serializing Avro message", e);
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close() {

    }

    public byte[] serializeNonNull(String topic, T data) throws RestClientException, IOException {
        Schema schema = writerSchemaCaching ? getWriterSchemaCache(topic).load(data.getClass(), dataClass -> loadWriterSchema(data)) : loadWriterSchema(data);
        String subject = getSubjectName(topic, isKey, data, schema);
        int schemaId = register(subject, schema);
        return serializeNonNullWithSchema(schemaId, schema, data);
    }

    protected Schema loadWriterSchema(Object data) {
        return AvroSchemas.getOrSupply(data, () -> writerSchemaGeneration ?
            AvroSerialization.generateWriterSchema(data, this::loadTypeSchema) : loadTypeSchema(data.getClass()));
    }

    protected final byte[] serializeNonNullWithSchema(int schemaId, Schema schema, T data) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(AbstractKafkaAvroSerDe.MAGIC_BYTE);
        output.write(ByteBuffer.allocate(AbstractKafkaAvroSerDe.idSize).putInt(schemaId).array());
        serializeDataToOutput(output, schema, data);
        return output.toByteArray();
    }

    protected abstract void serializeDataToOutput(ByteArrayOutputStream output, Schema schema, T data) throws IOException;

    private AvroSchemaCache<Class> getWriterSchemaCache(String topic) {
        String oldSubject = topic + (isKey ? "-key" : "-value");
        return WRITER_SCHEMA_CACHES_BY_OLD_SUBJECT.computeIfAbsent(oldSubject, key -> new AvroSchemaCache<>());
    }
}
