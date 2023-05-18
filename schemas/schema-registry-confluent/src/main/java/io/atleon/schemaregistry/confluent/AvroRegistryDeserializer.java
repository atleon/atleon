package io.atleon.schemaregistry.confluent;

import io.atleon.avro.AvroDeserializer;
import io.atleon.avro.GenericAvroDeserializer;
import io.atleon.avro.ReflectAvroDeserializer;
import io.atleon.avro.SpecificAvroDeserializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * A {@link RegistryDeserializer} that uses Avro and delegates to {@link ReflectAvroDeserializer}
 *
 * @param <T> The type of data deserialized by this deserializer
 */
public final class AvroRegistryDeserializer<T> extends RegistryDeserializer<T, Schema> {

    private AvroDeserializer<T> deserializer;

    @Override
    public void configure(Map<String, ?> properties) {
        configure(new AvroRegistryDeserializerConfig(properties));
    }

    public void configure(AvroRegistryDeserializerConfig config) {
        super.configure(config);
        this.deserializer = createDeserializer(config);
    }

    @Override
    protected SchemaProvider createSchemaProvider() {
        return new AvroSchemaProvider();
    }

    @Override
    protected AvroDeserializer<T> deserializer() {
        return deserializer;
    }

    @Override
    protected Schema toSchema(ParsedSchema parsedSchema) {
        return ParsedSchemas.extractTypedRawSchema(parsedSchema, Schema.class);
    }

    private AvroDeserializer<T> createDeserializer(AvroRegistryDeserializerConfig config) {
        if (useSchemaReflection) {
            return new ReflectAvroDeserializer<T>()
                .withReaderSchemaLoadingEnabled(config.readerSchemaLoading())
                .withReaderReferenceSchemaGenerationEnabled(config.readerReferenceSchemaGeneration())
                .withAllowNull(config.reflectionAllowNull());
        } else if (config.specificAvroReader()) {
            return new SpecificAvroDeserializer<T>()
                .withReaderSchemaLoadingEnabled(config.readerSchemaLoading());
        } else {
            return new GenericAvroDeserializer<>();
        }
    }
}
