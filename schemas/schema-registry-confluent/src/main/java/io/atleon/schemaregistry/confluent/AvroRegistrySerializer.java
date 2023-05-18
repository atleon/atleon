package io.atleon.schemaregistry.confluent;

import io.atleon.avro.AvroSerializer;
import io.atleon.avro.GenericAvroSerializer;
import io.atleon.avro.ReflectAvroSerializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * A {@link RegistrySerializer} that uses Avro and delegates to {@link ReflectAvroSerializer}
 *
 * @param <T> The type of data serialized by this serializer
 */
public final class AvroRegistrySerializer<T> extends RegistrySerializer<T, Schema> {

    private AvroSerializer<T> serializer;

    @Override
    public void configure(Map<String, ?> properties) {
        configure(new AvroRegistrySerializerConfig(properties));
    }

    public void configure(AvroRegistrySerializerConfig config) {
        super.configure(config);
        this.serializer = createSerializer(config);
    }

    @Override
    protected SchemaProvider createSchemaProvider() {
        return new AvroSchemaProvider();
    }

    @Override
    protected AvroSerializer<T> serializer() {
        return serializer;
    }

    @Override
    protected ParsedSchema toParsedSchema(Schema schema) {
        return new AvroSchema(schema);
    }

    private AvroSerializer<T> createSerializer(AvroRegistrySerializerConfig config) {
        if (useSchemaReflection) {
            return new ReflectAvroSerializer<T>()
                .withSchemaCachingEnabled(config.schemaCachingEnabled())
                .withSchemaGenerationEnabled(config.schemaGenerationEnabled())
                .withAllowNull(config.reflectionAllowNull());
        } else {
            return new GenericAvroSerializer<>();
        }
    }
}
