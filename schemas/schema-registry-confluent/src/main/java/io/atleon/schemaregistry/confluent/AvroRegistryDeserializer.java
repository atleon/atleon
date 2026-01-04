package io.atleon.schemaregistry.confluent;

import io.atleon.avro.AtleonReflectData;
import io.atleon.avro.AvroDeserializer;
import io.atleon.avro.GenericDatas;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

/**
 * A {@link RegistryDeserializer} that uses Avro and delegates to {@link AvroDeserializer}
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
        AvroDeserializer<T> deserializer = AvroDeserializer.create(createGenericData(config));
        deserializer = config.readerSchemaLoading()
                .map(deserializer::withReaderSchemaLoadingEnabled)
                .orElse(deserializer);
        return deserializer.withReaderReferenceSchemaGenerationEnabled(config.readerReferenceSchemaGeneration());
    }

    private GenericData createGenericData(AvroRegistryDeserializerConfig config) {
        GenericData genericData = instantiateGenericData(config);
        if (config.useLogicalTypeConverters()) {
            GenericDatas.addLogicalTypeConversion(genericData);
        }
        return genericData;
    }

    private GenericData instantiateGenericData(AvroRegistryDeserializerConfig config) {
        if (useSchemaReflection) {
            return config.reflectionAllowNull() ? new AtleonReflectData.AllowNull() : new AtleonReflectData();
        } else if (config.specificAvroReader()) {
            return new SpecificData();
        } else {
            return new GenericData();
        }
    }
}
