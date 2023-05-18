package io.atleon.schemaregistry.confluent;

import io.atleon.schema.SchematicDeserializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNull;

public class RegistryDeserializerTest {

    @Test
    public void invalidAvroDataWithoutSchemaIDCanBeNulledOut() {
        RegistryDeserializer<Object, ?> deserializer = new TestRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(RegistryDeserializerConfig.READ_NULL_ON_FAILURE_CONFIG, true);

        deserializer.configure(configs);

        assertNull(deserializer.deserialize(new byte[]{0, 1, 2, 3}));
    }

    @Test
    public void nonAvroDataCanBeDeserializedAsNull() {
        RegistryDeserializer<Object, ?> deserializer = new TestRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(RegistryDeserializerConfig.READ_NULL_ON_FAILURE_CONFIG, true);

        deserializer.configure(configs);

        assertNull(deserializer.deserialize("NON-AVRO".getBytes()));
    }

    private static final class TestRegistryDeserializer<T> extends RegistryDeserializer<T, Object> {

        @Override
        public void configure(Map<String, ?> properties) {
            super.configure(new RegistryDeserializerConfig(RegistryDeserializerConfig.registryDeserializerConfigDef(), properties));
        }

        @Override
        protected SchematicDeserializer<T, Object> deserializer() {
            return (data, __) -> {
                throw new UnsupportedOperationException();
            };
        }

        @Override
        protected SchemaProvider createSchemaProvider() {
            return new SchemaProvider() {
                @Override
                public String schemaType() {
                    return "TEST";
                }

                @Override
                public Optional<ParsedSchema> parseSchema(String schema, List<SchemaReference> references, boolean isNew) {
                    return Optional.empty();
                }
            };
        }

        @Override
        protected Object toSchema(ParsedSchema parsedSchema) {
            throw new UnsupportedOperationException();
        }
    }
}