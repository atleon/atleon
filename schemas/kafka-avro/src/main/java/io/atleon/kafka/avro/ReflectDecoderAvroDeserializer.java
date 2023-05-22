package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.AvroRegistryDeserializerConfig;
import io.atleon.schemaregistry.confluent.AvroRegistryKafkaDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @deprecated Use {@link AvroRegistryKafkaDeserializer}
 */
@Deprecated
public class ReflectDecoderAvroDeserializer<T> extends LoadingAvroDeserializer<T> {

    public static final String REFLECT_ALLOW_NULL_PROPERTY = AvroRegistryDeserializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG;

    private final AvroRegistryKafkaDeserializer<T> delegate = new AvroRegistryKafkaDeserializer<T>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> legacyConfigs = new HashMap<>(configs);

        // Default to using reflection
        if (!legacyConfigs.containsKey(AvroRegistryDeserializerConfig.SCHEMA_REFLECTION_CONFIG)) {
            legacyConfigs.put(AvroRegistryDeserializerConfig.SCHEMA_REFLECTION_CONFIG, true);
        }

        // Redirect to renamed config
        if (legacyConfigs.containsKey("reflect.allow.null")) {
            legacyConfigs.put(REFLECT_ALLOW_NULL_PROPERTY, legacyConfigs.get("reflect.allow.null"));
        }

        delegate.configure(legacyConfigs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }
}
