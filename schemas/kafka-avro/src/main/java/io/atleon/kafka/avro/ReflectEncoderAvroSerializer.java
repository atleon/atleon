package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.AvroRegistryKafkaSerializer;
import io.atleon.schemaregistry.confluent.AvroRegistrySerializerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @deprecated Use {@link AvroRegistryKafkaSerializer}
 */
@Deprecated
public class ReflectEncoderAvroSerializer<T> extends LoadingAvroSerializer<T> {

    public static final String REFLECT_ALLOW_NULL_PROPERTY = AvroRegistrySerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG;

    private final AvroRegistryKafkaSerializer<T> delegate = new AvroRegistryKafkaSerializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> legacyConfigs = new HashMap<>(configs);

        // Default to using reflection
        if (!legacyConfigs.containsKey(AvroRegistrySerializerConfig.SCHEMA_REFLECTION_CONFIG)) {
            legacyConfigs.put(AvroRegistrySerializerConfig.SCHEMA_REFLECTION_CONFIG, true);
        }

        // Redirect to renamed config
        if (legacyConfigs.containsKey("reflect.allow.null")) {
            legacyConfigs.put(REFLECT_ALLOW_NULL_PROPERTY, legacyConfigs.get("reflect.allow.null"));
        }

        delegate.configure(legacyConfigs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return delegate.serialize(topic, data);
    }
}
