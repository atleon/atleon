package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.AvroRegistryKafkaSerializer;
import io.atleon.schemaregistry.confluent.AvroRegistrySerializerConfig;

import java.util.Map;

/**
 * @deprecated Use {@link AvroRegistryKafkaSerializer}
 */
@Deprecated
public class ReflectEncoderAvroSerializer<T> extends LoadingAvroSerializer<T> {

    public static final String REFLECT_ALLOW_NULL_PROPERTY = AvroRegistrySerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG;

    private final AvroRegistryKafkaSerializer<T> serializer = new AvroRegistryKafkaSerializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return serializer.serialize(topic, data);
    }
}
