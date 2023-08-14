package io.atleon.kafka;

import io.atleon.core.ConfigSource;
import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class KafkaConfigSource extends ConfigSource<KafkaConfig, KafkaConfigSource> {

    protected KafkaConfigSource() {

    }

    protected KafkaConfigSource(String name) {
        super(name);
    }

    protected KafkaConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    public static KafkaConfigSource unnamed() {
        return new KafkaConfigSource();
    }

    public static KafkaConfigSource named(String name) {
        return new KafkaConfigSource(name);
    }

    public static KafkaConfigSource useClientIdAsName() {
        return new KafkaConfigSource(map -> ConfigLoading.loadString(map, CommonClientConfigs.CLIENT_ID_CONFIG));
    }

    public KafkaConfigSource withClientId(String clientId) {
        return with(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
    }

    public KafkaConfigSource withBootstrapServers(String bootstrapServers) {
        return with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public KafkaConfigSource withProducerOrderingAndResiliencyConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        return withAll(configs);
    }

    public KafkaConfigSource withConsumerGroupId(String groupId) {
        return with(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public KafkaConfigSource withKeySerializer(Class<? extends Serializer<?>> serializerClass) {
        return with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
    }

    public KafkaConfigSource withValueSerializer(Class<? extends Serializer<?>> serializerClass) {
        return with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
    }

    public KafkaConfigSource withKeyDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
        return with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
    }

    public KafkaConfigSource withValueDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
        return with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
    }

    @Override
    protected KafkaConfigSource initializeCopy() {
        return new KafkaConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateNonNullProperty(properties, CommonClientConfigs.CLIENT_ID_CONFIG);
        validateNonNullProperty(properties, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    protected KafkaConfig postProcessProperties(Map<String, Object> properties) {
        return KafkaConfig.create(properties);
    }
}