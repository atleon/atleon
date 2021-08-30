package io.atleon.kafka;

import io.atleon.core.ConfigSource;
import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class KafkaConfigSource extends ConfigSource<Map<String, Object>, KafkaConfigSource> {

    protected KafkaConfigSource() {

    }

    protected KafkaConfigSource(String name) {
        super(name);
    }

    protected KafkaConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    public static KafkaConfigSource named(String name) {
        return new KafkaConfigSource(name);
    }

    public static KafkaConfigSource useClientIdAsName() {
        return new KafkaConfigSource(properties ->
            ConfigLoading.load(properties, CommonClientConfigs.CLIENT_ID_CONFIG, Object::toString));
    }

    @Override
    protected KafkaConfigSource initializeSourceCopy() {
        return new KafkaConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateNonNullProperty(properties, CommonClientConfigs.CLIENT_ID_CONFIG);
        validateNonNullProperty(properties, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
        return properties;
    }
}