package io.atleon.kafka;

import io.atleon.core.ConfigFactory;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;

public class KafkaConfigFactory extends ConfigFactory<Map<String, Object>> {

    public KafkaConfigFactory copy() {
        return copyInto(KafkaConfigFactory::new);
    }

    @Override
    public KafkaConfigFactory with(String key, Object value) {
        put(key, value);
        return this;
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