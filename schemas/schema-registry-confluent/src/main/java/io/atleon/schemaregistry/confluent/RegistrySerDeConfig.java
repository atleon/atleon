package io.atleon.schemaregistry.confluent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RegistrySerDeConfig extends AbstractKafkaSchemaSerDeConfig {

    public RegistrySerDeConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }
}
