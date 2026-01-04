package io.atleon.schemaregistry.confluent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class RegistrySerDeConfig extends AbstractKafkaSchemaSerDeConfig {

    public RegistrySerDeConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }
}
