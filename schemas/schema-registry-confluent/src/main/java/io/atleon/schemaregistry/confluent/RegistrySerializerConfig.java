package io.atleon.schemaregistry.confluent;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class RegistrySerializerConfig extends RegistrySerDeConfig {

    public RegistrySerializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

    public static ConfigDef registrySerializerConfigDef() {
        return baseConfigDef();
    }
}
