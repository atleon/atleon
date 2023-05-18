package io.atleon.schemaregistry.confluent;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RegistrySerializerConfig extends RegistrySerDeConfig {

    public RegistrySerializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

    public static ConfigDef registrySerializerConfigDef() {
        return baseConfigDef();
    }
}
