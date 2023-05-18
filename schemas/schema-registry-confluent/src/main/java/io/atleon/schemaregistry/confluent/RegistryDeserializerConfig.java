package io.atleon.schemaregistry.confluent;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RegistryDeserializerConfig extends RegistrySerDeConfig {

    public static final String READ_NULL_ON_FAILURE_CONFIG = "read.null.on.failure";
    public static final boolean READ_NULL_ON_FAILURE_DEFAULT = false;
    public static final String READ_NULL_ON_FAILURE_DOC =
        "Boolean value that activates behavior of returning <null> when deserialization errors that" +
            " do NOT concern the schema registry are encountered. This can be useful under bad data" +
            " conditions where it is desirable to skip past that bad data in order to continue" +
            " processing.";

    public RegistryDeserializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

    public static ConfigDef registryDeserializerConfigDef() {
        return baseConfigDef()
            .define(READ_NULL_ON_FAILURE_CONFIG, ConfigDef.Type.BOOLEAN, READ_NULL_ON_FAILURE_DEFAULT, ConfigDef.Importance.LOW, READ_NULL_ON_FAILURE_DOC);
    }

    public boolean readNullOnFailure() {
        return getBoolean(READ_NULL_ON_FAILURE_CONFIG);
    }
}
