package io.atleon.schemaregistry.confluent;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public final class AvroRegistrySerializerConfig extends RegistrySerializerConfig {

    public static final String SCHEMA_CACHING_ENABLED_CONFIG = "writer.schema.caching";
    public static final boolean SCHEMA_CACHING_ENABLED_DEFAULT = false;
    public static final String SCHEMA_CACHING_ENABLED_DOC =
        "If true, activates caching of writer schemas. Only helpful when schemas are generated" +
            " dynamically at serialization time, and the schemas are stable by serialization " +
            " type (i.e. serialization type is concrete and not generic nor parameterized).";

    public static final String SCHEMA_GENERATION_ENABLED_CONFIG = "writer.schema.generation";
    public static final boolean SCHEMA_GENERATION_ENABLED_DEFAULT = false;
    public static final String SCHEMA_GENERATION_ENABLED_DOC =
        "If true, activates deep generation of writer schemas. Only helpful when schemas are" +
            " generated dynamically at serialization time, and serialization type is generic" +
            " or parameterized.";

    public static final String AVRO_REFLECTION_ALLOW_NULL_CONFIG = "avro.reflection.allow.null";
    public static final boolean AVRO_REFLECTION_ALLOW_NULL_DEFAULT = false;
    public static final String AVRO_REFLECTION_ALLOW_NULL_DOC =
        "If true, allows null field values used in ReflectAvroSerializer";

    public AvroRegistrySerializerConfig(Map<?, ?> props) {
        super(avroRegistrySerializerConfigDef(), props);
    }

    public static ConfigDef avroRegistrySerializerConfigDef() {
        return registrySerializerConfigDef()
            .define(SCHEMA_CACHING_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, SCHEMA_CACHING_ENABLED_DEFAULT, ConfigDef.Importance.MEDIUM, SCHEMA_CACHING_ENABLED_DOC)
            .define(SCHEMA_GENERATION_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, SCHEMA_GENERATION_ENABLED_DEFAULT, ConfigDef.Importance.MEDIUM, SCHEMA_GENERATION_ENABLED_DOC)
            .define(AVRO_REFLECTION_ALLOW_NULL_CONFIG, ConfigDef.Type.BOOLEAN, AVRO_REFLECTION_ALLOW_NULL_DEFAULT, ConfigDef.Importance.LOW, AVRO_REFLECTION_ALLOW_NULL_DOC);
    }

    public boolean schemaCachingEnabled() {
        return getBoolean(SCHEMA_CACHING_ENABLED_CONFIG);
    }

    public boolean schemaGenerationEnabled() {
        return getBoolean(SCHEMA_GENERATION_ENABLED_CONFIG);
    }

    public boolean reflectionAllowNull() {
        return getBoolean(AVRO_REFLECTION_ALLOW_NULL_CONFIG);
    }
}
