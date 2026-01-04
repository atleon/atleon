package io.atleon.schemaregistry.confluent;

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;

public final class AvroRegistryDeserializerConfig extends RegistryDeserializerConfig {

    public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";
    public static final boolean SPECIFIC_AVRO_READER_DEFAULT = false;
    public static final String SPECIFIC_AVRO_READER_DOC = "If true, tries to look up the SpecificRecord class";

    public static final String AVRO_REFLECTION_ALLOW_NULL_CONFIG = "avro.reflection.allow.null";
    public static final boolean AVRO_REFLECTION_ALLOW_NULL_DEFAULT = false;
    public static final String AVRO_REFLECTION_ALLOW_NULL_DOC =
            "If true, allows null field values used in ReflectAvroDeserializer";

    public static final String AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG = "avro.use.logical.type.converters";
    public static final boolean AVRO_USE_LOGICAL_TYPE_CONVERTERS_DEFAULT = false;
    public static final String AVRO_USE_LOGICAL_TYPE_CONVERTERS_DOC =
            "If true, use logical type converter in generic record";

    public static final String READER_SCHEMA_LOADING_CONFIG = "reader.schema.loading";
    public static final Boolean READER_SCHEMA_LOADING_DEFAULT = null;
    public static final String READER_SCHEMA_LOADING_DOC =
            "If true, activates lookup of runtime reader schema based on writer schema type information."
                    + " If null (or not set), schema loading is based on active type of GenericData.";

    public static final String READER_REFERENCE_SCHEMA_GENERATION_CONFIG = "reader.reference.schema.generation";
    public static final boolean READER_REFERENCE_SCHEMA_GENERATION_DEFAULT = false;
    public static final String READER_REFERENCE_SCHEMA_GENERATION_DOC =
            "If true, activates deep generation of objects that may provide schema data for"
                    + " deserialization. Only useful when deserialization types are generic or parameterized.";

    public AvroRegistryDeserializerConfig(Map<?, ?> props) {
        super(avroRegistryDeserializerConfigDef(), props);
    }

    public static ConfigDef avroRegistryDeserializerConfigDef() {
        return registryDeserializerConfigDef()
                .define(
                        SPECIFIC_AVRO_READER_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        SPECIFIC_AVRO_READER_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SPECIFIC_AVRO_READER_DOC)
                .define(
                        AVRO_REFLECTION_ALLOW_NULL_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        AVRO_REFLECTION_ALLOW_NULL_DEFAULT,
                        ConfigDef.Importance.LOW,
                        AVRO_REFLECTION_ALLOW_NULL_DOC)
                .define(
                        AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        AVRO_USE_LOGICAL_TYPE_CONVERTERS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        AVRO_USE_LOGICAL_TYPE_CONVERTERS_DOC)
                .define(
                        READER_SCHEMA_LOADING_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        READER_SCHEMA_LOADING_DEFAULT,
                        ConfigDef.Importance.LOW,
                        READER_SCHEMA_LOADING_DOC)
                .define(
                        READER_REFERENCE_SCHEMA_GENERATION_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        READER_REFERENCE_SCHEMA_GENERATION_DEFAULT,
                        ConfigDef.Importance.LOW,
                        READER_REFERENCE_SCHEMA_GENERATION_DOC);
    }

    public boolean specificAvroReader() {
        return getBoolean(SPECIFIC_AVRO_READER_CONFIG);
    }

    public boolean reflectionAllowNull() {
        return getBoolean(AVRO_REFLECTION_ALLOW_NULL_CONFIG);
    }

    public boolean useLogicalTypeConverters() {
        return getBoolean(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG);
    }

    public Optional<Boolean> readerSchemaLoading() {
        return Optional.ofNullable(getBoolean(READER_SCHEMA_LOADING_CONFIG));
    }

    public boolean readerReferenceSchemaGeneration() {
        return getBoolean(READER_REFERENCE_SCHEMA_GENERATION_CONFIG);
    }
}
