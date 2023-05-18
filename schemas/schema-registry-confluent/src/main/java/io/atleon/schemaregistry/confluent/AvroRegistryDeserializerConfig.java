package io.atleon.schemaregistry.confluent;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public final class AvroRegistryDeserializerConfig extends RegistryDeserializerConfig {

    public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";
    public static final boolean SPECIFIC_AVRO_READER_DEFAULT = false;
    public static final String SPECIFIC_AVRO_READER_DOC =
        "If true, tries to look up the SpecificRecord class";

    public static final String READER_SCHEMA_LOADING_CONFIG = "reader.schema.loading";
    public static final boolean READER_SCHEMA_LOADING_DEFAULT = true;
    public static final String READER_SCHEMA_LOADING_DOC =
        "If true, activates lookup of runtime reader schema based on writer schema type information";

    public static final String READER_REFERENCE_SCHEMA_GENERATION_CONFIG = "reader.reference.schema.generation";
    public static final boolean READER_REFERENCE_SCHEMA_GENERATION_DEFAULT = false;
    public static final String READER_REFERENCE_SCHEMA_GENERATION_DOC =
        "If true, activates deep generation of objects that may provide schema data for" +
            " deserialization. Only useful when deserialization types are generic or parameterized.";

    public static final String AVRO_REFLECTION_ALLOW_NULL_CONFIG = "avro.reflection.allow.null";
    public static final boolean AVRO_REFLECTION_ALLOW_NULL_DEFAULT = false;
    public static final String AVRO_REFLECTION_ALLOW_NULL_DOC =
        "If true, allows null field values used in ReflectAvroDeserializer";

    public AvroRegistryDeserializerConfig(Map<?, ?> props) {
        super(avroRegistryDeserializerConfigDef(), props);
    }

    public static ConfigDef avroRegistryDeserializerConfigDef() {
        return registryDeserializerConfigDef()
            .define(SPECIFIC_AVRO_READER_CONFIG, ConfigDef.Type.BOOLEAN, SPECIFIC_AVRO_READER_DEFAULT, ConfigDef.Importance.LOW, SPECIFIC_AVRO_READER_DOC)
            .define(READER_SCHEMA_LOADING_CONFIG, ConfigDef.Type.BOOLEAN, READER_SCHEMA_LOADING_DEFAULT, ConfigDef.Importance.LOW, READER_SCHEMA_LOADING_DOC)
            .define(READER_REFERENCE_SCHEMA_GENERATION_CONFIG, ConfigDef.Type.BOOLEAN, READER_REFERENCE_SCHEMA_GENERATION_DEFAULT, ConfigDef.Importance.LOW, READER_REFERENCE_SCHEMA_GENERATION_DOC)
            .define(AVRO_REFLECTION_ALLOW_NULL_CONFIG, ConfigDef.Type.BOOLEAN, AVRO_REFLECTION_ALLOW_NULL_DEFAULT, ConfigDef.Importance.LOW, AVRO_REFLECTION_ALLOW_NULL_DOC);
    }

    public boolean specificAvroReader() {
        return getBoolean(SPECIFIC_AVRO_READER_CONFIG);
    }

    public boolean readerSchemaLoading() {
        return getBoolean(READER_SCHEMA_LOADING_CONFIG);
    }

    public boolean readerReferenceSchemaGeneration() {
        return getBoolean(READER_REFERENCE_SCHEMA_GENERATION_CONFIG);
    }

    public boolean reflectionAllowNull() {
        return getBoolean(AVRO_REFLECTION_ALLOW_NULL_CONFIG);
    }
}
