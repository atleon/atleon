package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.AvroRegistrySerializerConfig;
import org.apache.kafka.common.serialization.Serializer;

@Deprecated
public abstract class LoadingAvroSerializer<T> extends LoadingAvroSerDe implements Serializer<T> {

    public static final String WRITER_SCHEMA_CACHING_PROPERTY = AvroRegistrySerializerConfig.SCHEMA_CACHING_ENABLED_CONFIG;

    public static final String WRITER_SCHEMA_GENERATION_PROPERTY = AvroRegistrySerializerConfig.SCHEMA_GENERATION_ENABLED_CONFIG;
}
