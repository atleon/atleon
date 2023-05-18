package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.AvroRegistryDeserializerConfig;
import io.atleon.schemaregistry.confluent.RegistryDeserializerConfig;
import org.apache.kafka.common.serialization.Deserializer;


@Deprecated
public abstract class LoadingAvroDeserializer<T> extends LoadingAvroSerDe implements Deserializer<T> {

    public static final String READ_NULL_ON_FAILURE_PROPERTY = RegistryDeserializerConfig.READ_NULL_ON_FAILURE_CONFIG;

    public static final String READER_SCHEMA_LOADING_PROPERTY = AvroRegistryDeserializerConfig.READER_SCHEMA_LOADING_CONFIG;

    public static final String READER_REFERENCE_SCHEMA_GENERATION_PROPERTY = AvroRegistryDeserializerConfig.READER_REFERENCE_SCHEMA_GENERATION_CONFIG;
}
