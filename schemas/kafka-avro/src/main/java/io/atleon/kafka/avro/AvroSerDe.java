package io.atleon.kafka.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public abstract class AvroSerDe extends AbstractKafkaSchemaSerDe {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    public static final String BASIC_AUTH_CREDENTIALS_SOURCE = AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;

    public static final String USER_INFO_CONFIG = AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;

    public static final String KEY_SUBJECT_NAME_STRATEGY = AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;

    public static final String VALUE_SUBJECT_NAME_STRATEGY = AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;

    protected static void validateByte(byte expectedByte, byte actualByte) {
        if (expectedByte != actualByte) {
            throw new IllegalArgumentException(String.format("Unexpected Byte. expected=%d actual=%d", expectedByte, actualByte));
        }
    }
}
