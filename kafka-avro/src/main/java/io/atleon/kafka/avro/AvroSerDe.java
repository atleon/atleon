package io.atleon.kafka.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public abstract class AvroSerDe extends AbstractKafkaAvroSerDe {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    public static final String BASIC_AUTH_CREDENTIALS_SOURCE = AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;

    public static final String USER_INFO_CONFIG = AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG;

    public static final String KEY_SUBJECT_NAME_STRATEGY = AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;

    public static final String VALUE_SUBJECT_NAME_STRATEGY = AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;

    protected static void validateByte(byte expectedByte, byte actualByte) {
        if (expectedByte != actualByte) {
            throw new IllegalArgumentException(String.format("Unexpected Byte. expected=%d actual=%d", expectedByte, actualByte));
        }
    }
}
