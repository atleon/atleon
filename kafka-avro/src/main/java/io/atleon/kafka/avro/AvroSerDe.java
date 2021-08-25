package io.atleon.kafka.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public abstract class AvroSerDe extends AbstractKafkaAvroSerDe {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    protected static void validateByte(byte expectedByte, byte actualByte) {
        if (expectedByte != actualByte) {
            throw new IllegalArgumentException(String.format("Unexpected Byte. expected=%d actual=%d", expectedByte, actualByte));
        }
    }
}
