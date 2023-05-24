package io.atleon.protobuf;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.function.Function;

public final class ProtobufKafkaDeserializer<T extends Message> implements Deserializer<T> {

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String MESSAGE_TYPE_CONFIG = "protobuf.message.type";

    private Function<byte[], T> parser;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.parser = ProtobufMessages.loadParser(configs, MESSAGE_TYPE_CONFIG, byte[].class);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return parser.apply(data);
    }
}
