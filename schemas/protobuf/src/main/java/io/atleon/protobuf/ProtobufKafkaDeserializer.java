package io.atleon.protobuf;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.function.Function;

public final class ProtobufKafkaDeserializer<T extends Message> implements Deserializer<T> {

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String KEY_MESSAGE_TYPE_CONFIG = "protobuf.key.message.type";

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String VALUE_MESSAGE_TYPE_CONFIG = "protobuf.value.message.type";

    private Function<byte[], T> parser;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.parser = loadParser(configs, isKey ? KEY_MESSAGE_TYPE_CONFIG : VALUE_MESSAGE_TYPE_CONFIG);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return data == null ? null : parser.apply(data);
    }

    private static <T extends Message> Function<byte[], T> loadParser(Map<String, ?> configs, String specificKey) {
        return ProtobufMessages.loadParserOrThrow(configs, specificKey, byte[].class);
    }
}
