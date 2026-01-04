package io.atleon.protobuf;

import com.google.protobuf.Message;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProtobufKafkaDeserializer<T extends Message> implements Deserializer<T> {

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String KEY_MESSAGE_TYPE_CONFIG = "protobuf.key.message.type";

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String VALUE_MESSAGE_TYPE_CONFIG = "protobuf.value.message.type";

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     *
     * @deprecated Use {@value #KEY_MESSAGE_TYPE_CONFIG} or {@value #VALUE_MESSAGE_TYPE_CONFIG}
     */
    @Deprecated
    public static final String MESSAGE_TYPE_CONFIG = "protobuf.message.type";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufKafkaDeserializer.class);

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
        Optional<Function<byte[], T>> parser;
        if ((parser = ProtobufMessages.loadParser(configs, specificKey, byte[].class)).isPresent()) {
            return parser.get();
        } else if ((parser = ProtobufMessages.loadParser(configs, MESSAGE_TYPE_CONFIG, byte[].class)).isPresent()) {
            LOGGER.warn("Deprecated config '{}'. Please configure '{}'.", MESSAGE_TYPE_CONFIG, specificKey);
            return parser.get();
        } else {
            throw new IllegalArgumentException("Missing config: " + specificKey);
        }
    }
}
