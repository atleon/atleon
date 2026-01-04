package io.atleon.protobuf;

import com.google.protobuf.Message;
import io.atleon.rabbitmq.BodyDeserializer;
import io.atleon.rabbitmq.SerializedBody;
import java.util.Map;
import java.util.function.Function;

public final class ProtobufRabbitMQBodyDeserializer<T extends Message> implements BodyDeserializer<T> {

    /**
     * Qualified class name of the type of {@link Message} to deserialize into
     */
    public static final String MESSAGE_TYPE_CONFIG = "protobuf.message.type";

    private Function<byte[], T> parser;

    @Override
    public void configure(Map<String, ?> properties) {
        this.parser = ProtobufMessages.loadParserOrThrow(properties, MESSAGE_TYPE_CONFIG, byte[].class);
    }

    @Override
    public T deserialize(SerializedBody data) {
        return parser.apply(data.bytes());
    }
}
