package io.atleon.protobuf;

import com.google.protobuf.Message;
import io.atleon.rabbitmq.BodySerializer;
import io.atleon.rabbitmq.SerializedBody;

public final class ProtobufRabbitMQBodySerializer<T extends Message> implements BodySerializer<T> {

    @Override
    public SerializedBody serialize(T data) {
        return SerializedBody.ofBytes(data.toByteArray());
    }
}
