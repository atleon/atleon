package io.atleon.json.jackson;

import io.atleon.rabbitmq.BodySerializer;
import io.atleon.rabbitmq.SerializedBody;

public final class JsonRabbitMQBodySerializer<T> implements BodySerializer<T> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public SerializedBody serialize(T data) {
        return SerializedBody.ofBytes(objectMapperFacade.writeAsBytes(data));
    }
}
