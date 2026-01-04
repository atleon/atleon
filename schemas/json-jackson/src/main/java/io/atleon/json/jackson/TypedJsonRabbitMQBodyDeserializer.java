package io.atleon.json.jackson;

import io.atleon.rabbitmq.BodyDeserializer;
import io.atleon.rabbitmq.SerializedBody;
import io.atleon.util.ConfigLoading;
import java.util.Map;

public final class TypedJsonRabbitMQBodyDeserializer<T> implements BodyDeserializer<T> {

    /**
     * Qualified class name of the type to deserialize into
     */
    public static final String TYPE_CONFIG = "json.body.type";

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> properties) {
        this.type = (Class<T>) ConfigLoading.loadClassOrThrow(properties, TYPE_CONFIG);
    }

    @Override
    public T deserialize(SerializedBody data) {
        return objectMapperFacade.readAs(data.bytes(), type);
    }
}
