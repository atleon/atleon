package io.atleon.json.jackson;

import io.atleon.aws.sqs.BodyDeserializer;
import io.atleon.util.ConfigLoading;
import java.util.Map;

public final class TypedJsonSqsBodyDeserializer<T> implements BodyDeserializer<T> {

    public static final String TYPE_CONFIG = "json.body.type";

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> properties) {
        this.type = (Class<T>) ConfigLoading.loadClassOrThrow(properties, TYPE_CONFIG);
    }

    @Override
    public T deserialize(String data) {
        return objectMapperFacade.readAs(data, type);
    }
}
