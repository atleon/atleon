package io.atleon.json.jackson;

import io.atleon.aws.sns.BodySerializer;

public final class JsonSnsBodySerializer<T> implements BodySerializer<T> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public String serialize(T data) {
        return objectMapperFacade.writeAsString(data);
    }
}
