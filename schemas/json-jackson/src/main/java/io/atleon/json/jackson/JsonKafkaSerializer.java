package io.atleon.json.jackson;

import org.apache.kafka.common.serialization.Serializer;

public final class JsonKafkaSerializer<T> implements Serializer<T> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public byte[] serialize(String topic, T data) {
        return data == null ? null : objectMapperFacade.writeAsBytes(data);
    }
}
