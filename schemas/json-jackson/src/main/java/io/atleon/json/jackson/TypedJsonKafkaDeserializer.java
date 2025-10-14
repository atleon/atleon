package io.atleon.json.jackson;

import io.atleon.util.ConfigLoading;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public final class TypedJsonKafkaDeserializer<T> implements Deserializer<T> {

    /**
     * Qualified class name of the type to deserialize into
     */
    public static final String KEY_TYPE_CONFIG = "json.key.type";

    /**
     * Qualified class name of the type to deserialize into
     */
    public static final String VALUE_TYPE_CONFIG = "json.value.type";

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.type = (Class<T>) ConfigLoading.loadClassOrThrow(configs, isKey ? KEY_TYPE_CONFIG : VALUE_TYPE_CONFIG);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return data == null ? null : objectMapperFacade.readAs(data, type);
    }
}
