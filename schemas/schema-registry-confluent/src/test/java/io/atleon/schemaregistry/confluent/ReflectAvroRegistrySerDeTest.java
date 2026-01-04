package io.atleon.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ReflectAvroRegistrySerDeTest {

    private static final String SCHEMA_REGISTRY_URL = "mock://test";

    @Test
    public void dataCanBeSerializedAndDeserialized() {
        RegistrySerializer<Object, ?> serializer = new AvroRegistrySerializer<>();
        RegistryDeserializer<Object, ?> deserializer = new AvroRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        configs.put(RegistrySerDeConfig.SCHEMA_REFLECTION_CONFIG, true);

        serializer.configure(configs);
        deserializer.configure(configs);

        TestData data = TestData.create();

        byte[] serializedData = serializer.serialize(data);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        Object deserialized = deserializer.deserialize(serializedData);

        assertEquals(data, deserialized);
    }
}
