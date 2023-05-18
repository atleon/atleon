package io.atleon.schemaregistry.confluent;

import io.atleon.schemaregistry.confluent.embedded.EmbeddedSchemaRegistry;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EmbeddedReflectAvroRegistrySerDeTest {

    private static final URL SCHEMA_REGISTRY_URL = EmbeddedSchemaRegistry.startAndGetConnectUrl();

    @Test
    public void dataCanBeSerializedAndDeserialized() {
        RegistrySerializer<Object, ?> serializer = new AvroRegistrySerializer<>();
        RegistryDeserializer<Object, ?> deserializer = new AvroRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<String, Object>() {{
            put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL.toString());
            put(RegistrySerDeConfig.SCHEMA_REFLECTION_CONFIG, true);
        }};

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
