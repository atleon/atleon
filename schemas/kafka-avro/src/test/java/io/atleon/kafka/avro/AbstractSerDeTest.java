package io.atleon.kafka.avro;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractSerDeTest {

    protected static final String TOPIC = "topic";

    protected final Serializer serializer;

    protected final Deserializer deserializer;

    protected AbstractSerDeTest(Serializer serializer, Deserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Test
    public void dataCanBeSerializedAndDeserialized() {
        TestData data = TestData.create();

        byte[] serializedData = serializer.serialize(TOPIC, data);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestData deserialized = (TestData) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(data, deserialized);
    }
}
