package io.atleon.kafka.avro;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReflectDecoderAvroSerDeTest extends LoadingAvroSerDeTest {

    public ReflectDecoderAvroSerDeTest() {
        super(TestReflectEncoderAvroSerializer::new, TestReflectDecoderAvroDeserializer::new);
    }

    @Test
    public void recursivelyGenericDataCanBeSerializedAndDeserialized() {
        String data = "DATA";

        TestGenericData<String> genericData = new TestGenericData<>();
        genericData.setData(data);

        TestGenericData<TestGenericData<String>> genericGenericData = new TestGenericData<>();
        genericGenericData.setData(genericData);

        byte[] serializedData = serializer.serialize(AbstractSerDeTest.TOPIC, genericGenericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestGenericData deserialized = (TestGenericData) deserializer.deserialize(AbstractSerDeTest.TOPIC, serializedData);

        assertEquals(genericGenericData, deserialized);
    }

    @Test // Note that this test is mainly pertinent to Avro >= 1.9.0
    public void nullableGenericDataWithAddedFieldCanBeDeserialized() {
        TestData data = TestData.create();

        GenericDataHolderWithAdditionalField<TestData> genericData = new GenericDataHolderWithAdditionalField<>();
        genericData.setData(data);
        genericData.setExtraData("extraData");

        byte[] serializedData = serializer.serialize(TOPIC, genericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        GenericDataHolder deserialized = (GenericDataHolder) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(data, deserialized.getData());
    }
}
