package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReflectAvroSerDeTest extends AvroSerDeTest {

    public ReflectAvroSerDeTest() {
        super(new ReflectAvroSerializer<>(), new ReflectAvroDeserializer<>().withReaderReferenceSchemaGenerationEnabled(true));
    }

    @Test
    public void recursivelyGenericDataCanBeSerializedAndDeserialized() {
        String data = "DATA";

        TestGenericData<String> genericData = new TestGenericData<>();
        genericData.setData(data);

        TestGenericData<TestGenericData<String>> genericGenericData = new TestGenericData<>();
        genericGenericData.setData(genericData);

        SchemaBytes<Schema> schemaBytes = serializer.serialize(genericGenericData);

        assertTrue(schemaBytes.bytes().length > 0);

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertEquals(genericGenericData, deserialized);
    }

    @Test // Note that this test is mainly pertinent to Avro >= 1.9.0
    public void nullableGenericDataWithAddedFieldCanBeDeserialized() {
        TestData data = TestData.create();

        GenericDataHolderWithAdditionalField<TestData> genericData = new GenericDataHolderWithAdditionalField<>();
        genericData.setData(data);
        genericData.setExtraData("extraData");

        SchemaBytes<Schema> schemaBytes = serializer.serialize(genericData);

        assertTrue(schemaBytes.bytes().length > 0);

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertTrue(deserialized instanceof GenericDataHolder);
        assertEquals(data, GenericDataHolder.class.cast(deserialized).getData());
    }
}
