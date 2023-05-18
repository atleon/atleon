package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AvroSerDeTest {

    protected final AvroSerializer<Object> serializer;

    protected final AvroDeserializer<Object> deserializer;

    protected AvroSerDeTest(AvroSerializer<Object> serializer, AvroDeserializer<Object> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Test
    public void dataCanBeSerializedAndDeserialized() {
        TestData data = TestData.create();

        SchemaBytes<Schema> schemaBytes = serializer.serialize(data);

        assertTrue(schemaBytes.bytes().length > 0);

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertEquals(data, deserialized);
    }

    @Test
    public void dataCanBeDeserializedWhenWrittenSchemaAddsField() {
        TestData data = TestData.create();
        TestDataWithAdditionalField dataWithAdditionalField = TestDataWithAdditionalField.fromTestData(data);

        SchemaBytes<Schema> schemaBytes = serializer.serialize(dataWithAdditionalField);

        assertTrue(schemaBytes.bytes().length > 0);

        Schema modifiedWriterSchema = Schema.createRecord(
            TestData.class.getCanonicalName(),
            schemaBytes.schema().getDoc(),
            null,
            schemaBytes.schema().isError(),
            copyFields(schemaBytes.schema().getFields())
        );

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), modifiedWriterSchema);

        assertEquals(data, deserialized);
    }

    @Test
    public void genericDataCanBeSerializedAsNull() {
        TestGenericData<TestData> genericData = new TestGenericData<>();
        genericData.setData(null);

        SchemaBytes<Schema> schemaBytes = serializer.serialize(genericData);

        assertEquals(0, schemaBytes.bytes().length);

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertEquals(genericData, deserialized);
    }

    @Test
    public void genericDataCanBeSerializedAndDeserialized() {
        TestData testData = TestData.create();

        TestGenericData<TestData> genericData = new TestGenericData<>();
        genericData.setData(testData);

        SchemaBytes<Schema> schemaBytes = serializer.serialize(genericData);

        assertTrue(schemaBytes.bytes().length > 0);

        Object deserialized = deserializer.deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertEquals(genericData, deserialized);
    }

    private List<Schema.Field> copyFields(List<Schema.Field> fields) {
        return fields.stream()
            .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
            .collect(Collectors.toList());
    }
}
