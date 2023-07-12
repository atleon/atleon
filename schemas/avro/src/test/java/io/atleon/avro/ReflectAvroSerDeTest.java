package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReflectAvroSerDeTest extends AvroSerDeTest {

    public ReflectAvroSerDeTest() {
        super(AvroSerializer.reflect(), AvroDeserializer.reflect().withReaderReferenceSchemaGenerationEnabled(true));
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

    @Test
    public void deserializingFieldsWithUnknownTypesResultsInNullValues() {
        TestGenericData<TestData> data = new TestGenericData<>();
        data.setData(TestData.create());

        SchemaBytes<Schema> schemaBytes = serializer.serialize(data);

        Schema schema = schemaBytes.schema();
        List<Schema.Field> modifiedFields = schema.getFields().stream()
            .map(field -> {
                if (field.name().equals("data")) {
                    Schema nonNullSchema = AvroSchemas.reduceNonNull(field.schema()).orElseThrow(IllegalStateException::new);
                    List<Schema.Field> copiedFields = nonNullSchema.getFields().stream()
                        .map(it -> new Schema.Field(it.name(), it.schema(), it.doc(), it.defaultVal()))
                        .collect(Collectors.toList());
                    Schema dataSchema = Schema.createRecord("TestDataUndefined", nonNullSchema.getDoc(), nonNullSchema.getNamespace(), nonNullSchema.isError(), copiedFields);
                    return new Schema.Field(field.name(), AvroSchemas.makeNullable(dataSchema), field.doc(), field.defaultVal());
                } else {
                    return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
                }
            })
            .collect(Collectors.toList());

        Object deserialized = deserializer.deserialize(
            schemaBytes.bytes(),
            Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), modifiedFields)
        );

        assertTrue(deserialized instanceof TestGenericData);
        assertNull(TestGenericData.class.cast(deserialized).getData());
    }
}
