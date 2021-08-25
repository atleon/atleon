package io.atleon.kafka.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class LoadingAvroSerDeTest extends AbstractSerDeTest {

    protected final TestSchemaRegistry registry;

    protected LoadingAvroSerDeTest(
        Function<TestSchemaRegistry, ? extends LoadingAvroSerializer> serializerCreator,
        Function<TestSchemaRegistry, ? extends LoadingAvroDeserializer> deserializerCreator) {
        this(new TestSchemaRegistry(), serializerCreator, deserializerCreator);
    }

    protected LoadingAvroSerDeTest(
        TestSchemaRegistry registry,
        Function<TestSchemaRegistry, ? extends LoadingAvroSerializer> serializerCreator,
        Function<TestSchemaRegistry, ? extends LoadingAvroDeserializer> deserializerCreator) {
        super(serializerCreator.apply(registry), deserializerCreator.apply(registry));
        this.registry = registry;
    }

    @BeforeEach
    public void setup() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AvroSerDe.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(LoadingAvroDeserializer.READER_REFERENCE_SCHEMA_GENERATION_PROPERTY, true);

        serializer.configure(configs, false);
        deserializer.configure(configs, false);
    }

    @Test
    public void nonAvroDataCanBeDeserializedAsNull() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AvroSerDe.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(LoadingAvroDeserializer.READ_NULL_ON_FAILURE_PROPERTY, true);

        deserializer.configure(configs, false);

        assertNull(deserializer.deserialize(TOPIC, "NON-AVRO".getBytes()));
    }

    @Test
    public void dataCanBeDeserializedWhenWrittenSchemaAddsField() {
        TestData data = TestData.create();
        TestDataWithAdditionalField dataWithAdditionalField = TestDataWithAdditionalField.fromTestData(data);

        registry.setRegistrationHook(schema ->
            Schema.createRecord(TestData.class.getCanonicalName(), schema.getDoc(), null, schema.isError(), copyFields(schema.getFields())));

        byte[] serializedData = serializer.serialize(TOPIC, dataWithAdditionalField);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestData deserialized = (TestData) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(data, deserialized);
    }

    @Test
    public void genericDataCanBeSerializedAsNull() {
        TestGenericData<TestData> genericData = new TestGenericData<>();
        genericData.setData(null);

        byte[] serializedData = serializer.serialize(TOPIC, genericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestGenericData deserialized = (TestGenericData) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(genericData, deserialized);
    }

    @Test
    public void genericDataCanBeSerializedAndDeserialized() {
        TestData testData = TestData.create();

        TestGenericData<TestData> genericData = new TestGenericData<>();
        genericData.setData(testData);

        byte[] serializedData = serializer.serialize(TOPIC, genericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestGenericData deserialized = (TestGenericData) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(genericData, deserialized);
    }

    //TODO This test exposes a backward incompatible change when upgrading to Avro >=1.9.0. In
    // the case of Jackson, Avro has switched to using the FasterXML variant of Jackson which
    // results in NoSuchMethodErrors when looking for the CodeHaus method variants targeted by
    // jackson-dataformat-avro. That dependency needs to be patched to use the newer version of
    // Avro, at which point we can upgrade and use Avro >=1.9.0
    @Test
    public void dataWithProblematicTypesCanBeSerializedAndDeserialized() {
        Set<String> dataSet = new HashSet<>();
        dataSet.add("HELLO");
        dataSet.add("AMIGO");

        SortedSet<String> sortedDataSet = new TreeSet<>();
        sortedDataSet.add("HOLA");
        sortedDataSet.add("FRIEND");

        TestDataWithProblematicTypes testDataWithProblematicTypes = new TestDataWithProblematicTypes();
        testDataWithProblematicTypes.setDataSet(dataSet);
        testDataWithProblematicTypes.setSortedDataSet(sortedDataSet);

        byte[] serializedData = serializer.serialize(TOPIC, testDataWithProblematicTypes);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestDataWithProblematicTypes deserialized = (TestDataWithProblematicTypes) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(testDataWithProblematicTypes, deserialized);
    }

    private List<Schema.Field> copyFields(List<Schema.Field> fields) {
        return fields.stream()
            .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
            .collect(Collectors.toList());
    }
}
