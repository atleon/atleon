package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroUpgradeTest {

    @Test // Upgrading from 1.8.2
    public void nullableMapWithBackwardCompatibleValueChangesCanBeDeserialized() {
        TestDataWithNullableMap data = new TestDataWithNullableMap();
        data.setMap(Collections.singletonMap("KEY", TestData.create()));

        SchemaBytes<Schema> schemaBytes = AvroSerializer.reflect().serialize(data);

        Schema readerMapSchema = Schema.createMap(
            Schema.createRecord(
                TestData.class.getName(),
                null,
                null,
                false,
                Collections.singletonList(
                    new Schema.Field("data1", Schema.create(Schema.Type.STRING), null, Object.class.cast(null))
                )
            )
        );
        Schema readerSchema = Schema.createRecord(
            TestDataWithNullableMap.class.getName(),
            null,
            null,
            false,
            Collections.singletonList(
                new Schema.Field("map", ReflectData.makeNullable(readerMapSchema), null, Object.class.cast(null))
            )
        );

        Object deserialized = new AvroDeserializer<>(ReflectData.get(), __ -> readerSchema)
            .deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertTrue(deserialized instanceof TestDataWithNullableMap);
        assertEquals(
            data.getMap().get("KEY").getData1(),
            TestDataWithNullableMap.class.cast(deserialized).getMap().get("KEY").getData1()
        );
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

        SchemaBytes<Schema> schemaBytes = AvroSerializer.reflect().serialize(testDataWithProblematicTypes);

        assertTrue(schemaBytes.bytes().length > 0);

        Object deserialized = AvroDeserializer.reflect().deserialize(schemaBytes.bytes(), schemaBytes.schema());

        assertEquals(testDataWithProblematicTypes, deserialized);
    }
}
