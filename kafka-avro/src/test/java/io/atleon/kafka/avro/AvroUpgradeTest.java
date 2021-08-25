package io.atleon.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroUpgradeTest {

    @Test // Upgrading from 1.8.2
    public void nullableMapWithBackwardCompatibleValueChangesCanBeDeserialized() throws Exception {
        TestDataWithNullableMap data = new TestDataWithNullableMap();
        data.setMap(Collections.singletonMap("KEY", TestData.create()));

        Schema writerSchema = ReflectData.get().getSchema(TestDataWithNullableMap.class);

        Schema readerMapSchema = Schema.createMap(Schema.createRecord(TestData.class.getName(), null, null, false,
            Collections.singletonList(new Schema.Field("data1", Schema.create(Schema.Type.STRING), null, Object.class.cast(null)))));
        Schema readerSchema = Schema.createRecord(TestDataWithNullableMap.class.getName(), null, null, false,
            Collections.singletonList(new Schema.Field("map", ReflectData.makeNullable(readerMapSchema), null, Object.class.cast(null))));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new ReflectDatumWriter<>(writerSchema, ReflectData.get()).write(data, EncoderFactory.get().directBinaryEncoder(outputStream, null));

        byte[] serialized = outputStream.toByteArray();

        TestDataWithNullableMap deserialized = new ReflectDecoderAvroDeserializer.ReflectDecoderDatumReader<TestDataWithNullableMap>(writerSchema, readerSchema, ReflectData.get())
            .read(null, DecoderFactory.get().binaryDecoder(serialized, 0, serialized.length, null));

        assertEquals(data.getMap().get("KEY").getData1(), deserialized.getMap().get("KEY").getData1());
    }
}
