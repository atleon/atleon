package io.atleon.kafka.avro;

import io.atelon.util.ConfigLoading;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

public class ReflectEncoderAvroSerializer<T> extends LoadingAvroSerializer<T> {

    public static final String REFLECT_ALLOW_NULL_PROPERTY = "reflect.allow.null";

    private boolean reflectAllowNull = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        this.reflectAllowNull = ConfigLoading.load(configs, REFLECT_ALLOW_NULL_PROPERTY, Boolean::valueOf, reflectAllowNull);
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        return getReflectData().getSchema(dataType);
    }

    @Override
    protected void serializeDataToOutput(ByteArrayOutputStream output, Schema schema, T data) throws IOException {
        new ReflectDatumWriter<>(schema, getReflectData()).write(data, EncoderFactory.get().directBinaryEncoder(output, null));
    }

    private ReflectData getReflectData() {
        return reflectAllowNull ? ReflectData.AllowNull.get() : ReflectData.get();
    }
}
