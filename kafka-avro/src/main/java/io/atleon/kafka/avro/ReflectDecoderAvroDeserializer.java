package io.atleon.kafka.avro;

import io.atelon.util.ConfigLoading;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

public class ReflectDecoderAvroDeserializer<T> extends LoadingAvroDeserializer<T> {

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
    protected T deserializeNonNullWithSchemas(Schema writerSchema, Schema readerSchema, ByteBuffer dataBuffer) throws IOException {
        return createDatumReader(writerSchema, readerSchema, getReflectData())
            .read(null, DecoderFactory.get().binaryDecoder(dataBuffer.array(), dataBuffer.arrayOffset() + dataBuffer.position(), dataBuffer.remaining(), null));
    }

    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema, ReflectData reflectData) {
        return new ReflectDecoderDatumReader<>(writerSchema, readerSchema, reflectData);
    }

    private ReflectData getReflectData() {
        return reflectAllowNull ? ReflectData.AllowNull.get() : ReflectData.get();
    }

    /**
     * At one point, Avro did not know how to handle the deserialization of certain abstract types,
     * like Sets and SortedSets. This DatumReader addresses types we wanted to support at ToW.
     * Clients that need to support other types should be able to follow a similar pattern
     */
    protected static class ReflectDecoderDatumReader<T> extends ReflectDatumReader<T> {

        private static final Map<String, Supplier<?>> CREATORS_BY_CLASS_NAME = createCreatorsByClassName();

        public ReflectDecoderDatumReader(Schema writer, Schema reader, ReflectData data) {
            super(writer, reader, data);
        }

        @Override
        protected Object newArray(Object old, int size, Schema schema) {
            Supplier<?> creator = CREATORS_BY_CLASS_NAME.get(schema.getProp(SpecificData.CLASS_PROP));
            return old == null && creator != null ? creator.get() : super.newArray(old, size, schema);
        }

        private static Map<String, Supplier<?>> createCreatorsByClassName() {
            Map<String, Supplier<?>> creatorsByClassName = new HashMap<>();
            creatorsByClassName.put(Set.class.getName(), HashSet::new);
            creatorsByClassName.put(SortedSet.class.getName(), TreeSet::new);
            return creatorsByClassName;
        }
    }
}
