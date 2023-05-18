package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * An {@link AvroDeserializer} that uses Avro's {@link ReflectData} for type loading and
 * {@link ReflectDatumReader} for data parsing.
 *
 * @param <T> The type of data resulting from deserialization
 */
public final class ReflectAvroDeserializer<T> extends AvroDeserializer<T> {

    private final boolean allowNull;

    public ReflectAvroDeserializer() {
        this.allowNull = false;
    }

    private ReflectAvroDeserializer(boolean readerSchemaLoadingEnabled, boolean readerReferenceSchemaGenerationEnabled, boolean allowNull) {
        super(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled);
        this.allowNull = allowNull;
    }

    public ReflectAvroDeserializer<T> withReaderSchemaLoadingEnabled(boolean readerSchemaLoadingEnabled) {
        return new ReflectAvroDeserializer<>(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled, allowNull);
    }

    public ReflectAvroDeserializer<T> withReaderReferenceSchemaGenerationEnabled(boolean readerReferenceSchemaGenerationEnabled) {
        return new ReflectAvroDeserializer<>(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled, allowNull);
    }

    public ReflectAvroDeserializer<T> withAllowNull(boolean allowNull) {
        return new ReflectAvroDeserializer<>(readerSchemaLoadingEnabled, readerReferenceSchemaGenerationEnabled, allowNull);
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        return getReflectData().getSchema(dataType);
    }

    @Override
    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return new ReflectAvroDatumReader<>(writerSchema, readerSchema, getReflectData());
    }

    private ReflectData getReflectData() {
        return allowNull ? ReflectData.AllowNull.get() : ReflectData.get();
    }

    /**
     * At one point, Avro did not know how to handle the deserialization of certain abstract types,
     * like Sets and SortedSets. This DatumReader addresses types we wanted to support at ToW.
     * Clients that need to support other types should be able to follow a similar pattern
     */
    protected static final class ReflectAvroDatumReader<T> extends ReflectDatumReader<T> {

        private static final Map<String, Supplier<?>> CREATORS_BY_CLASS_NAME = createCreatorsByClassName();

        public ReflectAvroDatumReader(Schema writer, Schema reader, ReflectData data) {
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
