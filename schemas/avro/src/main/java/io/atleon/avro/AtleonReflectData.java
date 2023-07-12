package io.atleon.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

public final class AtleonReflectData extends ReflectData {

    private static final AtleonReflectData INSTANCE = new AtleonReflectData();

    public static AtleonReflectData get() {
        return INSTANCE;
    }

    @Override
    public ReflectDatumReader<?> createDatumReader(Schema writer, Schema reader) {
        return new AtleonReflectData.DatumReader<>(writer, reader, this);
    }

    public static final class AllowNull extends ReflectData.AllowNull {

        @Override
        public ReflectDatumReader<?> createDatumReader(Schema writer, Schema reader) {
            return new AtleonReflectData.DatumReader<>(writer, reader, this);
        }
    }

    /**
     * At one point, Avro did not know how to handle the deserialization of certain abstract types,
     * like Sets and SortedSets. This DatumReader addresses types we wanted to support at ToW.
     * Clients that need to support other types should be able to follow a similar pattern
     */
    private static final class DatumReader<T> extends ReflectDatumReader<T> {

        private static final Map<String, Supplier<?>> CREATORS_BY_CLASS_NAME = createCreatorsByClassName();

        public DatumReader(Schema writer, Schema reader, ReflectData data) {
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
