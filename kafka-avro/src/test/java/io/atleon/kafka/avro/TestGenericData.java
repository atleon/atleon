package io.atleon.kafka.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

import java.util.Collections;
import java.util.Objects;

/**
 * This class should demonstrate how Schemas for generic data fields may be generated and
 * included (or omitted) with the Schemas created at serialization-time for these generic
 * data containers
 */
public class TestGenericData<T> implements GenericContainer {

    private T data;

    /**
     * This method illustrates the use case for generating Schemas when generic data is null. Since
     * we can't union Null with a meaningful Schema (and in the absence of any other way to create
     * such a Schema) and we can't reliably generate a default value, our only backward compatible
     * solution is to omit that data from the Schema.
     */
    @Override
    public Schema getSchema() {
        return Schema.createRecord(TestGenericData.class.getCanonicalName(), null, null, false,
            data == null ? Collections.emptyList() : Collections.singletonList(createDataSchemaField()));
    }

    /**
     * This method illustrates the use case for generating Schemas for non-null generic data that
     * itself may be generic. When that data is generic, it must be serialized as non-nullable (to
     * avoid Avro re-attempting to resolve the non-null Schema) with no default value (since we
     * have no reliable way of generating such a value). When that data is non-generic, we show
     * that the Schema for that field can be made nullable with a default (JSON) value of NULL
     */
    protected Schema.Field createDataSchemaField() {
        return data instanceof TestGenericData ?
            new Schema.Field("data", AvroSchemas.getOrReflect(data), null, (Object) null) :
            new Schema.Field("data", AvroSchemas.getOrReflectNullable(data), null, JsonProperties.NULL_VALUE);
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestGenericData<?> that = (TestGenericData<?>) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
