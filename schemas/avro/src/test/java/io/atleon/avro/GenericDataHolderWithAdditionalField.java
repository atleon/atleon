package io.atleon.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

import java.util.Arrays;
import java.util.Objects;

public class GenericDataHolderWithAdditionalField<T> extends GenericDataHolder<T> implements GenericContainer {

    private String extraData;

    @Override
    public Schema getSchema() {
        return Schema.createRecord(
                GenericDataHolder.class.getName(),
                null,
                null,
                false,
                Arrays.asList(
                        new Schema.Field("extraData", Schema.create(Schema.Type.STRING), null, Object.class.cast(null)),
                        new Schema.Field(
                                "data", AvroSchemas.getOrReflectNullable(getData()), null, JsonProperties.NULL_VALUE)));
    }

    public void setExtraData(String extraData) {
        this.extraData = extraData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GenericDataHolderWithAdditionalField<?> that = (GenericDataHolderWithAdditionalField<?>) o;
        return Objects.equals(extraData, that.extraData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), extraData);
    }
}
