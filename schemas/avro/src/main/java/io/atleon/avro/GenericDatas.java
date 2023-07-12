package io.atleon.avro;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

import java.lang.reflect.Type;
import java.util.function.Function;

public final class GenericDatas {

    private GenericDatas() {

    }

    public static <T extends GenericData> T addLogicalTypeConversion(T genericData) {
        genericData.addLogicalTypeConversion(new Conversions.UUIDConversion());
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        genericData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());

        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());

        return genericData;
    }

    static Function<Type, Schema> createTypeSchemaLoader(GenericData genericData) {
        if (genericData instanceof SpecificData) {
            return SpecificData.class.cast(genericData)::getSchema;
        } else {
            return type -> {
                throw new UnsupportedOperationException("Cannot load schema using GenericData for type=" + type);
            };
        }
    }
}
