package io.atleon.avro;

import io.atleon.util.Instantiation;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.function.Function;
import java.util.stream.Stream;

public final class GenericDatas {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericDatas.class);

    private GenericDatas() {}

    public static void addLogicalTypeConversion(GenericData genericData) {
        instantiateConversionsFrom(Conversions.class).forEach(genericData::addLogicalTypeConversion);
        instantiateConversionsFrom(TimeConversions.class).forEach(genericData::addLogicalTypeConversion);
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

    private static Stream<Conversion<?>> instantiateConversionsFrom(Class<?> outerClass) {
        return Stream.of(outerClass.getDeclaredClasses())
                .filter(Conversion.class::isAssignableFrom)
                .flatMap(GenericDatas::tryInstantiateConversion);
    }

    private static Stream<Conversion<?>> tryInstantiateConversion(Class<?> conversionClass) {
        try {
            return Stream.of((Conversion<?>) Instantiation.one(conversionClass));
        } catch (Throwable error) {
            LOGGER.debug("Failed to instantiate Conversion of type={}", conversionClass, error);
            return Stream.empty();
        }
    }
}
