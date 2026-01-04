package io.atleon.avro;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

public class GenericDatasTest {

    @Test
    public void logicalTypeConversionsCanBeAdded() {
        GenericData genericData = new GenericData();

        GenericDatas.addLogicalTypeConversion(genericData);

        assertTrue(genericData.getConversions().stream().anyMatch(Conversions.DecimalConversion.class::isInstance));
        assertTrue(
                genericData.getConversions().stream().anyMatch(TimeConversions.TimeMillisConversion.class::isInstance));
    }
}
