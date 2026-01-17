package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class StringHashGroupExtractorTest {

    @Test
    public void stringsAreAppropriatelyHashed() {
        String string1 = "Hello, World";
        String string2 = "Hola, Mundo";

        Function<String, Integer> groupExtractor = StringHashGroupExtractor.composed(Function.identity(), 2);

        assertEquals(groupExtractor.apply(string1), groupExtractor.apply(string1));
        assertNotEquals(groupExtractor.apply(string1), groupExtractor.apply(string2));
    }
}
