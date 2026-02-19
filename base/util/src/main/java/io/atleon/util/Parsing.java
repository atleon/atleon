package io.atleon.util;

import java.util.function.Function;

public final class Parsing {

    private Parsing() {}

    public static Integer toInteger(String value) {
        return parse(value, Integer::valueOf);
    }

    public static Long toLong(String value) {
        return parse(value, Long::valueOf);
    }

    private static <T extends Number> T parse(String value, Function<? super String, ? extends T> parser) {
        try {
            return parser.apply(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Could not parse value=" + value, e);
        }
    }
}
