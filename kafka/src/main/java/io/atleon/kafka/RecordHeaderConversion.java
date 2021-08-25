package io.atleon.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class RecordHeaderConversion {

    private RecordHeaderConversion() {

    }

    public static Header toHeader(String key, String value) {
        return new RecordHeader(key, serializeValue(value));
    }

    public static Map<String, String> toMap(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
            .collect(Collectors.toMap(Header::key, header -> deserializeValue(header.value()), lastValueWins()));
    }

    private static byte[] serializeValue(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String deserializeValue(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static BinaryOperator<String> lastValueWins() {
        return (value1, value2) -> value2;
    }
}
