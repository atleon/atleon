/**
 * Copyright 2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
