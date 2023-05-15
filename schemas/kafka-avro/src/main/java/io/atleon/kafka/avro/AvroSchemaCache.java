package io.atleon.kafka.avro;

import org.apache.avro.Schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class AvroSchemaCache<K> {

    private final Map<K, Schema> cache = new ConcurrentHashMap<>();

    public Schema load(K key, Function<K, Schema> cacheLoader) {
        return cache.computeIfAbsent(key, cacheLoader);
    }
}
