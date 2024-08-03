package io.atleon.core;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interceptor that can be used to randomize configurations contained in Config Providers. For
 * example, you might want to randomize a consumer group like the following:
 * <p>
 * group.id: my-group-id,
 * group.id.randomize: true
 * <p>
 * Upon creation of the configs, group.id will have a value of  my-group-id with a random UUID
 * appended to it, i.e. my-group-id-deadbeef-dead-dead-beef-deadcafebeef
 */
public final class ConditionallyRandomizedConfigs implements ConfigInterceptor {

    public static final String PROPERTY_SUFFIX = ".randomize";

    private static final Map<String, Map<Object, String>> RANDOMIZATIONS_BY_NAME = new ConcurrentHashMap<>();

    @Override
    public Map<String, Object> intercept(String name, Map<String, Object> configs) {
        return conditionallyRandomize(configs, createNamedRandomizer(name));
    }

    @Override
    public Map<String, Object> intercept(Map<String, Object> configs) {
        return conditionallyRandomize(configs, ConditionallyRandomizedConfigs::randomize);
    }

    private Map<String, Object> conditionallyRandomize(Map<String, Object> configs, Function<Object, String> randomizer) {
        return configs.entrySet().stream()
            .filter(entry -> !entry.getKey().endsWith(PROPERTY_SUFFIX))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> shouldRandomize(configs, entry.getKey())
                    ? randomizer.apply(entry.getValue()) : entry.getValue()));
    }

    private static Function<Object, String> createNamedRandomizer(String name) {
        return value -> RANDOMIZATIONS_BY_NAME.computeIfAbsent(name, unused -> new ConcurrentHashMap<>())
            .computeIfAbsent(value, ConditionallyRandomizedConfigs::randomize);
    }

    private static boolean shouldRandomize(Map<String, Object> configs, String property) {
        return Objects.equals(Boolean.toString(true), Objects.toString(configs.get(property + PROPERTY_SUFFIX)));
    }

    private static String randomize(Object value) {
        return value + "-" + UUID.randomUUID();
    }
}
