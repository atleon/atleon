package io.atleon.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Base class of Config-producing resources
 *
 * @param <P> The type of this ConfigProducer
 */
public abstract class ConfigProvider<T, P extends ConfigProvider<T, P>> {

    private Map<String, Object> properties = Collections.emptyMap();

    private Function<Map<String, Object>, Optional<String>> propertiesToName;

    protected ConfigProvider() {
        this(properties -> Optional.empty());
    }

    protected ConfigProvider(String name) {
        this(properties -> Optional.of(name));
    }

    protected ConfigProvider(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }

    public final T create() {
        return propertiesToName.apply(properties)
            .map(name -> create(name, properties))
            .orElseGet(() -> create(properties));
    }

    public final P rename(String name) {
        P copy = copy();
        copy.setPropertiesToName(properties -> Optional.of(name));
        return copy;
    }

    public P with(String key, Object value) {
        Map<String, Object> copiedProperties = new HashMap<>(this.properties);
        copiedProperties.put(key, value);
        P copy = copy();
        copy.setProperties(Collections.unmodifiableMap(copiedProperties));
        return copy;
    }

    public P withAll(Map<String, ?> properties) {
        Map<String, Object> copiedProperties = new HashMap<>(this.properties);
        copiedProperties.putAll(properties);
        P copy = copy();
        copy.setProperties(Collections.unmodifiableMap(copiedProperties));
        return copy;
    }

    protected abstract T create(String name, Map<String, Object> properties);

    protected abstract T create(Map<String, Object> properties);

    protected final P copy() {
        P copy = initializeCopy();
        copy.setProperties(properties);
        copy.setPropertiesToName(propertiesToName);
        return copy;
    }

    protected abstract P initializeCopy();

    protected Collection<ConfigInterceptor> defaultInterceptors() {
        return Arrays.asList(new EnvironmentalConfigs(), new ConditionallyRandomizedConfigs());
    }

    protected static void validateNonNullProperty(Map<String, Object> properties, String key) {
        Objects.requireNonNull(properties.get(key), key + " is a required Configuration");
    }

    protected static <T extends Enum<T>> void
    validateEnumProperty(Map<String, Object> properties, String key, Class<T> enumClass) {
        try {
            Enum.valueOf(enumClass, Objects.toString(properties.get(key)));
        } catch (Exception e) {
            throw new IllegalArgumentException(key + " must be configured as an Enum value from " + enumClass, e);
        }
    }

    final void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    final void setPropertiesToName(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }
}
