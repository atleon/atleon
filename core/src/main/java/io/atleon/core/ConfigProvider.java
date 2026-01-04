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
 * Base class of Config-producing resources. Provided Configs are (typically) based solely on the
 * contents of the {@code properties} Map wrapped by this Provider.
 *
 * <p>Config Providers can be named either explicitly or via property introspection. This is useful
 * when applying {@link ConfigInterceptor}s. By default, Providers should use the default
 * ConfigInterceptors to transform the properties Map prior to generating the final Config. The
 * default interceptors allow clients to:
 * <ul>
 *     <li>Override named properties via system and/or environment variables:
 *     <p>Override the value of "key" in a ConfigProvider with the name "aws" to "overridden"
 *     <p>{@code System.setProperty("atleon.config.aws.key", "overridden")}</li>
 *     <li>Randomize properties:
 *     <p>Randomize "value" associated with "key" by appending a random UUID
 *     <p>{@code configProvider.with("key", "value").with("key.randomize", true)}</li>
 * </ul>
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

    public final <V> V as(Function<? super P, ? extends V> transformer) {
        return transformer.apply((P) this);
    }

    public final T create() {
        return propertiesToName
                .apply(properties)
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

    protected static void validateAnyNonNullProperty(Map<String, Object> properties, String... keys) {
        for (String key : keys) {
            if (properties.get(key) != null) {
                return;
            }
        }
        throw new IllegalArgumentException("At least one of " + Arrays.toString(keys) + " must be configured");
    }

    final void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    final void setPropertiesToName(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }
}
