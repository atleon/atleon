package io.atleon.aws.sqs;

import io.atleon.aws.util.SdkConfig;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Config used by SQS Resources to build Clients and load typed configuration values.
 */
public class SqsConfig {

    /**
     * @deprecated Use {@link SdkConfig#SQS_ENDPOINT_OVERRIDE_CONFIG} instead.
     */
    @Deprecated
    public static final String ENDPOINT_OVERRIDE_CONFIG = SdkConfig.SQS_ENDPOINT_OVERRIDE_CONFIG;

    private final Map<String, ?> properties;

    protected SqsConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static SqsConfig create(Map<String, ?> map) {
        return new SqsConfig(Collections.unmodifiableMap(new HashMap<>(map)));
    }

    public SqsAsyncClient buildClient() {
        return ConfigurableSqsAsyncClientSupplier.load(properties, AtleonSqsAsyncClientSupplier::new)
                .getClient();
    }

    public Map<String, Object> modifyAndGetProperties(Consumer<Map<String, Object>> modifier) {
        Map<String, Object> modifiedProperties = new HashMap<>(properties);
        modifier.accept(modifiedProperties);
        return modifiedProperties;
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property, Class<? extends T> type) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property, type);
    }

    public Set<String> loadSetOfStringOrEmpty(String key) {
        return ConfigLoading.loadSetOfStringOrEmpty(properties, key);
    }

    public <T extends Configurable> Optional<T> loadConfiguredWithPredefinedTypes(
            String key, Class<? extends T> type, Function<String, Optional<T>> predefinedTypeInstantiator) {
        return ConfigLoading.loadConfiguredWithPredefinedTypes(properties, key, type, predefinedTypeInstantiator);
    }

    public Optional<Duration> loadDuration(String key) {
        return ConfigLoading.loadDuration(properties, key);
    }

    public Optional<String> loadString(String key) {
        return ConfigLoading.loadString(properties, key);
    }

    public Optional<Integer> loadInt(String key) {
        return ConfigLoading.loadInt(properties, key);
    }
}
