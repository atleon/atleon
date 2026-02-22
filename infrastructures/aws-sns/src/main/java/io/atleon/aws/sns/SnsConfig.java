package io.atleon.aws.sns;

import io.atleon.aws.util.SdkConfig;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Config used by SNS Resources to build Clients and load typed configuration values.
 */
public class SnsConfig {

    /**
     * @deprecated Use {@link SdkConfig#SNS_ENDPOINT_OVERRIDE_CONFIG} instead.
     */
    @Deprecated
    public static final String ENDPOINT_OVERRIDE_CONFIG = SdkConfig.SNS_ENDPOINT_OVERRIDE_CONFIG;

    private final Map<String, ?> properties;

    protected SnsConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static SnsConfig create(Map<String, ?> map) {
        return new SnsConfig(Collections.unmodifiableMap(new HashMap<>(map)));
    }

    public SnsAsyncClient buildClient() {
        return ConfigurableSnsAsyncClientSupplier.load(properties, AtleonSnsAsyncClientSupplier::new)
                .getClient();
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property, Class<? extends T> type) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property, type);
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
