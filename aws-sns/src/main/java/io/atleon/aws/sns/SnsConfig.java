package io.atleon.aws.sns;

import io.atleon.aws.util.AwsConfig;
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

    public static final String ENDPOINT_OVERRIDE_CONFIG = "sns.endpoint.override";

    private final Map<String, ?> properties;

    private SnsConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static SnsConfig fromMap(Map<String, ?> map) {
        return new SnsConfig(Collections.unmodifiableMap(new HashMap<>(map)));
    }

    public SnsAsyncClient buildClient() {
        return SnsAsyncClient.builder()
            .endpointOverride(ConfigLoading.loadUri(properties, ENDPOINT_OVERRIDE_CONFIG).orElse(null))
            .credentialsProvider(AwsConfig.loadCredentialsProvider(properties))
            .region(AwsConfig.loadRegion(properties).orElse(null))
            .build();
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property, Class<? extends T> type) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property, type);
    }

    public Optional<Duration> loadDuration(String key) {
        return ConfigLoading.loadDuration(properties, key);
    }

    public Optional<Integer> loadInt(String key) {
        return ConfigLoading.loadInt(properties, key);
    }
}
