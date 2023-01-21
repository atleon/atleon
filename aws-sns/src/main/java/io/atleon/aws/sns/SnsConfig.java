package io.atleon.aws.sns;

import io.atleon.aws.util.AwsConfig;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Config used by SNS Resources to build Clients and load typed configuration values.
 */
public final class SnsConfig {

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
            .endpointOverride(ConfigLoading.load(properties, ENDPOINT_OVERRIDE_CONFIG, URI::create).orElse(null))
            .credentialsProvider(AwsConfig.loadCredentialsProvider(properties))
            .region(AwsConfig.loadRegion(properties).orElse(null))
            .build();
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property);
    }

    public Duration loadDuration(String key, Duration defaultValue) {
        return ConfigLoading.load(properties, key, Duration::parse, defaultValue);
    }

    public int loadInt(String key, int defaultValue) {
        return ConfigLoading.load(properties, key, Integer::parseInt, defaultValue);
    }
}
