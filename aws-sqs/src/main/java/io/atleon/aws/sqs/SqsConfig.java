package io.atleon.aws.sqs;

import io.atleon.aws.util.AwsConfig;
import io.atleon.core.AloDecorator;
import io.atleon.core.AloDecoratorConfig;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import io.atleon.util.Instantiation;
import io.atleon.util.TypeResolution;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Config used by SQS Resources to build Clients and load typed configuration values.
 */
public class SqsConfig {

    public static final String ENDPOINT_OVERRIDE_CONFIG = "sqs.endpoint.override";

    private final Map<String, ?> properties;

    private SqsConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static SqsConfig fromMap(Map<String, ?> map) {
        return new SqsConfig(Collections.unmodifiableMap(new HashMap<>(map)));
    }

    public SqsAsyncClient buildClient() {
        return SqsAsyncClient.builder()
            .endpointOverride(ConfigLoading.load(properties, ENDPOINT_OVERRIDE_CONFIG, URI::create).orElse(null))
            .credentialsProvider(AwsConfig.loadCredentialsProvider(properties))
            .region(AwsConfig.loadRegion(properties).orElse(null))
            .build();
    }

    public <T> Optional<AloDecorator<ReceivedSqsMessage<T>>> loadAloDecorator(String descriptorsProperty) {
        return AloDecoratorConfig.load(AloReceivedSqsMessageDecorator.class, properties, descriptorsProperty);
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property);
    }

    public <T extends Configurable> T createConfigured(String qualifiedName) {
        return Instantiation.oneConfigured(TypeResolution.classForQualifiedName(qualifiedName), properties);
    }

    public Set<String> loadSetOfStringOrEmpty(String key) {
        return ConfigLoading.loadSetOrEmpty(properties, key, Function.identity());
    }

    public Duration loadDuration(String key, Duration defaultValue) {
        return ConfigLoading.load(properties, key, Duration::parse, defaultValue);
    }

    public String loadString(String key, String defaultValue) {
        return ConfigLoading.load(properties, key, Function.identity(), defaultValue);
    }

    public int loadInt(String key, int defaultValue) {
        return ConfigLoading.load(properties, key, Integer::parseInt, defaultValue);
    }
}
