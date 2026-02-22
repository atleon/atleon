package io.atleon.aws.util;

import io.atleon.util.ConfigLoading;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Properties and utility methods used to configure AWS resources built by extensions of
 * {@code SdkClientBuilder}.
 */
public final class SdkConfig {

    public static final String SNS_ENDPOINT_OVERRIDE_CONFIG = "sns.endpoint.override";

    public static final String SQS_ENDPOINT_OVERRIDE_CONFIG = "sqs.endpoint.override";

    private SdkConfig() {}

    public static Optional<URI> loadSnsEndpointOverride(Map<String, ?> properties) {
        return ConfigLoading.loadUri(properties, SNS_ENDPOINT_OVERRIDE_CONFIG);
    }

    public static Optional<URI> loadSqsEndpointOverride(Map<String, ?> properties) {
        return ConfigLoading.loadUri(properties, SQS_ENDPOINT_OVERRIDE_CONFIG);
    }
}
