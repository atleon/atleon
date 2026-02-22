package io.atleon.aws.sqs;

import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of {@link ConfigurableSqsAsyncClientSupplier} that creates {@link SqsAsyncClient}
 * instances using Atleon-defined configuration properties.
 * @see SdkConfig
 * @see AwsConfig
 */
public class AtleonSqsAsyncClientSupplier implements ConfigurableSqsAsyncClientSupplier {

    private Map<String, ?> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = properties;
    }

    @Override
    public SqsAsyncClient getClient() {
        return SqsAsyncClient.builder()
                .endpointOverride(SdkConfig.loadSqsEndpointOverride(properties).orElse(null))
                .credentialsProvider(AwsConfig.loadCredentialsProvider(properties))
                .region(AwsConfig.loadRegion(properties).orElse(null))
                .build();
    }
}
