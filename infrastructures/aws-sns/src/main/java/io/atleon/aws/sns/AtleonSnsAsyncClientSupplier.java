package io.atleon.aws.sns;

import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of {@link ConfigurableSnsAsyncClientSupplier} that creates {@link SnsAsyncClient}
 * instances using Atleon-defined configuration properties.
 * @see SdkConfig
 * @see AwsConfig
 */
public class AtleonSnsAsyncClientSupplier implements ConfigurableSnsAsyncClientSupplier {

    private Map<String, ?> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = properties;
    }

    @Override
    public SnsAsyncClient getClient() {
        return SnsAsyncClient.builder()
                .endpointOverride(SdkConfig.loadSnsEndpointOverride(properties).orElse(null))
                .credentialsProvider(AwsConfig.loadCredentialsProvider(properties))
                .region(AwsConfig.loadRegion(properties).orElse(null))
                .build();
    }
}
