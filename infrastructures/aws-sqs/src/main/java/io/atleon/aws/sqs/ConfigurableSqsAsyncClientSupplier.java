package io.atleon.aws.sqs;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Map;
import java.util.function.Supplier;

/**
 * API through which a provider of {@link SqsAsyncClient} instances can be configured and invoked.
 */
public interface ConfigurableSqsAsyncClientSupplier extends Configurable {

    /**
     * When configuring a client supplier in properties, the value for this key can be either the
     * qualified name of a {@link ConfigurableSqsAsyncClientSupplier} implementation or an instance
     * of one.
     */
    String CONFIG = "configurable.sqs.async.client.supplier";

    static ConfigurableSqsAsyncClientSupplier load(
            Map<String, ?> properties, Supplier<? extends ConfigurableSqsAsyncClientSupplier> defaultSupplier) {
        return ConfigLoading.loadConfigured(
                properties, CONFIG, ConfigurableSqsAsyncClientSupplier.class, defaultSupplier);
    }

    static ConfigurableSqsAsyncClientSupplier wrap(Supplier<SqsAsyncClient> supplier) {
        return new ConfigurableSqsAsyncClientSupplier() {
            @Override
            public void configure(Map<String, ?> properties) {}

            @Override
            public SqsAsyncClient getClient() {
                return supplier.get();
            }
        };
    }

    SqsAsyncClient getClient();
}
