package io.atleon.aws.sns;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.util.Map;
import java.util.function.Supplier;

/**
 * API through which a provider of {@link SnsAsyncClient} instances can be configured and invoked.
 */
public interface ConfigurableSnsAsyncClientSupplier extends Configurable {

    /**
     * When configuring a client supplier in properties, the value for this key can be either the
     * qualified name of a {@link ConfigurableSnsAsyncClientSupplier} implementation or an instance
     * of one.
     */
    String CONFIG = "configurable.sns.async.client.supplier";

    static ConfigurableSnsAsyncClientSupplier load(
            Map<String, ?> properties, Supplier<? extends ConfigurableSnsAsyncClientSupplier> defaultSupplier) {
        return ConfigLoading.loadConfigured(
                properties, CONFIG, ConfigurableSnsAsyncClientSupplier.class, defaultSupplier);
    }

    static ConfigurableSnsAsyncClientSupplier wrap(Supplier<SnsAsyncClient> supplier) {
        return new ConfigurableSnsAsyncClientSupplier() {
            @Override
            public void configure(Map<String, ?> properties) {}

            @Override
            public SnsAsyncClient getClient() {
                return supplier.get();
            }
        };
    }

    SnsAsyncClient getClient();
}
