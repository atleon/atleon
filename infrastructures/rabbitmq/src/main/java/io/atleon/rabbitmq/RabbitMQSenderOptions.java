package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
import io.atleon.util.IOSupplier;
import org.jspecify.annotations.Nullable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Options used to configure the reactive publishing of messages to RabbitMQ.
 */
public final class RabbitMQSenderOptions {

    private static final int DEFAULT_MAX_POOLED_CHANNELS = 32;

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private final Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionSupplierFactory;

    private final Map<String, Object> connectionProperties;

    private final int maxPooledChannels;

    private final int maxInFlight;

    private final Duration closeTimeout;

    private final Function<@Nullable Object, Consumer<Runnable>> correlationRunnerFactory;

    private RabbitMQSenderOptions(
            Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionSupplierFactory,
            Map<String, Object> connectionProperties,
            int maxPooledChannels,
            int maxInFlight,
            Duration closeTimeout,
            Function<@Nullable Object, Consumer<Runnable>> correlationRunnerFactory) {
        this.connectionSupplierFactory = connectionSupplierFactory;
        this.connectionProperties = connectionProperties;
        this.maxPooledChannels = maxPooledChannels;
        this.maxInFlight = maxInFlight;
        this.closeTimeout = closeTimeout;
        this.correlationRunnerFactory = correlationRunnerFactory;
    }

    public static RabbitMQSenderOptions defaultOptions() {
        return newBuilder().build();
    }

    public static RabbitMQSenderOptions.Builder newBuilder() {
        return new Builder(it -> ConfigurableConnectionSupplier.load(it, ConfiguratorConnectionSupplier::new));
    }

    public static RabbitMQSenderOptions.Builder newBuilder(IOSupplier<Connection> connectionSupplier) {
        return new Builder(__ -> ConfigurableConnectionSupplier.wrap(connectionSupplier));
    }

    public Connection createConnection() throws IOException {
        return connectionSupplierFactory.apply(connectionProperties).getConnection();
    }

    public Scheduler createIOScheduler() {
        return Schedulers.newSingle("atleon-rabbitmq-send");
    }

    /**
     * @see Builder#maxInFlight(int)
     */
    public int maxInFlight() {
        return maxInFlight;
    }

    /**
     * @see Builder#maxPooledChannels(int)
     */
    public int maxPooledChannels() {
        return maxPooledChannels;
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    /**
     * @see Builder#correlationRunnerFactory(Class, Function)
     */
    public Consumer<Runnable> createCorrelationRunner(@Nullable Object correlationMetadata) {
        return correlationRunnerFactory.apply(correlationMetadata);
    }

    public static final class Builder {

        private final Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionSupplierFactory;

        private Map<String, Object> connectionProperties = Collections.emptyMap();

        private int maxPooledChannels = DEFAULT_MAX_POOLED_CHANNELS;

        private int maxInFlight = Queues.SMALL_BUFFER_SIZE;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Function<@Nullable Object, Consumer<Runnable>> correlationRunnerFactory = __ -> Runnable::run;

        private Builder(Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionSupplierFactory) {
            this.connectionSupplierFactory = connectionSupplierFactory;
        }

        public RabbitMQSenderOptions build() {
            return new RabbitMQSenderOptions(
                    connectionSupplierFactory,
                    connectionProperties,
                    maxPooledChannels,
                    maxInFlight,
                    closeTimeout,
                    correlationRunnerFactory);
        }

        /**
         * Sets a single native {@link ConnectionFactoryConfigurator ConnectionFactory property}
         * used to configure supplied {@link ConnectionFactory} instances.
         */
        public Builder connectionProperty(String key, Object value) {
            Map<String, Object> updatedConnectionProperties = new HashMap<>(this.connectionProperties);
            updatedConnectionProperties.put(key, value);
            return connectionProperties(updatedConnectionProperties);
        }

        /**
         * Sets the native {@link ConnectionFactoryConfigurator ConnectionFactory properties} used
         * to configure supplied {@link ConnectionFactory} instances.
         */
        public Builder connectionProperties(Map<String, Object> connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        /**
         * Sets the maximum number of channels allowed in sending pool.
         */
        public Builder maxPooledChannels(int maxPooledChannels) {
            this.maxPooledChannels = maxPooledChannels;
            return this;
        }

        /**
         * Sets the maximum number of deliveries that are allowed to have their publishing
         * initiated, and for which we are awaiting a result (of either success or failure).
         */
        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        /**
         * Sets timeout used on invocations to {@link com.rabbitmq.client.Connection#close(int)}.
         */
        public Builder closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * Sets a factory which is used to create "runners" (immediate invocators of
         * {@link Runnable#run()}) from individual {@link RabbitMQSenderMessage#correlationMetadata()}
         * values. This can be useful in cases where correlation metadata may provide execution
         * context (like thread locals or scoped values) around low-level sending operations, e.g.
         * distributed tracing.
         *
         * @param metadataType The type of correlation metadata from which runners can be created
         * @param factory      Function used to derive a runner from an instance the metadata type
         * @return This builder
         */
        <T> Builder correlationRunnerFactory(
                Class<T> metadataType, Function<? super T, ? extends Consumer<Runnable>> factory) {
            this.correlationRunnerFactory = metadata ->
                    metadataType.isInstance(metadata) ? factory.apply(metadataType.cast(metadata)) : Runnable::run;
            return this;
        }
    }
}
