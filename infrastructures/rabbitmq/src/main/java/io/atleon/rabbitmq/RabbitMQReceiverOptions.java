package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
import io.atleon.util.Defaults;
import io.atleon.util.IOSupplier;
import org.jspecify.annotations.Nullable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Options used to configure the reactive reception of messages from RabbitMQ.
 */
public final class RabbitMQReceiverOptions {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private final IOSupplier<Connection> connectionSupplier;

    private final int prefetch;

    private final String consumerTag;

    private final @Nullable Integer priority;

    private final Duration closeTimeout;

    private RabbitMQReceiverOptions(
            IOSupplier<Connection> connectionSupplier,
            int prefetch,
            String consumerTag,
            @Nullable Integer priority,
            Duration closeTimeout) {
        this.connectionSupplier = connectionSupplier;
        this.prefetch = prefetch;
        this.consumerTag = consumerTag;
        this.priority = priority;
        this.closeTimeout = closeTimeout;
    }

    public static RabbitMQReceiverOptions defaultOptions() {
        return newBuilder().build();
    }

    public static RabbitMQReceiverOptions.Builder newBuilder() {
        return new RabbitMQReceiverOptions.Builder(
                it -> ConfigurableConnectionSupplier.load(it, ConfiguratorConnectionSupplier::new));
    }

    public static RabbitMQReceiverOptions.Builder newBuilder(IOSupplier<Connection> connectionSupplier) {
        return new RabbitMQReceiverOptions.Builder(__ -> connectionSupplier::get);
    }

    public Connection createConnection() throws IOException {
        return connectionSupplier.get();
    }

    public Scheduler createIOScheduler() {
        return Schedulers.newSingle("atleon-rabbitmq-receive");
    }

    /**
     * @see Builder#prefetch(int)
     */
    public int prefetch() {
        return prefetch;
    }

    /**
     * @see Builder#consumerTag(String)
     */
    public String consumerTag() {
        return consumerTag;
    }

    /**
     * @see Builder#priority(int)
     */
    public Optional<Integer> priority() {
        return Optional.ofNullable(priority);
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    public static final class Builder {

        private final Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionProviderFactory;

        private Map<String, Object> connectionProperties = Collections.emptyMap();

        private int prefetch = Defaults.PREFETCH;

        private String consumerTag = "";

        private @Nullable Integer priority;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Builder(Function<Map<String, Object>, ConfigurableConnectionSupplier> connectionProviderFactory) {
            this.connectionProviderFactory = connectionProviderFactory;
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
         * Sets the maximum number of unacked/unsettled messages that the underlying channel will
         * allow to be delivered to per consumption.
         */
        public Builder prefetch(int prefetch) {
            this.prefetch = prefetch;
            return this;
        }

        /**
         * Sets the tag used to identify a consumer.
         */
        public Builder consumerTag(String consumerTag) {
            this.consumerTag = consumerTag;
            return this;
        }

        /**
         * Sets the priority for underlying consumption.
         */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Sets timeout used on invocations to {@link com.rabbitmq.client.Connection#close(int)}.
         */
        public Builder closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public RabbitMQReceiverOptions build() {
            ConfigurableConnectionSupplier connectionSupplier = connectionProviderFactory.apply(connectionProperties);
            return new RabbitMQReceiverOptions(
                    connectionSupplier::getConnection, prefetch, consumerTag, priority, closeTimeout);
        }
    }
}
