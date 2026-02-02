package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

/**
 * A RabbitMQ message that can be sent reactively
 *
 * @param <T> The type of correlation metadata propagated by this message
 */
public final class RabbitMQSenderMessage<T> {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final byte[] body;

    private final T correlationMetadata;

    private RabbitMQSenderMessage(
            String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body, T correlationMetadata) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
        this.correlationMetadata = correlationMetadata;
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties properties() {
        return properties;
    }

    public byte[] body() {
        return body;
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }

    public static final class Builder<T> {

        private String exchange = "";

        private String routingKey = "";

        private AMQP.BasicProperties properties = null;

        private byte[] body = new byte[0];

        private T correlationMetadata = null;

        private Builder() {}

        public Builder<T> exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder<T> routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder<T> properties(AMQP.BasicProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder<T> body(byte[] body) {
            this.body = body;
            return this;
        }

        public Builder<T> correlationMetadata(T correlationMetadata) {
            this.correlationMetadata = correlationMetadata;
            return this;
        }

        public RabbitMQSenderMessage<T> build() {
            return new RabbitMQSenderMessage<>(exchange, routingKey, properties, body, correlationMetadata);
        }
    }
}
