package io.atleon.rabbitmq;

import java.util.Collections;
import java.util.Map;

public final class QueueBinding {

    private final String queue;

    private final String exchange;

    private final String routingKey;

    private final Map<String, Object> arguments;

    private QueueBinding(String queue, String exchange, String routingKey) {
        this(queue, exchange, routingKey, Collections.emptyMap());
    }

    private QueueBinding(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        this.queue = queue;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.arguments = arguments;
    }

    /**
     * Starts a binding declaration for a given queue
     *
     * @param queue the queue to start a binding declaration for
     * @return a {@link QueueToBind} representing the start of a {@link QueueBinding} declaration
     */
    public static QueueToBind forQueue(String queue) {
        return new QueueToBind(queue);
    }

    public QueueBinding arguments(Map<String, Object> arguments) {
        return new QueueBinding(queue, exchange, routingKey, arguments);
    }

    public String getQueue() {
        return queue;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public static final class QueueToBind {

        private final String queue;

        private QueueToBind(String queue) {
            this.queue = queue;
        }

        /**
         * Pairs the queue contained by this {@link QueueToBind} with an exchange to create a
         * {@link QueueAndExchangeToBind} through which the binding declaration can be completed
         * with a routing key.
         *
         * @param exchange The exchange to pair with this {@link QueueToBind}'s queue
         * @return A {@link QueueAndExchangeToBind} through which the declaration can be completed
         */
        public QueueAndExchangeToBind toExchange(String exchange) {
            return new QueueAndExchangeToBind(queue, exchange);
        }
    }

    public static final class QueueAndExchangeToBind {

        private final String queue;

        private final String exchange;

        private QueueAndExchangeToBind(String queue, String exchange) {
            this.queue = queue;
            this.exchange = exchange;
        }

        /**
         * Creates a {@link QueueBinding} for this {@link QueueAndExchangeToBind}'s queue and
         * exchange through the provided routing key. This is useful for bindings on DIRECT and
         * TOPIC exchanges.
         *
         * @param routingKey Key through which contained queue and exchange will be bound by
         * @return A {@link QueueBinding}
         */
        public QueueBinding usingRoutingKey(String routingKey) {
            return new QueueBinding(queue, exchange, routingKey);
        }

        /**
         * Creates a {@link QueueBinding} for this {@link QueueAndExchangeToBind}'s queue and
         * exchange through an empty routing key. This is useful for bindings on FANOUT and HEADERS
         * exchanges.
         *
         * @return A {@link QueueBinding}
         */
        public QueueBinding usingEmptyRoutingKey() {
            return new QueueBinding(queue, exchange, "");
        }
    }
}
