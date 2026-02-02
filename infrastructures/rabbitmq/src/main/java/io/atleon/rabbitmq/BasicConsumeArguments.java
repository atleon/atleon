package io.atleon.rabbitmq;

import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * Encapsulates arguments for <code>basicConsume</code> on {@link com.rabbitmq.client.Channel}.
 */
final class BasicConsumeArguments {

    private final String queue;

    private final String consumerTag;

    private final @Nullable Integer priority;

    public BasicConsumeArguments(String queue, String consumerTag, @Nullable Integer priority) {
        this.queue = queue;
        this.consumerTag = consumerTag;
        this.priority = priority;
    }

    public String queue() {
        return queue;
    }

    public String consumerTag() {
        return consumerTag;
    }

    public Optional<Integer> priority() {
        return Optional.ofNullable(priority);
    }
}
