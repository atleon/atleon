package io.atleon.rabbitmq;

/**
 * Encapsulates arguments for <code>basicConsume</code> on {@link com.rabbitmq.client.Channel}.
 */
final class ConsumeArguments {

    private final String queue;

    private final String consumerTag;

    public ConsumeArguments(String queue, String consumerTag) {
        this.queue = queue;
        this.consumerTag = consumerTag;
    }

    public String queue() {
        return queue;
    }

    public String consumerTag() {
        return consumerTag;
    }
}
