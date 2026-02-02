package io.atleon.rabbitmq;

import java.io.IOException;

/**
 * Wraps error (as cause) from <code>basicPublish</code> on {@link com.rabbitmq.client.Channel}
 * with corresponding delivery tag.
 */
final class PublishException extends RuntimeException {

    private final long deliveryTag;

    public PublishException(long deliveryTag, IOException cause) {
        super(cause);
        this.deliveryTag = deliveryTag;
    }

    public long deliveryTag() {
        return deliveryTag;
    }
}
