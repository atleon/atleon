package io.atleon.kafka;

import io.atleon.core.AloQueueListener;
import org.apache.kafka.common.TopicPartition;

/**
 * An adapter that bridges {@link AloQueueListener} to {@link ReceptionListener}.
 */
final class AloQueueReceptionListener implements ReceptionListener {

    private final AloQueueListener delegate;

    public AloQueueReceptionListener(AloQueueListener delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onPartitionActivated(TopicPartition partition) {
        delegate.created(partition);
    }

    @Override
    public void onRecordsActivated(TopicPartition partition, long count) {
        delegate.enqueued(partition, count);
    }

    @Override
    public void onRecordsDeactivated(TopicPartition partition, long count) {
        delegate.dequeued(partition, count);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
