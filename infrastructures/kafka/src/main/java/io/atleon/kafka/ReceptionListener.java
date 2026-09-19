package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * A listener interface for callbacks associated with the activation and deactivation of partitions
 * from which Kafka records may be received, as well as the activation and deactivation of records
 * received and emitted downstream. This can be useful for implementing observability around the
 * status of assignable partitions, and the number of in-flight records for a given reception
 * process.
 */
public interface ReceptionListener {

    /**
     * Creates a listener that does nothing.
     */
    static ReceptionListener noOp() {
        return new ReceptionListener() {};
    }

    /**
     * Invoked when a partition has been assigned and from which records may now be received.
     */
    default void onPartitionActivated(TopicPartition partition) {}

    /**
     * Invoked when a partition has been revoked or lost, and any remaining activated records from
     * that partition have also been deactivated.
     */
    default void onPartitionDeactivated(TopicPartition partition) {}

    /**
     * Invoked when the provided {@link ConsumerRecord} has been polled. Note that this does not
     * guarantee outstanding downstream demand for the given record, nor that the given record will
     * be emitted, but rather that the record has been prefetched for possible emission.
     */
    default void onRecordPolled(ConsumerRecord<?, ?> consumerRecord) {}

    /**
     * Invoked when records have been received from the given partition and activated for ordered
     * processing and acknowledgement. This includes records that an {@link OffsetTracker}
     * prohibits from being emitted and which are therefore immediately acknowledged/deactivated.
     *
     * @param partition The active partition from which records have been activated
     * @param count     The number of records that have been activated
     */
    default void onRecordsActivated(TopicPartition partition, long count) {}

    /**
     * Invoked when activated records have either been immediately acknowledged without emission,
     * had their downstream processing completed (via positive or negative acknowledgement), or
     * been forcefully deactivated due to reception error or downstream cancellation.
     *
     * @param partition The active partition from which records have been deactivated
     * @param count     The number of records that have been deactivated
     */
    default void onRecordsDeactivated(TopicPartition partition, long count) {}

    /**
     * Invoked once the reception process has been terminated.
     */
    default void close() {}
}
