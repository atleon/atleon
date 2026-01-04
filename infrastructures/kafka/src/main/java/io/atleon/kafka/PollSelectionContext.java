package io.atleon.kafka;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Interface through which {@link PollStrategy} implementations may select partitions for polling
 * and access relevant consumer metadata.
 */
public interface PollSelectionContext {

    /**
     * Configures the consumer to poll exclusively from the specified partitions, pausing all other
     * assigned partitions for the subsequent poll operation.
     *
     * @param partitions the set of topic partitions to poll exclusively
     */
    void selectExclusively(Set<TopicPartition> partitions);

    /**
     * Configures the consumer to use natural partition selection for the next poll operation,
     * allowing the consumer to poll from all assigned (and non-externally paused) partitions based
     * on the default Kafka consumer behavior.
     */
    void selectNaturally();

    /**
     * Retrieves current lag for the provided partitions. This method will return immediately, and
     * is backed by cached metadata in the underlying consumer. It is therefore possible that not
     * every provided partition will have lag metadata immediately available. In this case, the
     * provided default value will be returned for each partition that does not (yet) have
     * metadata. Note that the freshness of the underlying metadata is controlled by the consumer,
     * and may become relatively stale for any partition that has not been actively polled for a
     * significant period of time. It is therefore recommended that attention is given to the
     * configuration {@link org.apache.kafka.clients.CommonClientConfigs#METADATA_MAX_AGE_CONFIG}
     * such as to potentially limit the max staleness of lag metadata.
     *
     * @param partitions   the set of partitions for which to get lag measurements
     * @param defaultValue the default value to use when lag metadata is not immediately available
     * @return a map of partitions to their current lag values
     * @see org.apache.kafka.clients.consumer.Consumer#currentLag(TopicPartition)
     */
    Map<TopicPartition, Long> currentLag(Set<TopicPartition> partitions, long defaultValue);

    /**
     * Retrieves current lag in terms of the number of logical batches (as configured by
     * {@link org.apache.kafka.clients.consumer.ConsumerConfig#MAX_POLL_RECORDS_CONFIG}) for the
     * provided partitions. This method will return immediately, and is backed by cached metadata
     * in the underlying consumer. It is therefore possible that not every provided partition will
     * have lag metadata immediately available. In this case, the provided default value will be
     * returned for each partition that does not (yet) have metadata. Note that the freshness of
     * the underlying metadata is controlled by the consumer, and may become relatively stale for
     * any partition that has not been actively polled for a significant period of time. It is
     * therefore recommended that attention is given to the configuration of
     * {@link org.apache.kafka.clients.CommonClientConfigs#METADATA_MAX_AGE_CONFIG} such as to
     * potentially limit the max staleness of lag metadata.
     *
     * @param partitions   the set of partitions for which to get lag measurements
     * @param defaultValue the default value to use when lag metadata is not immediately available
     * @return a map of partitions to their current batch lag values
     * @see org.apache.kafka.clients.consumer.Consumer#currentLag(TopicPartition)
     */
    Map<TopicPartition, Long> currentBatchLag(Set<TopicPartition> partitions, long defaultValue);
}
