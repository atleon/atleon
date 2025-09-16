package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

/**
 * When using "prioritized" Kafka consumption, this interface is used to configure how records from
 * any given {@link TopicPartition} should be prioritized relative to others. The returned priority
 * number is inversely related to the priority level, i.e. priority number 1 is "higher" priority
 * than priority number 2. Records with higher priority, when available, will be emitted in
 * preference to records available with lower priority. By default, the cardinality of priority
 * numbers will be equal to the number of underlying Kafka Consumer instances and streams that are
 * merged together.
 */
@FunctionalInterface
public interface ReceptionPrioritization {

    /**
     * Returns the <i>non-negative</i> priority "number" for the given {@link TopicPartition}
     */
    int prioritize(TopicPartition topicPartition);
}
