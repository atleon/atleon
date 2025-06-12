package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * Interface used to specify what topics and/or partitions to request for assignment, with
 * callbacks for handling the lifecycle of assignment(s).
 */
interface AssignmentSpec {

    void apply(Consumer<?, ?> consumer, ConsumerRebalanceListener rebalanceListener);
}
