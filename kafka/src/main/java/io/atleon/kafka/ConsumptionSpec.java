package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Interface used to specify what topic(s) or partition(s) to request for consumption, with
 * callbacks for handling the lifecycle of assignment(s).
 */
interface ConsumptionSpec {

    static ConsumptionSpec subscribe(Collection<String> topics) {
        return (consumer, rebalanceListener) -> consumer.subscribe(topics, rebalanceListener);
    }

    static ConsumptionSpec subscribe(Pattern topicsPattern) {
        return (consumer, rebalanceListener) -> consumer.subscribe(topicsPattern, rebalanceListener);
    }

    static ConsumptionSpec assign(Collection<TopicPartition> topicPartitions) {
        return (consumer, rebalanceListener) -> {
            consumer.assign(topicPartitions);
            rebalanceListener.onPartitionsAssigned(topicPartitions);
        };
    }

    void apply(Consumer<?, ?> consumer, ConsumerRebalanceListener rebalanceListener);
}
