package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;

final class KafkaComparators {

    private KafkaComparators() {

    }

    public static Comparator<TopicPartition> topicThenPartition() {
        return Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);
    }
}
