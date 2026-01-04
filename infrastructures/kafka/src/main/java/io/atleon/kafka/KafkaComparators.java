package io.atleon.kafka;

import java.util.Comparator;
import org.apache.kafka.common.TopicPartition;

final class KafkaComparators {

    private KafkaComparators() {}

    public static Comparator<TopicPartition> topicThenPartition() {
        return Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);
    }
}
