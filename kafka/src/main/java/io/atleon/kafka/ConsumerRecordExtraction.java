package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public final class ConsumerRecordExtraction {

    private ConsumerRecordExtraction() {

    }

    public static TopicPartition topicPartition(ConsumerRecord<?, ?> consumerRecord) {
        return new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    }
}
