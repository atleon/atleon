package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public final class ConsumerRecordExtraction {

    private ConsumerRecordExtraction() {

    }

    public static Map<String, String> extractHeaderMap(ConsumerRecord consumerRecord) {
        return RecordHeaderConversion.toMap(consumerRecord.headers());
    }

    public static TopicPartition extractTopicPartition(ConsumerRecord consumerRecord) {
        return new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    }
}
