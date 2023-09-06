package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.kafka.receiver.ReceiverRecord;

public final class ConsumerRecordExtraction {

    private ConsumerRecordExtraction() {

    }

    public static TopicPartition topicPartition(ConsumerRecord<?, ?> consumerRecord) {
        return consumerRecord instanceof ReceiverRecord
            ? ReceiverRecord.class.cast(consumerRecord).receiverOffset().topicPartition()
            : new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    }
}
