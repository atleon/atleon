package io.atleon.kafka;

import io.atleon.core.Alo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.function.Consumer;

public class DefaultAloConsumerRecordFactory<K, V> implements AloConsumerRecordFactory<K, V> {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public Alo<ConsumerRecord<K, V>>
    create(ConsumerRecord<K, V> consumerRecord, Runnable acknowledger, Consumer<? super Throwable> nacknowedger) {
        return new DefaultAloConsumerRecord<>(consumerRecord, acknowledger, nacknowedger);
    }
}
