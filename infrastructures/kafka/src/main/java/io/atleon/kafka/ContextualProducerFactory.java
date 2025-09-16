package io.atleon.kafka;

import org.apache.kafka.clients.producer.Producer;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.internals.ProducerFactory;

final class ContextualProducerFactory extends ProducerFactory {

    public static final ProducerFactory INSTANCE = new ContextualProducerFactory();

    private ContextualProducerFactory() {

    }

    @Override
    public <K, V> Producer<K, V> createProducer(SenderOptions<K, V> senderOptions) {
        return new ContextualProducer<>(super.createProducer(senderOptions));
    }
}
