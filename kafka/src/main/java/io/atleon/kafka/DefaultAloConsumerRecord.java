package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

public class DefaultAloConsumerRecord<K, V> implements Alo<ConsumerRecord<K, V>> {

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAloConsumerRecord(ConsumerRecord<K, V> consumerRecord, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return ComposedAlo::new;
    }

    @Override
    public ConsumerRecord<K, V> get() {
        return consumerRecord;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }
}
