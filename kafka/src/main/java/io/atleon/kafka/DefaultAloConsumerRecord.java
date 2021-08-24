package io.atleon.kafka;

import io.atleon.core.AbstractAlo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

public class DefaultAloConsumerRecord<K, V> extends AbstractAlo<ConsumerRecord<K, V>> {

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAloConsumerRecord(ConsumerRecord<K, V> consumerRecord, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
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

    @Override
    protected <R> AloFactory<R> createPropagator() {
        return ComposedAlo::new;
    }
}
