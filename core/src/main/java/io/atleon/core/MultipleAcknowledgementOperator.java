package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

public class MultipleAcknowledgementOperator<T, A extends Alo<T>> implements Publisher<Alo<T>> {

    private final Publisher<? extends A> source;

    private final Function<T, ?> groupExtractor;

    private final long maxInFlight;

    public MultipleAcknowledgementOperator(Publisher<? extends A> source, Function<T, ?> groupExtractor, long maxInFlight) {
        this.source = source;
        this.groupExtractor = groupExtractor;
        this.maxInFlight = maxInFlight;
    }

    @Override
    public void subscribe(Subscriber<? super Alo<T>> actual) {
        source.subscribe(
            new AloQueueingSubscriber<>(actual, groupExtractor, MultipleAcknowledgementQueue::create, maxInFlight)
        );
    }
}
