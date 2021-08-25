package io.atleon.core;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.BaseSubscriber;

import java.util.function.Consumer;

public final class Adapters {

    private Adapters() {

    }

    public static <T> Subscriber<T> toSubscriber(Consumer<T> consumer) {
        return new ConsumingSubscriber<>(consumer);
    }

    private static final class ConsumingSubscriber<T> extends BaseSubscriber<T> {

        private final Consumer<T> consumer;

        ConsumingSubscriber(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        @Override
        protected void hookOnNext(T value) {
            consumer.accept(value);
        }
    }
}
