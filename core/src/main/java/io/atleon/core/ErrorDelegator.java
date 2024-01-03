package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A convenience interface for declaring error delegators to be used with
 * {@link AloFlux#addAloErrorDelegation(BiFunction)}.
 */
@FunctionalInterface
public interface ErrorDelegator<T> extends BiFunction<T, Throwable, Publisher<?>> {

    /**
     * Create a new error delegator that delegates to a "sending" function. This is useful, for
     * example, when wanting to implement "dead-lettering".
     *
     * @param sender The function invoked to send errored data
     * @return A fluent {@link Sending} ErrorDelegator
     * @param <T> The type of data consumed and sent by the resulting delegator
     */
    static <T> Sending<T, T> sending(Function<? super T, Mono<? extends SenderResult>> sender) {
        return new Sending<>(__ -> true, (data, __) -> data, UnaryOperator.identity(), sender);
    }

    @Override
    Publisher<?> apply(T data, Throwable error);

    /**
     * Error delegator that sends errored data
     *
     * @param <T> The type of errored data consumed
     * @param <S> The type of data sent
     */
    final class Sending<T, S> implements ErrorDelegator<T> {

        private final Predicate<Throwable> predicate;

        private final BiFunction<T, Throwable, S> mapper;

        private final UnaryOperator<Mono<S>> beforeSend;

        private final Function<? super S, Mono<? extends SenderResult>> sender;

        private Sending(
            Predicate<Throwable> predicate,
            BiFunction<T, Throwable, S> mapper,
            UnaryOperator<Mono<S>> beforeSend,
            Function<? super S, Mono<? extends SenderResult>> sender
        ) {
            this.predicate = predicate;
            this.mapper = mapper;
            this.beforeSend = beforeSend;
            this.sender = sender;
        }

        /**
         * Sets the error predicate that must be matched in order for errored data to be sent.
         *
         * @param predicate The predicate that errors must match to be sent
         * @return A <i>new</i> error delegator
         */
        public Sending<T, S> errorMustMatch(Predicate<Throwable> predicate) {
            return new Sending<>(predicate, mapper, beforeSend, sender);
        }

        /**
         * Sets the mapping function to transform errored data into data that can be sent.
         *
         * @param mapper Function that transforms errored data into sendable data
         * @return A <i>new</i> error delegator
         * @param <U> The type of consumed errored data
         */
        public <U> Sending<U, S> composeData(Function<U, S> mapper) {
            return composeData((data, __) -> mapper.apply(data));
        }

        /**
         * Sets the mapping function to transform errored data into data that can be sent.
         *
         * @param mapper BiFunction that transforms errored data and error into sendable data
         * @return A <i>new</i> error delegator
         * @param <U> The type of consumed errored data
         */
        public <U> Sending<U, S> composeData(BiFunction<U, Throwable, S> mapper) {
            return new Sending<>(predicate, mapper, beforeSend, sender);
        }

        /**
         * Sets transformation to be applied to outbound Mono before being sent. This can be
         * helpful for more fluently adding decorative behavior, like delaying or further
         * filtering.
         *
         * @param beforeSend Transformation to be applied to outbound Mono
         * @return A <i>new</i> error delegator
         */
        public Sending<T, S> beforeSend(UnaryOperator<Mono<S>> beforeSend) {
            return new Sending<>(predicate, mapper, beforeSend, sender);
        }

        @Override
        public Mono<SenderResult> apply(T data, Throwable error) {
            if (predicate.test(error)) {
                return Mono.just(mapper.apply(data, error))
                    .transform(beforeSend)
                    .flatMap(sender)
                    .flatMap(result -> result.failureCause().map(Mono::<SenderResult>error).orElse(Mono.just(result)));
            } else {
                return Mono.error(error);
            }
        }
    }
}
