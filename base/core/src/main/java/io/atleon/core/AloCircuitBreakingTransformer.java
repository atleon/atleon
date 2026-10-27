package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Gates an {@link Alo} stream with a circuit breaker, using an independent breaker registration
 * for each downstream subscription. A tripped breaker disconnects by canceling the source;
 * transitioning to a non-tripped state resubscribes. While connected, denied permissions cause
 * bounded buffering until state notifications prompt retries. Acknowledgements report permit
 * outcomes, and source completion ends the stream.
 *
 * @param <T> The type of value carried by each Alo
 */
final class AloCircuitBreakingTransformer<T> implements Function<Publisher<Alo<T>>, Publisher<Alo<T>>> {

    private final ReactiveCircuitBreaker breaker;

    private final int maxPrefetch;

    private AloCircuitBreakingTransformer(ReactiveCircuitBreaker breaker, int maxPrefetch) {
        this.breaker = breaker;
        this.maxPrefetch = maxPrefetch;
    }

    public static <T> AloCircuitBreakingTransformer<T> create(ReactiveCircuitBreaker breaker) {
        return new AloCircuitBreakingTransformer<>(breaker, 1);
    }

    public AloCircuitBreakingTransformer<T> withMaxPrefetch(int maxPrefetch) {
        if (maxPrefetch <= 0) {
            throw new IllegalArgumentException("maxPrefetch must be greater than 0");
        }
        return new AloCircuitBreakingTransformer<>(breaker, maxPrefetch);
    }

    @Override
    public Flux<Alo<T>> apply(Publisher<Alo<T>> publisher) {
        return Flux.defer(() -> {
            Installation<T> installation = new Installation<>(breaker, maxPrefetch);
            return installation
                    .currentStates()
                    .distinctUntilChanged(ReactiveCircuitBreaker.State::tripped)
                    .switchMap(state -> state.tripped() ? Flux.never() : installation.connect(publisher))
                    .takeUntilOther(installation.currentStates().then())
                    .doFinally(__ -> installation.uninstall());
        });
    }

    private static final class Installation<T> {

        private final SerialQueue<Consumer<Sinks.Many<ReactiveCircuitBreaker.State>>> currentStateQueue;

        private final Flux<ReactiveCircuitBreaker.State> currentStates;

        private final ReactiveCircuitBreaker.Mounted mounted;

        private final int maxPrefetch;

        public Installation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            var currentStateSink = Sinks.unsafe().many().replay().<ReactiveCircuitBreaker.State>latest();
            this.currentStateQueue = SerialQueue.on(currentStateSink);
            this.currentStates = currentStateSink.asFlux();
            this.mounted = breaker.mount(state -> currentStateQueue.addAndDrain(it -> it.tryEmitNext(state)));
            this.maxPrefetch = maxPrefetch;
        }

        public Flux<ReactiveCircuitBreaker.State> currentStates() {
            return currentStates;
        }

        public Flux<Alo<T>> connect(Publisher<Alo<T>> source) {
            return new AloCircuitBreakingOperator<>(source, currentStates, mounted::connect, maxPrefetch)
                    .doOnComplete(() -> currentStateQueue.addAndDrain(Sinks.Many::tryEmitComplete));
        }

        public void uninstall() {
            mounted.unmount();
        }
    }
}
