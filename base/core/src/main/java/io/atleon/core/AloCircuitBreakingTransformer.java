package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Gates a stream or sequence of streams with a circuit breaker, using an independent breaker mount
 * for each downstream subscription. A tripped breaker cancels the source subscription;
 * transitioning to a non-tripped state resubscribes, so the source must support resubscription.
 * While connected, denied permissions cause bounded buffering until state notifications prompt
 * retries. Acknowledgements trigger reporting of permit outcomes. Completion of a non-grouped
 * source completes its transformed stream without further resubscription.
 * <p>
 * For grouped streams, all groups share the mount, with a separate permitter and buffer for each
 * subscribed inner group. Group keys are preserved. When the breaker trips, active inner groups
 * complete downstream and their upstream subscriptions are canceled. Recovery resubscribes to
 * the outer source to create fresh groups; previously completed groups remain completed. Normal
 * completion of the outer group sequence allows existing inner groups to finish independently,
 * but does not complete the transformed group sequence. A transformed group sequence remains
 * active until canceled or errored.
 *
 * @param <T> The type emitted by the source: an Alo or an AloGroupedFlux
 * @param <P> The type of source publisher
 */
final class AloCircuitBreakingTransformer<T, P extends Publisher<T>> implements Function<P, Flux<T>> {

    private final ReactiveCircuitBreaker breaker;

    private final int maxPrefetch;

    private final Installer<T, P> installer;

    private AloCircuitBreakingTransformer(ReactiveCircuitBreaker breaker, int maxPrefetch, Installer<T, P> installer) {
        this.breaker = breaker;
        this.maxPrefetch = maxPrefetch;
        this.installer = installer;
    }

    public static <T> AloCircuitBreakingTransformer<Alo<T>, Flux<Alo<T>>> simple(ReactiveCircuitBreaker breaker) {
        return new AloCircuitBreakingTransformer<>(breaker, 1, AloInstallation::new);
    }

    public static <K, T> AloCircuitBreakingTransformer<AloGroupedFlux<K, T>, Flux<AloGroupedFlux<K, T>>> groups(
            ReactiveCircuitBreaker breaker) {
        return new AloCircuitBreakingTransformer<>(breaker, 1, AloGroupsInstallation::new);
    }

    public AloCircuitBreakingTransformer<T, P> withMaxPrefetch(int maxPrefetch) {
        if (maxPrefetch <= 0) {
            throw new IllegalArgumentException("maxPrefetch must be greater than 0");
        }
        return new AloCircuitBreakingTransformer<>(breaker, maxPrefetch, installer);
    }

    @Override
    public Flux<T> apply(P publisher) {
        return Flux.defer(() -> {
            Installation<T, P> installation = installer.install(breaker, maxPrefetch);
            return installation
                    .currentStates()
                    .distinctUntilChanged(ReactiveCircuitBreaker.State::tripped)
                    .switchMap(state -> state.tripped() ? Flux.never() : installation.connect(publisher))
                    .takeUntilOther(installation.currentStates().then())
                    .doFinally(__ -> installation.uninstall());
        });
    }

    @FunctionalInterface
    private interface Installer<T, P extends Publisher<T>> {

        Installation<T, P> install(ReactiveCircuitBreaker breaker, int maxPrefetch);
    }

    private abstract static class Installation<T, P extends Publisher<T>> {

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

        public final Flux<T> connect(P source) {
            return wrap(source).doOnComplete(() -> currentStateQueue.addAndDrain(Sinks.Many::tryEmitComplete));
        }

        public final void uninstall() {
            mounted.unmount();
        }

        public final Flux<ReactiveCircuitBreaker.State> currentStates() {
            return currentStates;
        }

        protected abstract Flux<T> wrap(P source);

        protected final <V> Flux<Alo<V>> wrapAlo(Publisher<Alo<V>> aloSource) {
            return new AloCircuitBreakingOperator<>(aloSource, currentStates, mounted::connect, maxPrefetch);
        }
    }

    private static final class AloInstallation<T> extends Installation<Alo<T>, Flux<Alo<T>>> {

        public AloInstallation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            super(breaker, maxPrefetch);
        }

        @Override
        protected Flux<Alo<T>> wrap(Flux<Alo<T>> source) {
            return wrapAlo(source);
        }
    }

    private static final class AloGroupsInstallation<K, T>
            extends Installation<AloGroupedFlux<K, T>, Flux<AloGroupedFlux<K, T>>> {

        public AloGroupsInstallation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            super(breaker, maxPrefetch);
        }

        @Override
        public Flux<AloGroupedFlux<K, T>> wrap(Flux<AloGroupedFlux<K, T>> source) {
            // Cancel active/inner groups when tripped or when the outer subscription is canceled.
            Flux<ReactiveCircuitBreaker.State> trippedStates = currentStates()
                    .filter(ReactiveCircuitBreaker.State::tripped)
                    .concatWith(Flux.never());
            Sinks.Empty<Void> outerCancellation = Sinks.empty();
            Mono<?> innerCancel = Mono.firstWithSignal(trippedStates.next(), outerCancellation.asMono());
            return source.concatWith(Flux.never())
                    .map(it -> AloGroupedFlux.rewrap(it, wrapAlo(it).takeUntilOther(innerCancel)))
                    .doOnCancel(outerCancellation::tryEmitEmpty);
        }
    }
}
