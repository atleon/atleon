package io.atleon.core;

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
 * @param <T> The type emitted by the source
 */
final class CircuitBreakingTransformer<T> implements Function<Flux<? extends T>, Flux<T>> {

    private final Installer<T> installer;

    private final ReactiveCircuitBreaker breaker;

    private final int maxPrefetch;

    private CircuitBreakingTransformer(Installer<T> installer, ReactiveCircuitBreaker breaker, int maxPrefetch) {
        this.installer = installer;
        this.breaker = breaker;
        this.maxPrefetch = maxPrefetch;
    }

    public static <T> CircuitBreakingTransformer<Alo<T>> alo(ReactiveCircuitBreaker breaker) {
        return new CircuitBreakingTransformer<>(AloInstallation::new, breaker, 1);
    }

    public static <K, T> CircuitBreakingTransformer<AloGroupedFlux<K, T>> aloGroups(ReactiveCircuitBreaker breaker) {
        return new CircuitBreakingTransformer<>(AloGroupsInstallation::new, breaker, 1);
    }

    public CircuitBreakingTransformer<T> withMaxPrefetch(int maxPrefetch) {
        if (maxPrefetch <= 0) {
            throw new IllegalArgumentException("maxPrefetch must be greater than 0");
        }
        return new CircuitBreakingTransformer<>(installer, breaker, maxPrefetch);
    }

    @Override
    public Flux<T> apply(Flux<? extends T> source) {
        return Flux.defer(() -> {
            Installation<T> installation = installer.install(breaker, maxPrefetch);
            return installation
                    .currentStates()
                    .distinctUntilChanged(ReactiveCircuitBreaker.State::tripped)
                    .switchMap(state -> state.tripped() ? Flux.never() : installation.connect(source))
                    .takeUntilOther(installation.currentStates().then())
                    .doFinally(__ -> installation.uninstall());
        });
    }

    @FunctionalInterface
    private interface Installer<T> {

        Installation<T> install(ReactiveCircuitBreaker breaker, int maxPrefetch);
    }

    private abstract static class Installation<T> {

        private final SerialQueue<Consumer<Sinks.Many<ReactiveCircuitBreaker.State>>> currentStateQueue;

        private final Flux<ReactiveCircuitBreaker.State> currentStates;

        private final ReactiveCircuitBreaker.Mount mount;

        private final int maxPrefetch;

        public Installation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            var currentStateSink = Sinks.unsafe().many().replay().<ReactiveCircuitBreaker.State>latest();
            this.currentStateQueue = SerialQueue.on(currentStateSink);
            this.currentStates = currentStateSink.asFlux();
            this.mount = breaker.mount(state -> currentStateQueue.addAndDrain(it -> it.tryEmitNext(state)));
            this.maxPrefetch = maxPrefetch;
        }

        public final Flux<T> connect(Flux<? extends T> source) {
            return apply(source).doOnComplete(() -> currentStateQueue.addAndDrain(Sinks.Many::tryEmitComplete));
        }

        public final void uninstall() {
            mount.unmount();
        }

        public final Flux<ReactiveCircuitBreaker.State> currentStates() {
            return currentStates;
        }

        protected abstract Flux<T> apply(Flux<? extends T> source);

        protected final <V> Flux<Alo<V>> applyAlo(Flux<? extends Alo<V>> aloSource) {
            return new AloCircuitBreakingOperator<>(aloSource, currentStates, mount::connect, maxPrefetch);
        }
    }

    private static final class AloInstallation<T> extends Installation<Alo<T>> {

        public AloInstallation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            super(breaker, maxPrefetch);
        }

        @Override
        protected Flux<Alo<T>> apply(Flux<? extends Alo<T>> source) {
            return applyAlo(source);
        }
    }

    private static final class AloGroupsInstallation<K, T> extends Installation<AloGroupedFlux<K, T>> {

        public AloGroupsInstallation(ReactiveCircuitBreaker breaker, int maxPrefetch) {
            super(breaker, maxPrefetch);
        }

        @Override
        protected Flux<AloGroupedFlux<K, T>> apply(Flux<? extends AloGroupedFlux<K, T>> source) {
            // Cancel active/inner groups when tripped or when the outer subscription is canceled.
            Flux<ReactiveCircuitBreaker.State> trippedStates = currentStates()
                    .filter(ReactiveCircuitBreaker.State::tripped)
                    .concatWith(Flux.never());
            Sinks.Empty<Void> outerCancellation = Sinks.empty();
            Mono<?> innerCancel = Mono.firstWithSignal(trippedStates.next(), outerCancellation.asMono());
            return source.concatWith(Flux.never())
                    .map(it -> AloGroupedFlux.rewrap(it, applyAlo(it.unwrap()).takeUntilOther(innerCancel)))
                    .doOnCancel(outerCancellation::tryEmitEmpty);
        }
    }
}
