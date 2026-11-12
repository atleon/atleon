package io.atleon.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Limits the throughput of a stream or sequence of streams, using an independent limiter mount
 * for each downstream subscription. A blocked limiter cancels the source subscription;
 * transitioning to an unblocked state resubscribes, so the source must support resubscription.
 * While connected, elements are emitted in order after their capacity reservations complete.
 * Completion of a non-grouped source completes its transformed stream without further
 * resubscription.
 * <p>
 * For grouped streams, all groups share the mount, with capacity reserved sequentially within
 * each subscribed inner group. Group keys are preserved. When the limiter blocks, active inner
 * groups complete downstream and their upstream subscriptions are canceled. Unblocking
 * resubscribes to the outer source to create fresh groups; Normal completion of the outer group
 * sequence allows existing inner groups to finish independently, but does not complete the
 * transformed group sequence. A transformed group sequence remains active until canceled or
 * errored, even when throughput is unlimited (including when the limit publisher is empty).
 * This keeps the shared limiter mount available for active groups and subsequent state
 * notifications.
 *
 * @param <T> The type of elements upon which throughput limiting is enforced
 * @param <I> A container of the elements being limited, which may be the element itself
 */
final class ThroughputLimitingTransformer<T, I> implements Function<Flux<? extends I>, Flux<I>> {

    private final Installer<T, I> installer;

    private final ReactiveThroughputLimiter<? super T> limiter;

    private final int prefetch;

    private ThroughputLimitingTransformer(
            Installer<T, I> installer, ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
        this.installer = installer;
        this.limiter = limiter;
        this.prefetch = prefetch;
    }

    static <T> ThroughputLimitingTransformer<T, Alo<T>> alo(
            ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
        return new ThroughputLimitingTransformer<>(AloInstallation::new, limiter, prefetch);
    }

    static <K, T> ThroughputLimitingTransformer<T, AloGroupedFlux<K, T>> aloGroups(
            ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
        return new ThroughputLimitingTransformer<>(AloGroupsInstallation::new, limiter, prefetch);
    }

    @Override
    public Flux<I> apply(Flux<? extends I> source) {
        // Implementation notes:
        // 1. Subscription, cancellation, and resubscription to the source publisher is gated based
        //    on the underlying installation signaling blocked vs. unblocked state.
        // 2. Per subscription, any completion signal emitted by the source publisher is treated
        //    as a terminal signal on the transformed publisher such that application to finite
        //    sources (when possible) propagates that completion.
        return Flux.defer(() -> {
            Installation<T, I> installation = installer.install(limiter, prefetch);
            return installation
                    .currentStates()
                    .distinctUntilChanged(ReactiveThroughputLimiter.State::blocked)
                    .switchMap(it -> it.blocked() ? Flux.never() : installation.connect(source))
                    .takeUntilOther(installation.currentStates().then())
                    .doFinally(__ -> installation.uninstall());
        });
    }

    @FunctionalInterface
    private interface Installer<T, I> {

        Installation<T, I> install(ReactiveThroughputLimiter<? super T> limiter, int prefetch);
    }

    private abstract static class Installation<T, I> {

        private final SerialQueue<Consumer<Sinks.Many<ReactiveThroughputLimiter.State>>> currentStateQueue;

        private final Flux<ReactiveThroughputLimiter.State> currentStates;

        private final ReactiveThroughputLimiter.Mount<? super T> mount;

        private final int prefetch;

        public Installation(ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
            var currentStateSink = Sinks.unsafe().many().replay().<ReactiveThroughputLimiter.State>latest();
            this.currentStateQueue = SerialQueue.on(currentStateSink);
            this.currentStates = currentStateSink.asFlux();
            this.mount = limiter.mount(this::emitCurrentState, this::emitCurrentStateError);
            this.prefetch = prefetch;
        }

        public Flux<I> connect(Flux<? extends I> source) {
            return apply(source).doOnComplete(this::emitCurrentStateCompletion);
        }

        public void uninstall() {
            mount.unmount();
        }

        public Flux<ReactiveThroughputLimiter.State> currentStates() {
            return currentStates;
        }

        protected abstract Flux<I> apply(Flux<? extends I> source);

        protected final Flux<Alo<T>> applyAlo(Flux<? extends Alo<T>> aloSource) {
            return aloSource.concatMap(alo -> mount.reserveCapacity(alo.get()).thenReturn(alo), prefetch);
        }

        private void emitCurrentState(ReactiveThroughputLimiter.State state) {
            currentStateQueue.addAndDrain(sink -> sink.tryEmitNext(state));
        }

        private void emitCurrentStateCompletion() {
            currentStateQueue.addAndDrain(Sinks.Many::tryEmitComplete);
        }

        private void emitCurrentStateError(Throwable error) {
            currentStateQueue.addAndDrain(sink -> sink.tryEmitError(error));
        }
    }

    private static final class AloInstallation<T> extends Installation<T, Alo<T>> {

        public AloInstallation(ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
            super(limiter, prefetch);
        }

        @Override
        protected Flux<Alo<T>> apply(Flux<? extends Alo<T>> source) {
            return applyAlo(source);
        }
    }

    private static final class AloGroupsInstallation<K, T> extends Installation<T, AloGroupedFlux<K, T>> {

        public AloGroupsInstallation(ReactiveThroughputLimiter<? super T> limiter, int prefetch) {
            super(limiter, prefetch);
        }

        @Override
        protected Flux<AloGroupedFlux<K, T>> apply(Flux<? extends AloGroupedFlux<K, T>> source) {
            // Cancel active/inner groups when blocked or when the outer subscription is canceled.
            Flux<ReactiveThroughputLimiter.State> blockedStates = currentStates()
                    .filter(ReactiveThroughputLimiter.State::blocked)
                    .concatWith(Flux.never());
            Sinks.Empty<Void> outerCancellation = Sinks.empty();
            Mono<?> innerCancel = Mono.firstWithSignal(blockedStates.next(), outerCancellation.asMono());
            return source.concatWith(Flux.never())
                    .map(it -> AloGroupedFlux.rewrap(it, applyAlo(it.unwrap()).takeUntilOther(innerCancel)))
                    .doOnCancel(outerCancellation::tryEmitEmpty);
        }
    }
}
