package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;

final class DeduplicatingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final DeduplicationConfig config;

    private final Deduplicator<T, ?> deduplicator;

    private final Scheduler sourceScheduler;

    private DeduplicatingTransformer(
        DeduplicationConfig config,
        Deduplicator<T, ?> deduplicator,
        Scheduler sourceScheduler
    ) {
        this.config = config;
        this.deduplicator = deduplicator;
        this.sourceScheduler = sourceScheduler;
    }

    static <T> DeduplicatingTransformer<T>
    identity(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, Deduplicator.identity(deduplication), sourceScheduler);
    }

    static <T> DeduplicatingTransformer<Alo<T>>
    alo(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, Deduplicator.alo(deduplication), sourceScheduler);
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        // When enabled, apply transformation on occurrence of the first signal. This is needed (in
        // comparison to a simple transform) to avoid changing the initial downstream subscription
        // thread which would otherwise be switched due to required subscribeOn on in the transform
        return config.isEnabled()
            ? Flux.from(publisher).switchOnFirst((signal, flux) -> flux.transform(this::applyDeduplication))
            : publisher;
    }

    private Flux<T> applyDeduplication(Publisher<T> publisher) {
        // - Use Scheduler with single worker for publishing, buffering, and subscribing
        //   (https://github.com/reactor/reactor-core/issues/2352)
        // - Each deduplication key gets its own Group
        // - Buffer max in-flight groups bounded in Duration and size
        Scheduler scheduler = Schedulers.single(sourceScheduler);
        return Flux.from(publisher)
            .publishOn(scheduler, config.getDeduplicationSourcePrefetch())
            .groupBy(deduplicator::extractKey)
            .flatMap(groupedFlux -> deduplicateGroup(groupedFlux, scheduler), config.getDeduplicationConcurrency())
            .subscribeOn(scheduler);
    }

    private Mono<T> deduplicateGroup(GroupedFlux<Object, T> groupedFlux, Scheduler scheduler) {
        return groupedFlux.take(config.getDeduplicationTimeout(), scheduler)
            .take(config.getMaxDeduplicationSize())
            .collectList()
            .map(deduplicator::deduplicate);
    }

    private static final class Deduplicator<T, R> {

        private final Function<T, R> dataExtractor;

        private final Function<R, Object> keyExtractor;

        private final Function<List<T>, T> reducer;

        private Deduplicator(Function<T, R> dataExtractor, Function<R, Object> keyExtractor, Function<List<T>, T> reducer) {
            this.dataExtractor = dataExtractor;
            this.keyExtractor = keyExtractor;
            this.reducer = reducer;
        }

        public static <T> Deduplicator<T, T> identity(Deduplication<T> deduplication) {
            Function<List<T>, T> reducer = group -> reduceToSingle(group, deduplication::reduceDuplicates);
            return new Deduplicator<>(Function.identity(), deduplication::extractKey, reducer);
        }

        public static <T> Deduplicator<Alo<T>, T> alo(Deduplication<T> deduplication) {
            Function<List<Alo<T>>, Alo<T>> aloReducer = group -> reduceToSingleAlo(group, deduplication::reduceDuplicates);
            return new Deduplicator<>(Alo::get, deduplication::extractKey, aloReducer);
        }

        public Object extractKey(T t) {
            return keyExtractor.apply(dataExtractor.apply(t));
        }

        public T deduplicate(List<T> list) {
            return reducer.apply(list);
        }

        private static <T> Alo<T> reduceToSingleAlo(List<Alo<T>> group, BinaryOperator<T> accumulator) {
            if (group.isEmpty()) {
                throw newEmptyDeduplicationGroupException();
            } else {
                return group.size() == 1 ? group.get(0) : AloOps.fanIn(group).map(it -> reduceToSingle(it, accumulator));
            }
        }

        private static <T> T reduceToSingle(List<T> group, BinaryOperator<T> accumulator) {
            return group.stream().reduce(accumulator).orElseThrow(Deduplicator::newEmptyDeduplicationGroupException);
        }

        private static IllegalStateException newEmptyDeduplicationGroupException() {
            return new IllegalStateException("Something bad has happened. Deduplication group was empty.");
        }
    }
}
