package io.atleon.core;

import io.atelon.util.Defaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Function;

final class DeduplicatingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private static final Scheduler DEFAULT_SCHEDULER = Schedulers.newBoundedElastic(
        Defaults.THREAD_CAP, Integer.MAX_VALUE, DeduplicatingTransformer.class.getSimpleName());

    private final DeduplicationConfig config;

    private final Deduplication<T> deduplication;

    private final Scheduler sourceScheduler;

    private DeduplicatingTransformer(
        DeduplicationConfig config,
        Deduplication<T> deduplication,
        Scheduler sourceScheduler) {
        this.config = config;
        this.deduplication = deduplication;
        this.sourceScheduler = sourceScheduler;
    }

    static <T> DeduplicatingTransformer<T>
    identity(DeduplicationConfig config, Deduplication<T> deduplication) {
        return identity(config, deduplication, DEFAULT_SCHEDULER);
    }

    static <T> DeduplicatingTransformer<T>
    identity(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, deduplication, sourceScheduler);
    }

    static <T> DeduplicatingTransformer<Alo<T>>
    alo(DeduplicationConfig config, Deduplication<T> deduplication) {
        return alo(config, deduplication, DEFAULT_SCHEDULER);
    }

    static <T> DeduplicatingTransformer<Alo<T>>
    alo(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, new AloDeduplication<>(deduplication), sourceScheduler);
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? applyDeduplication(publisher) : publisher;
    }

    private Flux<T> applyDeduplication(Publisher<T> publisher) {
        // - Use Scheduler with single worker for publishing, buffering, and subscribing
        //   (https://github.com/reactor/reactor-core/issues/2352)
        // - Each deduplication key gets its own Group
        // - Buffer max in-flight groups bounded in Duration and size
        Scheduler scheduler = Schedulers.single(sourceScheduler);
        return Flux.from(publisher)
            .publishOn(scheduler, config.getDeduplicationSourcePrefetch())
            .groupBy(deduplication::extractKey)
            .flatMap(groupedFlux -> deduplicateGroup(groupedFlux, scheduler), config.getDeduplicationConcurrency())
            .subscribeOn(scheduler);
    }

    private Mono<T> deduplicateGroup(GroupedFlux<Object, T> groupedFlux, Scheduler scheduler) {
        return groupedFlux.take(config.getDeduplicationDuration(), scheduler)
            .take(config.getMaxDeduplicationSize())
            .reduce(deduplication::reduceDuplicates);
    }

    private static final class AloDeduplication<T> implements Deduplication<Alo<T>> {

        private final Deduplication<T> deduplication;

        public AloDeduplication(Deduplication<T> deduplication) {
            this.deduplication = deduplication;
        }

        @Override
        public Object extractKey(Alo<T> alo) {
            return deduplication.extractKey(alo.get());
        }

        @Override
        public Alo<T> reduceDuplicates(Alo<T> alo1, Alo<T> alo2) {
            return alo1.reduce(deduplication::reduceDuplicates, alo2);
        }
    }
}
