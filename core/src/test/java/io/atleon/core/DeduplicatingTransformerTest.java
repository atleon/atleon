package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeduplicatingTransformerTest {

    private static final DeduplicationConfig CONFIG = new DeduplicationConfig(Duration.ofMillis(800), 4);

    private final Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

    private final Flux<String> downstream = sink.asFlux()
            .transform(
                    DeduplicatingTransformer.identity(CONFIG, Deduplication.identity(), Schedulers.boundedElastic()));

    @Test
    public void duplicatesAreNotEmitted() {
        StepVerifier.withVirtualTime(() -> downstream)
                .expectSubscription()
                .then(() -> {
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("ONE");
                })
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .expectNext("ONE")
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .thenCancel()
                .verify();
    }

    @Test
    public void deduplicatesOnlyWithinDuration() {
        StepVerifier.withVirtualTime(() -> downstream)
                .expectSubscription()
                .then(() -> sink.tryEmitNext("ONE"))
                .thenAwait(CONFIG.getDeduplicationTimeout())
                .expectNext("ONE")
                .then(() -> sink.tryEmitNext("ONE"))
                .thenAwait(CONFIG.getDeduplicationTimeout())
                .expectNext("ONE")
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .thenCancel()
                .verify();
    }

    @Test
    public void deduplicatesOnlyWithinMaxSize() {
        StepVerifier.withVirtualTime(() -> downstream)
                .expectSubscription()
                .then(() -> {
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("ONE");
                })
                .expectNext("ONE")
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .expectNext("ONE")
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .thenCancel()
                .verify();
    }

    @Test
    public void dataAreSequentiallyProcessed() {
        DeduplicatingTransformer<String> transformer = DeduplicatingTransformer.identity(
                CONFIG, Deduplication.emitLast(Function.identity()), Schedulers.parallel());
        Flux<String> invertedDownstream = sink.asFlux().transform(transformer);

        StepVerifier.create(invertedDownstream)
                .expectSubscription()
                .then(() -> sink.tryEmitNext("ONE"))
                .thenAwait(Duration.ofMillis(200))
                .then(() -> {
                    sink.tryEmitNext("TWO");
                    sink.tryEmitNext("TWO");
                    sink.tryEmitNext("ONE");
                })
                .thenAwait(CONFIG.getDeduplicationTimeout())
                .expectNext("ONE")
                .expectNext("TWO")
                .expectNoEvent(CONFIG.getDeduplicationTimeout())
                .thenCancel()
                .verify();
    }

    @Test
    public void itemsAreNotDroppedUnderHeavyLoad() throws Exception {
        AtomicLong upstream = new AtomicLong(0L);
        AtomicLong downstream = new AtomicLong(0L);
        CountDownLatch latch = new CountDownLatch(1);

        Flux.fromStream(Stream.iterate(
                        randomLong(CONFIG.getDeduplicationConcurrency()),
                        last -> randomLong(CONFIG.getDeduplicationConcurrency())))
                .flatMap(
                        number -> Flux.concat(
                                Mono.just(number).repeat(CONFIG.getMaxDeduplicationSize() - 1),
                                Mono.just(number)
                                        .delayElement(randomDuration(
                                                CONFIG.getDeduplicationTimeout().multipliedBy(2)))),
                        CONFIG.getDeduplicationConcurrency())
                .take(Duration.ofSeconds(10L))
                .doOnNext(next -> upstream.incrementAndGet())
                .map(Collections::singletonList)
                .transform(DeduplicatingTransformer.identity(
                        CONFIG,
                        new Deduplication<List<Long>>() {

                            @Override
                            public Object extractKey(List<Long> longs) {
                                return longs.get(0);
                            }

                            @Override
                            public List<Long> reduceDuplicates(List<Long> first, List<Long> second) {
                                return Stream.concat(first.stream(), second.stream())
                                        .collect(Collectors.toList());
                            }
                        },
                        Schedulers.parallel()))
                .doOnNext(buffer -> {
                    // Mimic real-world computationally-bound processing overhead
                    long startNano = System.nanoTime();
                    while (System.nanoTime() - startNano < 1_000_000)
                        ;
                })
                .map(Collection::size)
                .subscribe(downstream::addAndGet, System.err::println, latch::countDown);

        latch.await();
        assertEquals(upstream.get(), downstream.get());
        System.out.println("Emitted: " + downstream.get());
    }

    private static long randomLong(long exclusiveUpperBound) {
        return (long) (Math.random() * exclusiveUpperBound);
    }

    private static Duration randomDuration(Duration exclusiveUpperBound) {
        return Duration.ofMillis((long) (Math.random() * exclusiveUpperBound.toMillis()));
    }
}
