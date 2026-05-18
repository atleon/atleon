package io.atleon.core;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloGroupByWithAutoCompleteOperatorTest {

    @Test
    public void subscribe_givenInvalidMaxInFlight_expectsSubscriberError() {
        Flux<Alo<String>> source = Flux.empty();
        AtomicReference<Throwable> zeroError = new AtomicReference<>();
        AtomicReference<Throwable> negativeError = new AtomicReference<>();

        new AloGroupByWithAutoCompleteOperator<>(source, new IdentityGrouping<>(0))
                .subscribe(group -> {}, zeroError::set);
        new AloGroupByWithAutoCompleteOperator<>(source, new IdentityGrouping<>(-1))
                .subscribe(group -> {}, negativeError::set);

        assertNotNull(zeroError.get());
        assertInstanceOf(IllegalArgumentException.class, zeroError.get());
        assertNotNull(negativeError.get());
        assertInstanceOf(IllegalArgumentException.class, negativeError.get());
    }

    @Test
    public void subscribe_givenElementsWithSameKey_expectsEmissionInSingleGroup() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(groups::add);

        source.tryEmitNext(new TestAlo("A"));
        source.tryEmitNext(new TestAlo("A"));

        assertEquals(1, groups.size());
        assertEquals("A", groups.get(0).key());

        List<String> values = new ArrayList<>();
        groups.get(0).subscribe(alo -> {
            values.add(alo.get());
            Alo.acknowledge(alo);
        });

        assertEquals(Arrays.asList("A", "A"), values);
    }

    @Test
    public void subscribe_givenElementsWithDifferentKeys_expectsEmissionInSeparateGroups() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(groups::add);

        source.tryEmitNext(new TestAlo("A"));
        source.tryEmitNext(new TestAlo("B"));
        source.tryEmitNext(new TestAlo("C"));

        assertEquals(3, groups.size());
        assertEquals(
                Arrays.asList("A", "B", "C"),
                groups.stream().map(AloGroupedFlux::key).collect(Collectors.toList()));
    }

    @Test
    public void subscribe_givenAllGroupElementsAcknowledged_expectsGroupAutoCompletion() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(groups::add);

        TestAlo a = new TestAlo("A");
        source.tryEmitNext(a);

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(received::add);

        assertFalse(groupCompleted.get());
        assertFalse(a.isAcknowledged());

        Alo.acknowledge(received.get(0));

        assertTrue(a.isAcknowledged());
        assertTrue(groupCompleted.get());
    }

    @Test
    public void subscribe_givenAllGroupElementsNacknowledged_expectsGroupAutoCompletion() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(groups::add);

        TestAlo a = new TestAlo("A");
        source.tryEmitNext(a);

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(received::add);

        RuntimeException boom = new RuntimeException("Boom");
        Alo.nacknowledge(received.get(0), boom);

        assertTrue(a.isNacknowledged());
        assertSame(boom, a.getError().orElse(null));
        assertTrue(groupCompleted.get(), "Group should still complete normally on nack");
    }

    @Test
    public void subscribe_givenSiblingElementInFlight_expectsGroupRemainsOpen() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(groups::add);

        source.tryEmitNext(new TestAlo("A"));
        source.tryEmitNext(new TestAlo("A"));

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(received::add);

        assertEquals(2, received.size());

        Alo.acknowledge(received.get(0));
        assertFalse(groupCompleted.get(), "Group should not auto-complete while a sibling element is in flight");

        Alo.acknowledge(received.get(1));
        assertTrue(groupCompleted.get());
    }

    @Test
    public void subscribe_givenKeySeenAgainAfterAutoCompletion_expectsFreshGroupEmission() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        List<List<String>> receivedPerGroup = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(group -> {
            groups.add(group);
            List<String> sink = new ArrayList<>();
            receivedPerGroup.add(sink);
            group.unwrap().subscribe(alo -> {
                sink.add(alo.get());
                Alo.acknowledge(alo);
            });
        });

        source.tryEmitNext(new TestAlo("A"));
        source.tryEmitNext(new TestAlo("A"));

        assertEquals(2, groups.size(), "Each emission should produce a fresh group after auto-complete");
        assertNotSame(groups.get(0), groups.get(1));
        assertEquals("A", groups.get(0).key());
        assertEquals("A", groups.get(1).key());
        assertEquals(Arrays.asList("A"), receivedPerGroup.get(0));
        assertEquals(Arrays.asList("A"), receivedPerGroup.get(1));
    }

    @Test
    public void subscribe_givenCompletesGroupElement_expectsEagerGroupCompletion() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>("DONE"::equals))
                .subscribe(groups::add);

        TestAlo done = new TestAlo("DONE");
        source.tryEmitNext(done);

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(received::add);

        assertEquals(1, received.size());
        assertEquals("DONE", received.get(0).get());
        assertTrue(groupCompleted.get(), "Group should complete eagerly when an element matches completesGroup");
        assertFalse(done.isAcknowledged(), "Eager completion should not require acknowledgement");
    }

    @Test
    public void subscribe_givenCompletesGroupElement_expectsAcknowledgementStillPropagates() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>(__ -> true))
                .subscribe(groups::add);

        TestAlo a = new TestAlo("A");
        source.tryEmitNext(a);

        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().subscribe(received::add);

        Alo.acknowledge(received.get(0));

        assertTrue(a.isAcknowledged(), "Acknowledgement should still flow upstream after eager group completion");
    }

    @Test
    public void subscribe_givenCompletesGroupAmongstUnmarkedElements_expectsAllEmittedThenCompletion() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();

        Grouping<String, String> grouping = new Grouping<String, String>() {
            @Override
            public String extractKey(String element) {
                return element.substring(0, 1);
            }

            @Override
            public boolean completesGroup(String element) {
                return element.endsWith("!");
            }

            @Override
            public Optional<Integer> sourcePrefetch() {
                return Optional.empty();
            }
        };

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), grouping).subscribe(groups::add);

        source.tryEmitNext(new TestAlo("A1"));
        source.tryEmitNext(new TestAlo("A2"));
        source.tryEmitNext(new TestAlo("A!"));

        assertEquals(1, groups.size(), "All emissions share key 'A' and should land in a single group");

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        List<String> received = new ArrayList<>();
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(alo -> received.add(alo.get()));

        assertEquals(Arrays.asList("A1", "A2", "A!"), received);
        assertTrue(groupCompleted.get(), "Group should complete after a completesGroup element is emitted into it");
    }

    @Test
    public void subscribe_givenSameKeyAfterCompletesGroup_expectsFreshGroupEmission() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        List<List<String>> receivedPerGroup = new ArrayList<>();
        List<AtomicBoolean> groupCompletions = new ArrayList<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>(__ -> true))
                .subscribe(group -> {
                    groups.add(group);
                    List<String> values = new ArrayList<>();
                    AtomicBoolean completed = new AtomicBoolean(false);
                    receivedPerGroup.add(values);
                    groupCompletions.add(completed);
                    group.unwrap().doOnComplete(() -> completed.set(true)).subscribe(alo -> values.add(alo.get()));
                });

        source.tryEmitNext(new TestAlo("A"));
        source.tryEmitNext(new TestAlo("A"));

        assertEquals(2, groups.size(), "Each emission should produce a fresh group after eager completion");
        assertNotSame(groups.get(0), groups.get(1));
        assertEquals(Arrays.asList("A"), receivedPerGroup.get(0));
        assertEquals(Arrays.asList("A"), receivedPerGroup.get(1));
        assertTrue(groupCompletions.get(0).get());
        assertTrue(groupCompletions.get(1).get());
    }

    @Test
    public void subscribe_givenUpstreamCompletionAndAllElementsAcknowledged_expectsStreamCompletion() {
        TestAlo a = new TestAlo("A");
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        new AloGroupByWithAutoCompleteOperator<>(Flux.just(a), new IdentityGrouping<>())
                .doOnComplete(() -> completed.set(true))
                .subscribe(groups::add);

        assertFalse(completed.get(), "Should not complete while an element is in flight");

        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().subscribe(received::add);
        Alo.acknowledge(received.get(0));

        assertTrue(completed.get());
        assertTrue(a.isAcknowledged());
    }

    @Test
    public void subscribe_givenUpstreamError_expectsErrorPropagationAndActiveGroupCompletion() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        AtomicReference<Throwable> downstreamError = new AtomicReference<>();

        new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>())
                .subscribe(groups::add, downstreamError::set);

        source.tryEmitNext(new TestAlo("A"));

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        AtomicReference<Throwable> groupError = new AtomicReference<>();
        groups.get(0)
                .unwrap()
                .doOnComplete(() -> groupCompleted.set(true))
                .doOnError(groupError::set)
                .subscribe(alo -> {}, e -> {});

        RuntimeException boom = new RuntimeException("Boom");
        source.tryEmitError(boom);

        assertSame(boom, downstreamError.get(), "Downstream group-emission should receive the upstream error");
        assertTrue(groupCompleted.get(), "Active groups should be completed (not errored) on upstream error");
        assertNull(groupError.get());
    }

    @Test
    public void subscribe_givenKeyExtractorFailure_expectsStreamErrorTermination() {
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        AtomicReference<Throwable> downstreamError = new AtomicReference<>();
        RuntimeException boom = new RuntimeException("Key extractor boom");

        new AloGroupByWithAutoCompleteOperator<String, String>(source.asFlux(), Grouping.simple(__ -> {
                    throw boom;
                }))
                .subscribe(group -> {}, downstreamError::set);

        source.tryEmitNext(new TestAlo("A"));

        assertSame(boom, downstreamError.get());
    }

    @Test
    public void subscribe_givenDownstreamCancellation_expectsUpstreamCancellationAndGroupCompletion() {
        AtomicBoolean upstreamCanceled = new AtomicBoolean(false);
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        Flux<Alo<String>> sourceFlux = source.asFlux().doOnCancel(() -> upstreamCanceled.set(true));

        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        Disposable disposable =
                new AloGroupByWithAutoCompleteOperator<>(sourceFlux, new IdentityGrouping<>()).subscribe(groups::add);

        source.tryEmitNext(new TestAlo("A"));

        AtomicBoolean groupCompleted = new AtomicBoolean(false);
        groups.get(0).unwrap().doOnComplete(() -> groupCompleted.set(true)).subscribe(alo -> {});

        disposable.dispose();

        assertTrue(upstreamCanceled.get());
        assertTrue(groupCompleted.get());
    }

    @Test
    public void subscribe_givenBoundedMaxInFlight_expectsUpstreamRequestsCappedAndReplenishedOnAcknowledgement() {
        AtomicLong totalRequested = new AtomicLong(0);
        Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
        Flux<Alo<String>> sourceFlux = source.asFlux().doOnRequest(totalRequested::addAndGet);

        List<AloGroupedFlux<String, String>> groups = new ArrayList<>();
        new AloGroupByWithAutoCompleteOperator<>(sourceFlux, new IdentityGrouping<>(2)).subscribe(groups::add);

        assertEquals(2L, totalRequested.get(), "Initial request should be capped at maxInFlight");

        source.tryEmitNext(new TestAlo("A"));
        assertEquals(2L, totalRequested.get(), "Emitting in-flight element should not increase request count");

        List<Alo<String>> received = new ArrayList<>();
        groups.get(0).unwrap().subscribe(received::add);
        Alo.acknowledge(received.get(0));

        assertEquals(3L, totalRequested.get(), "Acknowledgement should free one in-flight slot and request one more");
    }

    @Test
    public void subscribe_givenConcurrentAcknowledgementAcrossKeys_expectsNoLostElementsAndStreamCompletion()
            throws Exception {
        int numElements = 500;
        int numKeys = 8;

        List<TestAlo> alos = IntStream.range(0, numElements)
                .mapToObj(i -> new TestAlo("K" + (i % numKeys)))
                .collect(Collectors.toList());

        Map<String, List<String>> received = new ConcurrentHashMap<>();
        AtomicInteger consumed = new AtomicInteger(0);

        new AloGroupByWithAutoCompleteOperator<String, String>(Flux.fromIterable(alos), new IdentityGrouping<>())
                .flatMap(
                        group -> group.unwrap().publishOn(Schedulers.parallel()).doOnNext(alo -> {
                            received.computeIfAbsent(group.key(), k -> new CopyOnWriteArrayList<>())
                                    .add(alo.get());
                            consumed.incrementAndGet();
                            Alo.acknowledge(alo);
                        }))
                .blockLast();

        assertEquals(numElements, consumed.get());
        assertEquals(numElements, alos.stream().filter(TestAlo::isAcknowledged).count());
        assertEquals(numKeys, received.size());
        int totalReceived = received.values().stream().mapToInt(List::size).sum();
        assertEquals(numElements, totalReceived);
    }

    @Test
    public void subscribe_givenConcurrentAckAndEmissionForSameKey_expectsExactlyOneCompletionPerEmittedGroup()
            throws Exception {
        int iterations = 200;
        for (int i = 0; i < iterations; i++) {
            Sinks.Many<Alo<String>> source = Sinks.many().unicast().onBackpressureBuffer();
            List<AloGroupedFlux<String, String>> groups = new CopyOnWriteArrayList<>();
            List<Alo<String>> received = new CopyOnWriteArrayList<>();
            AtomicInteger groupCompletions = new AtomicInteger(0);

            new AloGroupByWithAutoCompleteOperator<>(source.asFlux(), new IdentityGrouping<>()).subscribe(group -> {
                groups.add(group);
                group.unwrap().doOnComplete(groupCompletions::incrementAndGet).subscribe(received::add);
            });

            TestAlo first = new TestAlo("K");
            TestAlo second = new TestAlo("K");
            source.tryEmitNext(first);
            assertEquals(1, received.size());

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                pool.submit(() -> {
                    ready.countDown();
                    awaitUninterruptibly(go);
                    Alo.acknowledge(received.get(0));
                });
                pool.submit(() -> {
                    ready.countDown();
                    awaitUninterruptibly(go);
                    source.tryEmitNext(second);
                });
                ready.await();
                go.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

                // Always: both elements acknowledged eventually, both received, group count >= 1
                assertTrue(first.isAcknowledged());
                // Drain remaining: ack the second
                Alo<String> remaining = received.size() > 1 ? received.get(1) : null;
                if (remaining != null) {
                    Alo.acknowledge(remaining);
                }
                assertTrue(second.isAcknowledged(), "Second element must be processed even under race");
                assertEquals(2, received.size(), "Both emitted elements must reach a subscriber");

                // Either 1 group (no auto-complete in between) or 2 groups (auto-complete fired before second emit)
                assertTrue(
                        groups.size() == 1 || groups.size() == 2,
                        "Iteration " + i + ": unexpected group count " + groups.size());
                assertEquals(
                        groups.size(),
                        groupCompletions.get(),
                        "Iteration " + i + ": each emitted group must have completed exactly once");
            } finally {
                pool.shutdownNow();
            }
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class IdentityGrouping<T> implements Grouping<T, T> {

        private final Predicate<T> completesGroup;

        private final @Nullable Integer sourcePrefetch;

        public IdentityGrouping() {
            this(__ -> false, null);
        }

        public IdentityGrouping(int sourcePrefetch) {
            this(__ -> false, sourcePrefetch);
        }

        public IdentityGrouping(Predicate<T> completesGroup) {
            this(completesGroup, null);
        }

        public IdentityGrouping(Predicate<T> completesGroup, @Nullable Integer sourcePrefetch) {
            this.completesGroup = completesGroup;
            this.sourcePrefetch = sourcePrefetch;
        }

        @Override
        public T extractKey(T element) {
            return element;
        }

        @Override
        public boolean completesGroup(T element) {
            return completesGroup.test(element);
        }

        @Override
        public Optional<Integer> sourcePrefetch() {
            return Optional.ofNullable(sourcePrefetch);
        }
    }
}
