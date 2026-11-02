package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloFluxTest {

    @Test
    public void doOnAloAcknowledge_givenAcknowledgement_expectsSideEffectWithOriginalData() {
        TestAlo alo = new TestAlo("DATA");
        AtomicReference<String> sideEffectSetData = new AtomicReference<>(null);

        Alo<String> result = AloFlux.just(alo)
                .doOnAloAcknowledge(sideEffectSetData::set)
                .unwrap()
                .blockFirst();

        assertFalse(alo.isAcknowledged());
        assertNull(sideEffectSetData.get());

        Alo.acknowledge(result);

        assertTrue(alo.isAcknowledged());
        assertEquals(alo.get(), sideEffectSetData.get());
    }

    @Test
    public void filter_givenMatchingAndNonMatchingValues_expectsOnlyNonMatchingValuesAcknowledged() {
        TestAlo empty = new TestAlo("");
        AloFlux.just(empty).filter(String::isEmpty).unwrap().then().block();
        assertFalse(empty.isAcknowledged());

        TestAlo nonEmpty = new TestAlo("DATA");
        AloFlux.just(nonEmpty).filter(String::isEmpty).unwrap().then().block();
        assertTrue(nonEmpty.isAcknowledged());
    }

    @Test
    public void ofType_givenMixedValueTypes_expectsOnlyOtherTypesAcknowledged() {
        GenericAlo<Object> stringAlo = new GenericAlo<>("DATA");
        GenericAlo<Object> integralAlo = new GenericAlo<>(1234);

        AloFlux.just(stringAlo, integralAlo)
                .ofType(Integer.class)
                .unwrap()
                .then()
                .block();

        assertTrue(stringAlo.isAcknowledged());
        assertFalse(integralAlo.isAcknowledged());
    }

    @Test
    public void map_givenMappedValueAcknowledged_expectsOriginalValueAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Alo<String> result = AloFlux.just(alo).map(String::toLowerCase).unwrap().blockFirst();

        assertNotNull(result);
        assertEquals("data", result.get());
        assertFalse(alo.isAcknowledged());

        Alo.acknowledge(result);
        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void consume_givenSuccessfulConsumer_expectsAcknowledgementWithoutEmission() {
        TestAlo alo = new TestAlo("DATA");

        AloFlux.just(alo).consume(System.out::println).subscribe(__ -> {
            throw new IllegalStateException("Should not emit anything");
        });

        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void flatMapIterable_givenEmptyIterable_expectsOriginalValueAcknowledged() {
        TestAlo empty = new TestAlo("");
        AloFlux.just(empty)
                .flatMapIterable(this::extractCharacters)
                .unwrap()
                .then()
                .block();
        assertTrue(empty.isAcknowledged());
    }

    @Test
    public void flatMapIterable_givenAllOutputsAcknowledged_expectsOriginalValueAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result = AloFlux.just(alo)
                .flatMapIterable(this::extractCharacters)
                .unwrap()
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertFalse(alo.isAcknowledged());

        Alo.acknowledge(result.get(0));
        Alo.acknowledge(result.get(1));
        Alo.acknowledge(result.get(2));

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.acknowledge(result.get(3));

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void flatMapIterable_givenMultipleOutputsNacknowledged_expectsFirstErrorPropagated() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result = AloFlux.just(alo)
                .flatMapIterable(this::extractCharacters)
                .unwrap()
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.acknowledge(result.get(0));
        Alo.nacknowledge(result.get(1), new IllegalArgumentException());

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(
                alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false));

        Alo.nacknowledge(result.get(2), new RuntimeException());
        Alo.acknowledge(result.get(3));

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(
                alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false));
    }

    @Test
    public void flatMapIterable_givenAcknowledgedOutputThenNacknowledged_expectsNacknowledgementIgnored() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result = AloFlux.just(alo)
                .flatMapIterable(this::extractCharacters)
                .unwrap()
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.acknowledge(result.get(0));
        Alo.acknowledge(result.get(1));
        Alo.acknowledge(result.get(2));
        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.nacknowledge(result.get(2), new IllegalArgumentException());
        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.acknowledge(result.get(3));
        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void concatMap_givenCompletedPublisher_expectsAcknowledgementAfterAllOutputsAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
                .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToChars));

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(aloFlux, 3)
                .expectSubscription()
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("T", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .thenRequest(1)
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    lastAcknowledgeable.set(aloData);
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .expectComplete()
                .verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
        assertNotNull(lastAcknowledgeable.get());

        Alo.acknowledge(lastAcknowledgeable.get());
        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void concatMap_givenOutputAcknowledgedThenNacknowledged_expectsNacknowledgementIgnored() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
                .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToChars));

        StepVerifier.create(aloFlux, 3)
                .expectSubscription()
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("T", aloData.get());
                    Alo.acknowledge(aloData);
                    Alo.nacknowledge(aloData, new IllegalArgumentException());
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .thenRequest(1L)
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .then(() -> assertTrue(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .expectComplete()
                .verify();
    }

    @Test
    public void concatMap_givenPublisherLimitedByTake_expectsAcknowledgementAfterAllOutputsAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
                .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))))
                .take(3);

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToChars));

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(aloFlux)
                .expectSubscription()
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("T", aloData.get());
                    lastAcknowledgeable.set(aloData);
                })
                .expectComplete()
                .verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
        assertNotNull(lastAcknowledgeable.get());

        Alo.acknowledge(lastAcknowledgeable.get());
        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void concatMap_givenMappedPublisherError_expectsOriginalValueUnacknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Function<String, Flux<String>> stringToFlux = data -> sink.asFlux();

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToFlux));

        StepVerifier.create(aloFlux)
                .expectSubscription()
                .then(() -> sink.tryEmitNext("D"))
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .then(() -> sink.tryEmitError(new IllegalArgumentException()))
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .expectError()
                .verify();
    }

    @Test
    public void concatMap_givenMappedPublisherErrorThenOutputAcknowledged_expectsOriginalValueUnacknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Function<String, Flux<String>> stringToFlux = data -> sink.asFlux();

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToFlux));

        AtomicReference<Alo<String>> first = new AtomicReference<>(null);

        StepVerifier.create(aloFlux)
                .expectSubscription()
                .then(() -> sink.tryEmitNext("D"))
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    first.set(aloData);
                    sink.tryEmitError(new IllegalArgumentException());
                })
                .then(() -> Alo.acknowledge(first.get()))
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .expectError()
                .verify();
    }

    @Test
    public void concatMap_givenOutputNacknowledged_expectsOriginalValueNacknowledged() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
                .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.just(alo)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .flatMapAlo(it -> it.concatMap(stringToChars));

        StepVerifier.create(aloFlux, 2)
                .expectSubscription()
                .consumeNextWith(aloData -> {
                    assertEquals("D", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertFalse(alo.isNacknowledged()))
                .thenRequest(1)
                .consumeNextWith(aloData -> {
                    assertEquals("T", aloData.get());
                    Alo.nacknowledge(aloData, new IllegalArgumentException());
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertTrue(alo.getError()
                        .map(IllegalArgumentException.class::isInstance)
                        .orElse(false)))
                .thenRequest(1)
                .consumeNextWith(aloData -> {
                    assertEquals("A", aloData.get());
                    Alo.acknowledge(aloData);
                })
                .then(() -> assertFalse(alo.isAcknowledged()))
                .then(() -> assertTrue(alo.getError()
                        .map(IllegalArgumentException.class::isInstance)
                        .orElse(false)))
                .expectComplete()
                .verify();
    }

    @Test
    public void mapNotNull_givenNullAndNonNullResults_expectsOnlyNullResultsAcknowledged() {
        TestAlo empty = new TestAlo("");
        TestAlo nonEmpty = new TestAlo("data");

        List<String> result = new ArrayList<>();
        AloFlux.just(empty, nonEmpty)
                .mapNotNull((string) -> string.isEmpty() ? null : string)
                .subscribe((alo) -> result.add(alo.get()));

        assertEquals(Collections.singletonList(nonEmpty.get()), result);
        assertEquals(1, empty.mapCount());
        assertEquals(1, nonEmpty.mapCount());
        assertTrue(empty.isAcknowledged());
        assertFalse(nonEmpty.isAcknowledged());
    }

    @Test
    public void doOnDiscard_givenFilteredValue_expectsMatchingHooksInvokedInOrder() {
        TestAlo empty = new TestAlo("");
        TestAlo nonEmpty = new TestAlo("data");

        List<String> processed = new ArrayList<>();
        List<String> discardTexts = new ArrayList<>();
        AloFlux.just(empty, nonEmpty)
                .filter(string -> !string.isEmpty())
                .doOnDiscard(Object.class, value -> discardTexts.add("ONE: " + value))
                .doOnDiscard(Integer.class, value -> discardTexts.add("TWO: " + value))
                .doOnDiscard(String.class, value -> discardTexts.add("THREE: " + value))
                .subscribe((alo) -> processed.add(alo.get()));

        assertEquals(Collections.singletonList(nonEmpty.get()), processed);
        assertEquals(Arrays.asList("ONE: " + empty.get(), "THREE: " + empty.get()), discardTexts);
    }

    @Test
    public void onAloErrorEmit_givenConsumerError_expectsErrorEmissionWithoutAcknowledgement() {
        TestAlo alo = new TestAlo("data");

        AloFlux<Void> aloFlux = AloFlux.just(alo)
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorEmit();

        StepVerifier.create(aloFlux).expectError().verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorEmit_givenMappedPublisherError_expectsErrorEmissionWithoutAcknowledgement() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
                .onAloErrorEmit();

        StepVerifier.create(aloFlux).expectError().verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorEmitUnless_givenMatchingConsumerError_expectsAcknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorEmitUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorEmitUnless_givenMatchingMappedPublisherError_expectsAcknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
                .onAloErrorEmitUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorDelegate_givenConsumerError_expectsNacknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorDelegate_givenMappedPublisherError_expectsNacknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
                .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorDelegateUnless_givenMatchingConsumerError_expectsAcknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorDelegateUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void onAloErrorDelegateUnless_givenMatchingMappedPublisherError_expectsAcknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
                .onAloErrorDelegateUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void addAloErrorDelegation_givenSuccessfulDelegate_expectsAcknowledgementAndCompletion() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .addAloErrorDelegation((string, error) -> Mono.empty())
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void addAloErrorDelegation_givenDelegatePropagatesError_expectsNacknowledgementWithOriginalError() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .addAloErrorDelegation((string, error) -> Mono.error(error))
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(alo.getError().get() instanceof UnsupportedOperationException);
        assertTrue(alo.getError().get().getSuppressed().length == 0);
    }

    @Test
    public void addAloErrorDelegation_givenDelegateEmitsDifferentError_expectsOriginalErrorSuppressed() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = AloFlux.just(alo)
                .addAloErrorDelegation((string, error) -> Mono.error(new IllegalArgumentException("Bing")))
                .consume(data -> {
                    throw new UnsupportedOperationException("Boom");
                })
                .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(alo.getError().orElse(null) instanceof IllegalArgumentException);
        assertTrue(alo.getError().get().getSuppressed().length == 1);
        assertTrue(alo.getError().get().getSuppressed()[0] instanceof UnsupportedOperationException);
    }

    @Test
    public void resubscribeOnError_givenInterleavedSynchronousErrors_expectsReprocessingOfUnacknowledgedValues() {
        TestAlo alo1 = new TestAlo("DATA1");
        TestAlo alo2 = new TestAlo("DATA2");
        TestAlo alo3 = new TestAlo("DATA3");

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicBoolean thrownOnce = new AtomicBoolean(false);
        List<String> successfullyProcessed = new ArrayList<>();
        publishAsync(alo1, alo2, alo3)
                .as(AloFlux::wrap)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .innerPublishOn(Schedulers.boundedElastic())
                .innerMap(value -> {
                    if (value.equals("DATA1") && thrownOnce.compareAndSet(false, true)) {
                        awaitSynchronously(latch1);
                        throw new UnsupportedOperationException("Boom");
                    } else if (value.equals("DATA2")) {
                        awaitSynchronously(latch2);
                    }
                    return value;
                })
                .flatMapAlo()
                .doFinally(__ -> latch2.countDown())
                .resubscribeOnError(AloFluxTest.class.getSimpleName(), Duration.ofSeconds(1))
                .consumeAloAndGet(Alo::acknowledge)
                .doOnNext(successfullyProcessed::add)
                .subscribe(__ -> latch1.countDown(), System.err::println, completed::countDown);

        awaitSynchronously(completed);

        assertEquals(1, alo1.acknowledgedCount());
        assertEquals(1, alo2.acknowledgedCount());
        assertEquals(2, alo3.acknowledgedCount());

        assertEquals(4, successfullyProcessed.size());
        assertEquals("DATA3", successfullyProcessed.get(0));
        assertEquals(1, successfullyProcessed.stream().filter("DATA1"::equals).count());
        assertEquals(1, successfullyProcessed.stream().filter("DATA2"::equals).count());
        assertEquals(2, successfullyProcessed.stream().filter("DATA3"::equals).count());
    }

    @Test
    public void resubscribeOnError_givenInterleavedAsynchronousErrors_expectsReprocessingOfUnacknowledgedValues() {
        TestAlo alo1 = new TestAlo("DATA1");
        TestAlo alo2 = new TestAlo("DATA2");
        TestAlo alo3 = new TestAlo("DATA3");

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicBoolean erroredOnce = new AtomicBoolean(false);
        List<String> successfullyProcessed = new ArrayList<>();
        publishAsync(alo1, alo2, alo3)
                .as(AloFlux::wrap)
                .groupBy(Function.identity(), Integer.MAX_VALUE)
                .innerConcatMap(value -> {
                    if (value.equals("DATA1") && erroredOnce.compareAndSet(false, true)) {
                        return await(latch1)
                                .then(Mono.<String>error(new UnsupportedOperationException("Boom"))
                                        .delaySubscription(Duration.ofMillis(100)));
                    } else if (value.equals("DATA2")) {
                        return await(latch2).thenReturn(value);
                    } else {
                        return Mono.just(value).doFinally(__ -> latch1.countDown());
                    }
                })
                .flatMapAlo()
                .doFinally(__ -> latch2.countDown())
                .resubscribeOnError(AloFluxTest.class.getSimpleName(), Duration.ofSeconds(1))
                .consumeAloAndGet(Alo::acknowledge)
                .subscribe(successfullyProcessed::add, System.err::println, completed::countDown);

        awaitSynchronously(completed);

        assertEquals(1, alo1.acknowledgedCount());
        assertEquals(1, alo2.acknowledgedCount());
        assertEquals(2, alo3.acknowledgedCount());

        assertEquals(4, successfullyProcessed.size());
        assertEquals("DATA3", successfullyProcessed.get(0));
        assertEquals(1, successfullyProcessed.stream().filter("DATA1"::equals).count());
        assertEquals(1, successfullyProcessed.stream().filter("DATA2"::equals).count());
        assertEquals(2, successfullyProcessed.stream().filter("DATA3"::equals).count());
    }

    @Test
    void groupByRoundRobin_givenMultipleElements_expectsRoundRobinIntegerGroups() {
        Map<Integer, List<Integer>> groups = Flux.range(0, 8)
                .map(GenericAlo::new)
                .as(AloFlux::wrap)
                .groupByRoundRobin(3)
                .unwrap()
                .flatMap(group ->
                        group.unwrap().map(Alo::get).collectList().map(values -> Map.entry(group.key(), values)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .block();

        assertEquals(Map.of(0, List.of(0, 3, 6), 1, List.of(1, 4, 7), 2, List.of(2, 5)), groups);
    }

    @Test
    void groupByRoundRobin_givenAllGroupsEmitted_expectsCompletionOnlyWhenSourceCompletes() {
        TestPublisher<Alo<Integer>> source = TestPublisher.create();
        AtomicBoolean completed = new AtomicBoolean();
        List<Integer> values = new ArrayList<>();

        StepVerifier.create(AloFlux.wrap(source)
                        .groupByRoundRobin(2)
                        .unwrap()
                        .doOnNext(group -> group.subscribe(alo -> values.add(alo.get())))
                        .doOnComplete(() -> completed.set(true)))
                .then(() -> source.next(new GenericAlo<>(0), new GenericAlo<>(1)))
                .expectNextMatches(group -> group.key() == 0)
                .expectNextMatches(group -> group.key() == 1)
                .then(() -> assertFalse(completed.get()))
                .then(() -> source.next(new GenericAlo<>(2), new GenericAlo<>(3)))
                .then(() -> assertEquals(List.of(0, 1, 2, 3), values))
                .then(source::complete)
                .verifyComplete();
    }

    @Test
    void groupByRoundRobin_givenResubscription_expectsGroupingToRestartAtZero() {
        Flux<Integer> keys = Flux.range(0, 2)
                .map(GenericAlo::new)
                .as(AloFlux::wrap)
                .groupByRoundRobin(3)
                .unwrap()
                .doOnNext(group -> group.subscribe())
                .map(AloGroupedFlux::key);

        StepVerifier.create(keys.repeat(1)).expectNext(0, 1, 0, 1).verifyComplete();
    }

    @Test
    void groupByRoundRobin_givenEmptySource_expectsCompletionWithoutGroups() {
        StepVerifier.create(AloFlux.wrap(Flux.<Alo<Integer>>empty())
                        .groupByRoundRobin(3)
                        .unwrap())
                .verifyComplete();
    }

    @Test
    void groupByRoundRobin_givenNonPositiveCardinality_expectsIllegalArgumentException() {
        AloFlux<Integer> source = AloFlux.wrap(Flux.empty());

        assertThrows(IllegalArgumentException.class, () -> source.groupByRoundRobin(0));
        assertThrows(IllegalArgumentException.class, () -> source.groupByRoundRobin(-1));
    }

    private Collection<String> extractCharacters(String string) {
        return IntStream.range(0, string.length())
                .mapToObj(string::charAt)
                .map(Object::toString)
                .collect(Collectors.toList());
    }

    @SafeVarargs
    private static <T> Flux<T> publishAsync(T... values) {
        return Flux.create(sink -> {
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            sink.onDispose(executorService::shutdown);
            IntStream.range(0, values.length)
                    .forEach(
                            i -> executorService.schedule(() -> sink.next(values[i]), 100L * i, TimeUnit.MILLISECONDS));
            executorService.schedule(sink::complete, 100L * values.length, TimeUnit.MILLISECONDS);
        });
    }

    private static <T> Mono<T> await(CountDownLatch latch) {
        return Mono.<T>fromRunnable(() -> awaitSynchronously(latch)).subscribeOn(Schedulers.boundedElastic());
    }

    private static void awaitSynchronously(CountDownLatch latch) {
        try {
            latch.await();
        } catch (Exception e) {
            System.err.println("Unexpected failure=" + e);
        }
    }
}
