package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloFluxTest {

    @Test
    public void alosCanBeFiltered() {
        TestAlo empty = new TestAlo("");
        Flux.just(empty).as(AloFlux::wrap).filter(String::isEmpty).unwrap().then().block();
        assertFalse(empty.isAcknowledged());

        TestAlo nonEmpty = new TestAlo("DATA");
        Flux.just(nonEmpty).as(AloFlux::wrap).filter(String::isEmpty).unwrap().then().block();
        assertTrue(nonEmpty.isAcknowledged());
    }

    @Test
    public void alosWithSpecificSubTypeOfDataItemCanBeProcessed() {
        GenericAlo<Object> stringAlo = new GenericAlo<>("DATA");
        GenericAlo<Object> integralAlo = new GenericAlo<>(1234);

        AloFlux.wrap(Flux.just(stringAlo, integralAlo)).ofType(Integer.class).unwrap().then().block();

        assertTrue(stringAlo.isAcknowledged());
        assertFalse(integralAlo.isAcknowledged());
    }

    @Test
    public void acknowledgersArePropagated() {
        TestAlo alo = new TestAlo("DATA");

        Alo<String> result = Flux.just(alo).as(AloFlux::wrap).map(String::toLowerCase).unwrap().blockFirst();

        assertNotNull(result);
        assertEquals("data", result.get());
        assertFalse(alo.isAcknowledged());

        Alo.acknowledge(result);
        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void alosCanBeConsumed() {
        TestAlo alo = new TestAlo("DATA");

        Flux.just(alo).as(AloFlux::wrap).consume(System.out::println)
            .subscribe(__ -> {
                throw new IllegalStateException("Should not emit anything");
            });

        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void emptyManyMappingHasConsumerExecuted() {
        TestAlo empty = new TestAlo("");
        Flux.just(empty).as(AloFlux::wrap).flatMapIterable(this::extractCharacters).unwrap().then().block();
        assertTrue(empty.isAcknowledged());
    }

    @Test
    public void acknowledgerIsRunUponManyMappingsBeingAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result =
            Flux.just(alo).as(AloFlux::wrap).flatMapIterable(this::extractCharacters).unwrap().collectList().block();

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
    public void nacknowledgerIsRunWhenAnyMappingsNacknowledge() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result =
            Flux.just(alo).as(AloFlux::wrap).flatMapIterable(this::extractCharacters).unwrap().collectList().block();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());

        Alo.acknowledge(result.get(0));
        Alo.nacknowledge(result.get(1), new IllegalArgumentException());

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false));

        Alo.nacknowledge(result.get(2), new RuntimeException());
        Alo.acknowledge(result.get(3));

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false));
    }

    @Test
    public void nacknowledgerIsNotRunWhenAlreadyAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result =
            Flux.just(alo).as(AloFlux::wrap).flatMapIterable(this::extractCharacters).unwrap().collectList().block();

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
    public void publishedAloAcknowledgesAfterUpstreamCompletionAndAfterDownstreamAcknowledges() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
    public void downstreamPublishedAlosAreAcknowledgedOnlyOnce() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
    public void publishedAloAcknowledgesAfterDownstreamCancelsAndAcknowledges() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))))
            .take(3);

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
    public void publishedAloIsNotAcknowledgedWhenUpstreamErrors() {
        TestAlo alo = new TestAlo("DATA");

        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Function<String, Flux<String>> stringToFlux = data -> sink.asFlux();

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
    public void publishedAloIsNotAcknowledgedWhenUpstreamErrorsFollowedByAcknowledgement() {
        TestAlo alo = new TestAlo("DATA");

        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Function<String, Flux<String>> stringToFlux = data -> sink.asFlux();

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
    public void publishedAloNacknowledgesAfterDownstreamNacknowledgement() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AloFlux<String> aloFlux = AloFlux.wrap(Flux.just(alo))
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
            .then(() -> assertTrue(alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .thenRequest(1)
            .consumeNextWith(aloData -> {
                assertEquals("A", aloData.get());
                Alo.acknowledge(aloData);
            })
            .then(() -> assertFalse(alo.isAcknowledged()))
            .then(() -> assertTrue(alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .expectComplete()
            .verify();
    }

    @Test
    public void aloFluxValuesCanBeMappedToNonNullValues() {
        TestAlo empty = new TestAlo("");
        TestAlo nonEmpty = new TestAlo("data");

        List<String> result = new ArrayList<>();
        AloFlux.wrap(Flux.just(empty, nonEmpty))
            .mapNotNull((string) -> string.isEmpty() ? null : string)
            .subscribe((alo) -> result.add(alo.get()));

        assertEquals(Collections.singletonList(nonEmpty.get()), result);
        assertEquals(1, empty.mapCount());
        assertEquals(1, nonEmpty.mapCount());
        assertTrue(empty.isAcknowledged());
        assertFalse(nonEmpty.isAcknowledged());
    }

    @Test
    public void errorsCanBeEmitted() {
        TestAlo alo = new TestAlo("data");

        AloFlux<Void> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .consume(data -> {
                throw new UnsupportedOperationException("Boom");
            })
            .onAloErrorEmit();

        StepVerifier.create(aloFlux).expectError().verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeEmittedWhenPublishing() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
            .onAloErrorEmit();

        StepVerifier.create(aloFlux).expectError().verify();

        assertFalse(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeIgnoredRatherThanEmitted() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .consume(data -> {
                throw new UnsupportedOperationException("Boom");
            })
            .onAloErrorEmitUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeIgnoredRatherThanEmittedWhenPublishing() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
            .onAloErrorEmitUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorHandlingCanBeDelegated() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .consume(data -> {
                throw new UnsupportedOperationException("Boom");
            })
            .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
    }

    @Test
    public void errorHandlingCanBeDelegatedWhenPublishing() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
            .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeIgnoredRatherThanDelegated() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .consume(data -> {
                throw new UnsupportedOperationException("Boom");
            })
            .onAloErrorDelegateUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeIgnoredRatherThanDelegatedWhenPublishing() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .concatMap(__ -> Mono.error(new UnsupportedOperationException("Boom")))
            .onAloErrorDelegateUnless(UnsupportedOperationException.class::isInstance);

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void errorsCanBeDelegatedToFluentDelegate() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
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
    public void errorsCanBeDelegatedToFluentDelegateWhichMayPropagateTheError() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
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
    public void errorsCanBeDelegatedToFluentDelegateWhichMayConsolidateError() {
        TestAlo alo = new TestAlo("data");

        AloFlux<?> aloFlux = Flux.just(alo)
            .as(AloFlux::wrap)
            .addAloErrorDelegation((string, error) -> Mono.error(new IllegalArgumentException("Bing")))
            .consume(data -> {
                throw new UnsupportedOperationException("Boom");
            })
            .onAloErrorDelegate();

        StepVerifier.create(aloFlux).expectComplete().verify();

        assertFalse(alo.isAcknowledged());
        assertTrue(alo.isNacknowledged());
        assertTrue(alo.getError().orElse(null) instanceof UnsupportedOperationException);
        assertTrue(alo.getError().get().getSuppressed().length == 1);
        assertTrue(alo.getError().get().getSuppressed()[0] instanceof IllegalArgumentException);
    }

    @Test
    public void interleavedSynchronousErrorsResultInCorrectRepublishing() {
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
    public void interleavedAsynchronousErrorsResultInCorrectRepublishing() {
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
                    return await(latch1).then(Mono.<String>error(new UnsupportedOperationException("Boom")).delaySubscription(Duration.ofMillis(100)));
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
            IntStream.range(0, values.length).forEach(i ->
                    executorService.schedule(() -> sink.next(values[i]), 100L * i, TimeUnit.MILLISECONDS)
            );
            executorService.schedule(sink::complete, 100L * values.length, TimeUnit.MILLISECONDS);
        });
    }

    private static <T> Mono<T> await(CountDownLatch latch) {
        return Mono.<T>fromRunnable(() -> awaitSynchronously(latch))
            .subscribeOn(Schedulers.boundedElastic());
    }

    private static void awaitSynchronously(CountDownLatch latch) {
        try {
            latch.await();
        } catch (Exception e) {
            System.err.println("Unexpected failure=" + e);
        }
    }
}