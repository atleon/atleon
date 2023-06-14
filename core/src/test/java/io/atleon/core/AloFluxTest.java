package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
            .subscribe(__ -> { throw new IllegalStateException("Should not emit anything"); });

        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void emptyManyMappingHasConsumerExecuted() {
        TestAlo empty = new TestAlo("");
        Flux.just(empty).as(AloFlux::wrap).flatMapCollection(this::extractCharacters).unwrap().then().block();
        assertTrue(empty.isAcknowledged());
    }

    @Test
    public void acknowledgerIsRunUponManyMappingsBeingAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result =
            Flux.just(alo).as(AloFlux::wrap).flatMapCollection(this::extractCharacters).unwrap().collectList().block();

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
            Flux.just(alo).as(AloFlux::wrap).flatMapCollection(this::extractCharacters).unwrap().collectList().block();

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
            Flux.just(alo).as(AloFlux::wrap).flatMapCollection(this::extractCharacters).unwrap().collectList().block();

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

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(Flux.just(alo).as(AloFlux::wrap).concatMap(stringToChars), 3)
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

        StepVerifier.create(Flux.just(alo).as(AloFlux::wrap).concatMap(stringToChars), 3)
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

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(Flux.just(alo).as(AloFlux::wrap).concatMap(stringToChars))
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

        StepVerifier.create(Flux.just(alo).as(AloFlux::wrap).concatMap(stringToFlux))
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
    public void publishedAloNacknowledgesAfterDownstreamNacknowledgement() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        StepVerifier.create(Flux.just(alo).as(AloFlux::wrap).concatMap(stringToChars), 2)
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

    private Collection<String> extractCharacters(String string) {
        return IntStream.range(0, string.length())
            .mapToObj(string::charAt)
            .map(Object::toString)
            .collect(Collectors.toList());
    }
}