package io.atleon.core;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloOpsTest {

    @Test
    public void alosCanBeFiltered() {
        TestAlo empty = new TestAlo("");
        assertTrue(AloOps.filtering(String::isEmpty, Alo::acknowledge).test(empty));
        assertFalse(empty.isAcknowledged());

        TestAlo nonEmpty = new TestAlo("DATA");
        assertFalse(AloOps.filtering(String::isEmpty, Alo::acknowledge).test(nonEmpty));
        assertTrue(nonEmpty.isAcknowledged());
    }

    @Test
    public void acknowledgersArePropagated() {
        TestAlo alo = new TestAlo("DATA");

        Alo<String> result = alo.map(String::toLowerCase);

        assertEquals("data", result.get());
        assertFalse(alo.isAcknowledged());

        Alo.acknowledge(result);
        assertTrue(alo.isAcknowledged());
    }

    @Test
    public void emptyManyMappingHasConsumerExecuted() {
        TestAlo empty = new TestAlo("");
        assertTrue(AloOps.mappingToMany(this::extractCharacters, Alo::acknowledge).apply(empty).isEmpty());
        assertTrue(empty.isAcknowledged());
    }

    @Test
    public void acknowledgerIsRunUponManyMappingsBeingAcknowledged() {
        TestAlo alo = new TestAlo("DATA");

        List<Alo<String>> result = new ArrayList<>(AloOps.mappingToMany(this::extractCharacters, Alo::acknowledge).apply(alo));

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

        List<Alo<String>> result = new ArrayList<>(AloOps.mappingToMany(this::extractCharacters, Alo::acknowledge).apply(alo));

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

        List<Alo<String>> result = new ArrayList<>(AloOps.mappingToMany(this::extractCharacters, Alo::acknowledge).apply(alo));

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
    public void publishedAcknowledgeableAcknowledgesAfterUpstreamCompletionAndAfterDownstreamAcknowledges() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(AloOps.publishing(stringToChars).apply(alo), 3)
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
    public void downstreamPublishedAcknowledebalesAreAcknowledgedOnlyOnce() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        StepVerifier.create(AloOps.publishing(stringToChars).apply(alo), 3)
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
    public void publishedAcknowledgeableAcknowledgesAfterDownstreamCancelsAndAcknowledges() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))))
            .take(3);

        AtomicReference<Alo> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(AloOps.publishing(stringToChars).apply(alo))
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
    public void publishedAcknowledgeableNacknowledgesAfterUpstreamError() {
        TestAlo alo = new TestAlo("DATA");

        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Function<String, Flux<String>> stringToFlux = data -> sink.asFlux();

        StepVerifier.create(AloOps.publishing(stringToFlux).apply(alo))
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
            .then(() -> assertTrue(alo.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .expectError()
            .verify();
    }

    @Test
    public void publishedAcknowledgeableNacknowledgesAfterDownstreamNacknowledgement() {
        TestAlo alo = new TestAlo("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        StepVerifier.create(AloOps.publishing(stringToChars).apply(alo), 2)
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
    public void publishedAcknowledgeableCanOnlyBeSubscribedToOnce() {
        TestAlo alo = new TestAlo("DATA");

        Publisher<Alo<String>> publisher = AloOps.<String, String>publishing(Flux::just).apply(alo);

        StepVerifier.create(publisher)
            .expectSubscription()
            .consumeNextWith(aloData -> {
                assertEquals("DATA", aloData.get());
                Alo.acknowledge(aloData);
            })
            .expectComplete()
            .verify();

        assertThrows(IllegalStateException.class, () -> {
            Flux.from(publisher).subscribe(
                data -> {
                },
                error -> {
                    throw error instanceof RuntimeException ? RuntimeException.class.cast(error) : new RuntimeException(error);
                });
        });
    }

    private Collection<String> extractCharacters(String string) {
        return IntStream.range(0, string.length())
            .mapToObj(string::charAt)
            .map(Object::toString)
            .collect(Collectors.toList());
    }
}